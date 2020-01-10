/*
 * Copyright (c) 2010-2020. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.messaging;

import org.axonframework.eventhandling.EventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.utils.MockException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class MessageDispatchInterceptorsTest {

    private GenericEventMessage<String> message;
    private MessageDispatchInterceptors<EventMessage<?>, Message<?>> chain;

    @BeforeEach
    void setUp() {
        message = new GenericEventMessage<>("testPayload");
        chain = new MessageDispatchInterceptors<>();
    }

    @Test
    void emptyInterceptorChain() throws Exception {

        AtomicReference<Message<?>> result = new AtomicReference<>();
        chain.intercept(message, (m, r) -> result.set(m));

        assertNotNull(result.get());
    }

    @Test
    void interceptorChainModifiesMessage() throws Exception {
        AtomicReference<Message<?>> result = new AtomicReference<>();

        chain.registerDispatchInterceptor(new MetaDataAddingInterceptor("key", "value"));
        chain.registerDispatchInterceptor(new MetaDataAddingInterceptor("key2", "value2"));

        chain.intercept(message, (m, r) -> result.set(m));

        assertNotNull(result.get());
        assertEquals("value", result.get().getMetaData().get("key"));
        assertEquals("value2", result.get().getMetaData().get("key2"));

    }

    @Test
    void interceptorChainMayAdaptResultNotification() throws Exception {
        AtomicReference<Message<?>> sentMessage = new AtomicReference<>();
        AtomicReference<ResultHandler<EventMessage<?>, Message<?>>> resultHandler = new AtomicReference<>();

        ResultCountingInterceptor countingInterceptor1 = new ResultCountingInterceptor();
        ResultCountingInterceptor countingInterceptor2 = new ResultCountingInterceptor();
        chain.registerDispatchInterceptor(new MetaDataAddingInterceptor("key", "value"));
        chain.registerDispatchInterceptor(countingInterceptor1);
        chain.registerDispatchInterceptor(countingInterceptor2);

        chain.intercept(message, (m, r) -> {
            sentMessage.set(m);
            resultHandler.set(r);
        });

        assertNotNull(sentMessage.get());
        assertEquals("value", sentMessage.get().getMetaData().get("key"));
        assertEquals(0, countingInterceptor1.counter.get());
        assertEquals(0, countingInterceptor2.counter.get());

        resultHandler.get().onResult(message, new GenericMessage<>("result1"));
        resultHandler.get().onResult(message, new GenericMessage<>("result2"));

        assertEquals(2, countingInterceptor1.counter.get());
        assertEquals(2, countingInterceptor2.counter.get());
        assertFalse(countingInterceptor1.completed.get());
        assertFalse(countingInterceptor2.completed.get());

        resultHandler.get().onComplete(message);
        assertTrue(countingInterceptor1.completed.get());
        assertTrue(countingInterceptor2.completed.get());

        assertNull(countingInterceptor1.exception.get());
        assertNull(countingInterceptor2.exception.get());
    }

    @Test
    void testInterceptorChainRethrowsExceptionsFromInterceptor() throws Exception {
        chain.registerDispatchInterceptor(new MetaDataAddingInterceptor("key", "value"));
        chain.registerDispatchInterceptor(new ResultAwareMessageDispatchInterceptor<Message<?>, Message<?>>() {
            @Override
            public void dispatch(Message<?> message, ResultHandler<Message<?>, Message<?>> resultHandler,
                                 MessageDispatchInterceptorChain chain) {
                throw new MockException("");
            }
        });
        ResultAwareMessageDispatchInterceptor<Message<?>, Message<?>> mock = mock(ResultAwareMessageDispatchInterceptor.class);
        chain.registerDispatchInterceptor(mock);

        assertThrows(MockException.class,
                     () -> chain.intercept(GenericEventMessage.asEventMessage("test"), (m, r) -> {
                     }));
        verify(mock, never()).dispatch(any(), any(), any());
    }

    @Test
    void testInterceptorChainRethrowsExceptionsFromAction() throws Exception {
        chain.registerDispatchInterceptor(new MetaDataAddingInterceptor("key", "value"));

        assertThrows(MockException.class,
                     () -> chain.intercept(GenericEventMessage.asEventMessage("test"), (m, r) -> {
                         throw new MockException();
                     }));
    }

    @Test
    void testInterceptorMayProceedAgainToRetryOnFailures() throws Exception {
        chain.registerDispatchInterceptor(new MetaDataAddingInterceptor("key", "value"));
        chain.registerDispatchInterceptor(new ResultAwareMessageDispatchInterceptor<Message<?>, Message<?>>() {
            @Override
            public void dispatch(Message<?> message, ResultHandler<Message<?>, Message<?>> resultHandler,
                                 MessageDispatchInterceptorChain chain) {
                chain.proceed(message, new ResultHandlerAdapter<Message<?>, Message<?>>(resultHandler) {
                    @Override
                    public void onResult(Message<?> message, Message<?> result) {
                        int retries = (int) message.getMetaData().getOrDefault("retry", 0);
                        if (retries < 5) {
                            try {
                                chain.proceed(message.andMetaData(MetaData.with("retry", retries + 1)), this);
                            } catch (Exception e) {
                                super.onResult(message, result);
                            }
                        } else {
                            super.onResult(message, result);
                        }
                    }
                });
            }
        });
        List<EventMessage<?>> invocations = new ArrayList<>();
        chain.intercept(GenericEventMessage.asEventMessage("test"), (m, r) -> {
            invocations.add(m);
            r.onResult(m, m);
        });
        assertEquals(6, invocations.size());
    }

    @Test
    void testInterceptorChainAllowsDuplicateProceed() throws Exception {
        List<Message<?>> results = new ArrayList<>();
        chain.registerDispatchInterceptor((ResultAwareMessageDispatchInterceptor<Message<?>, Message<?>>) (message, resultHandler, chain) -> {
            chain.proceed(message, resultHandler);
            chain.proceed(message, resultHandler);
        });
        chain.registerDispatchInterceptor(new MetaDataAddingInterceptor(UUID.randomUUID().toString(), "value"));
        chain.intercept(message, (m, r) -> results.add(m));
        assertEquals(2, results.size());
        assertEquals(1, results.get(0).getMetaData().size());
        assertEquals(1, results.get(1).getMetaData().size());
    }

    private static class MetaDataAddingInterceptor implements ResultAwareMessageDispatchInterceptor<EventMessage<?>, Message<?>> {

        private final String key;
        private final String value;

        public MetaDataAddingInterceptor(String key, String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void dispatch(EventMessage<?> message, ResultHandler<EventMessage<?>, Message<?>> resultHandler, MessageDispatchInterceptorChain chain) throws Exception {
            chain.proceed(message.andMetaData(MetaData.with(key, value)), resultHandler);
        }
    }

    private static class ResultCountingInterceptor implements ResultAwareMessageDispatchInterceptor<Message<?>, Message<?>> {

        private final AtomicInteger counter = new AtomicInteger();
        private final AtomicBoolean completed = new AtomicBoolean(false);
        private final AtomicReference<Throwable> exception = new AtomicReference<>();

        @Override
        public void dispatch(Message<?> message, ResultHandler<Message<?>, Message<?>> resultHandler, MessageDispatchInterceptorChain chain) throws Exception {
            chain.proceed(message, new ResultHandler<Message<?>, Message<?>>() {

                @Override
                public void onResult(Message<?> message, Message<?> result) {
                    counter.incrementAndGet();
                    resultHandler.onResult(message, result);
                }

                @Override
                public void onComplete(Message<?> message) {
                    completed.set(true);
                    resultHandler.onComplete(message);
                }

                @Override
                public void onError(Message<?> message, Throwable error) {
                    exception.set(error);
                    resultHandler.onError(message, error);
                }
            });
        }
    }
}