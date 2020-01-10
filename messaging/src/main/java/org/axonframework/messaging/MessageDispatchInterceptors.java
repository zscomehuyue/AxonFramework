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

import org.axonframework.common.Registration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

public class MessageDispatchInterceptors<M extends Message<?>, R extends Message<?>> implements MessageDispatchInterceptorSupport<M> {

    private static final ResultHandlerAdapter<Message<?>, Message<?>> NOOP_RESULT_HANDLER = new ResultHandlerAdapter<>();
    private final CopyOnWriteArrayList<ResultAwareMessageDispatchInterceptor<? super M, ? super R>> interceptors = new CopyOnWriteArrayList<>();

    public <T extends M > T preProcess(T message) {
        return preProcess(Collections.singletonList(message)).get(0);
    }

    @SuppressWarnings("unchecked")
    public <T extends M> List<T> preProcess(List<T> messages) {
        if (interceptors.isEmpty()) {
            return messages;
        }

        Iterator<ResultAwareMessageDispatchInterceptor<? super M, ? super R>> iterator = interceptors.iterator();
        List<T> result = new ArrayList<>(messages);
        while (iterator.hasNext()) {
            BiFunction<Integer, ? super M, ? super M> f = iterator.next().handle(result);
            for (int i = 0; i < result.size(); i++) {
                if (f != null) {
                    result.set(i, (T) f.apply(i, result.get(i)));
                }
            }
        }
        return result;
    }

    public <T extends M, U extends R> void intercept(T message, BiConsumer<M, ResultHandler<M, R>> action) {
        intercept(message, NOOP_RESULT_HANDLER, action);
    }

    @SuppressWarnings("unchecked")
    public <T extends M, U extends R> void intercept(T message, ResultHandler<? super T, ? super U> resultHandler, BiConsumer<M, ResultHandler<M, R>> action) {
        if (interceptors.isEmpty()) {
            // shortcut for performance reasons
            action.accept(message, (ResultHandler<M, R>) resultHandler);
        } else {
            List<ResultAwareMessageDispatchInterceptor<? super M, ? super R>> chain = new ArrayList<>(interceptors);
            proceed(message, action, resultHandler, chain, 0);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T extends M, U extends R> void proceed(T message, BiConsumer<M, ResultHandler<M, R>> action,
                                       ResultHandler<? super T, ? super U> resultHandler,
                                       List<ResultAwareMessageDispatchInterceptor<? super M, ? super R>> interceptors,
                                       int index) {
        if (interceptors.size() > index) {
            ResultAwareMessageDispatchInterceptor<? super M, ? super R> interceptor = interceptors.get(index);
            try {
                interceptor.dispatch(message, (ResultHandler) resultHandler, (m, r) -> proceed((T)m, action, (ResultHandler) r, interceptors, index+1));
            } catch (Exception e) {
                resultHandler.onError(message, e);
            }
        } else {
            action.accept(message, (ResultHandler)resultHandler);
        }
    }

    @Override
    public Registration registerDispatchInterceptor(MessageDispatchInterceptor<? super M> dispatchInterceptor) {
        ResultAwareMessageDispatchInterceptor<? super M, ? super R> adapted = ResultAwareMessageDispatchInterceptorWrapper.adapt(dispatchInterceptor);
        interceptors.add(adapted);
        return () -> interceptors.remove(adapted);
    }

    private static class ResultAwareMessageDispatchInterceptorWrapper<T extends Message<?>, R extends Message<?>> implements ResultAwareMessageDispatchInterceptor<T, R> {

        private final MessageDispatchInterceptor<T> dispatchInterceptor;

        private ResultAwareMessageDispatchInterceptorWrapper(MessageDispatchInterceptor<T> dispatchInterceptor) {
            this.dispatchInterceptor = dispatchInterceptor;
        }

        private static <T extends Message<?>, R extends Message<?>> ResultAwareMessageDispatchInterceptor<T, R> adapt(MessageDispatchInterceptor<T> dispatchInterceptor) {
            if (dispatchInterceptor instanceof ResultAwareMessageDispatchInterceptor) {
                return (ResultAwareMessageDispatchInterceptor<T, R>) dispatchInterceptor;
            }
            return new ResultAwareMessageDispatchInterceptorWrapper<>(dispatchInterceptor);
        }

        @Override
        public void dispatch(T message, ResultHandler<T, R> resultHandler, MessageDispatchInterceptorChain chain) throws Exception {
            chain.proceed(message, resultHandler);
        }

        @Override
        public T handle(T message) {
            return dispatchInterceptor.handle(message);
        }

        @Override
        public BiFunction<Integer, T, T> handle(List<? extends T> messages) {
            return dispatchInterceptor.handle(messages);
        }

    }
}
