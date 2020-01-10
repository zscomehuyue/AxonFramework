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

public class ResultHandlerAdapter<M extends Message<?>, R extends Message<?>> implements ResultHandler<M, R> {

    private final ResultHandler<? super M, ? super R> delegate;

    public ResultHandlerAdapter() {
        this.delegate = new NoopResultHandler();
    }

    public ResultHandlerAdapter(ResultHandler<? super M, ? super R> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onResult(M message, R result) {
        delegate.onResult(message, result);
    }

    @Override
    public void onComplete(M message) {
        delegate.onComplete(message);
    }

    @Override
    public void onError(M message, Throwable error) {
        delegate.onError(message, error);
    }

    private static class NoopResultHandler implements ResultHandler<Message<?>, Message<?>> {
        @Override
        public void onResult(Message<?> message, Message<?> result) {

        }

        @Override
        public void onComplete(Message<?> message) {

        }

        @Override
        public void onError(Message<?> m, Throwable error) {

        }

    }
}
