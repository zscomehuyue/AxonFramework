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

import java.util.List;
import java.util.function.BiFunction;

public interface ResultAwareMessageDispatchInterceptor<T extends Message<?>, R extends Message<?>> extends MessageDispatchInterceptor<T> {

    void dispatch(T message, ResultHandler<T, R> resultHandler, MessageDispatchInterceptorChain chain) throws Exception;

    @Deprecated
    @Override
    default T handle(T message) {
        return message;
    }

    @Deprecated
    @Override
    default BiFunction<Integer, T, T> handle(List<? extends T> messages) {
        return (i, m) -> m;
    }

}
