/*
 * Copyright (c) 2010-2018. Axon Framework
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.axonframework.commandhandling.gateway;

import org.axonframework.commandhandling.CommandBus;
import org.axonframework.commandhandling.CommandMessage;
import org.axonframework.commandhandling.callbacks.FailureLoggingCallback;
import org.axonframework.commandhandling.callbacks.FutureCallback;
import org.axonframework.messaging.MessageDispatchInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * Default implementation of {@link ReactiveCommandGateway}.
 *
 * @author Milan Savic
 * @since 5.0
 */
public class DefaultReactiveCommandGateway extends AbstractCommandGateway implements ReactiveCommandGateway {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultReactiveCommandGateway.class);

    /**
     * Initializes a command gateway that dispatches commands to the given {@code commandBus} after they have been
     * handles by the given {@code commandDispatchInterceptors}. Commands will not be retried when command
     * execution fails.
     *
     * @param commandBus                  The CommandBus on which to dispatch the Command Messages
     * @param messageDispatchInterceptors The interceptors to invoke before dispatching commands to the Command Bus
     */
    @SafeVarargs
    public DefaultReactiveCommandGateway(CommandBus commandBus,
                                         MessageDispatchInterceptor<? super CommandMessage<?>>... messageDispatchInterceptors) {
        this(commandBus, null, messageDispatchInterceptors);
    }

    /**
     * Initializes a command gateway that dispatches commands to the given {@code commandBus} after they have been
     * handles by the given {@code commandDispatchInterceptors}. When command execution results in an unchecked
     * exception, the given {@code retryScheduler} is invoked to allow it to retry that command.
     * execution fails.
     *
     * @param commandBus                  The CommandBus on which to dispatch the Command Messages
     * @param retryScheduler              The scheduler that will decide whether to reschedule commands
     * @param messageDispatchInterceptors The interceptors to invoke before dispatching commands to the Command Bus
     */
    @SafeVarargs
    public DefaultReactiveCommandGateway(CommandBus commandBus, RetryScheduler retryScheduler,
                                         MessageDispatchInterceptor<? super CommandMessage<?>>... messageDispatchInterceptors) {
        this(commandBus, retryScheduler, asList(messageDispatchInterceptors));
    }

    /**
     * Initializes a command gateway that dispatches commands to the given {@code commandBus} after they have been
     * handles by the given {@code commandDispatchInterceptors}. When command execution results in an unchecked
     * exception, the given {@code retryScheduler} is invoked to allow it to retry that command.
     * execution fails.
     *
     * @param commandBus                  The CommandBus on which to dispatch the Command Messages
     * @param retryScheduler              The scheduler that will decide whether to reschedule commands
     * @param messageDispatchInterceptors The interceptors to invoke before dispatching commands to the Command Bus
     */
    public DefaultReactiveCommandGateway(CommandBus commandBus,
                                         RetryScheduler retryScheduler,
                                         List<MessageDispatchInterceptor<? super CommandMessage<?>>> messageDispatchInterceptors) {
        super(commandBus, retryScheduler, messageDispatchInterceptors);
    }

    @Override
    public <R> Mono<R> send(Object command) {
        FutureCallback<Object, R> callback = new FutureCallback<>();
        send(command, new FailureLoggingCallback<>(LOGGER, callback));
        return Mono.fromFuture(callback);
    }
}
