/*
 * Copyright (c) 2010-2018. Axon Framework
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

package org.axonframework.eventhandling;

import org.axonframework.common.Assert;
import org.axonframework.common.AxonConfigurationException;
import org.axonframework.common.Registration;
import org.axonframework.messaging.EventPublicationFailedException;
import org.axonframework.messaging.Message;
import org.axonframework.messaging.MessageDispatchInterceptor;
import org.axonframework.messaging.MessageDispatchInterceptorChain;
import org.axonframework.messaging.MessageDispatchInterceptors;
import org.axonframework.messaging.ResultAwareMessageDispatchInterceptor;
import org.axonframework.messaging.ResultHandler;
import org.axonframework.messaging.unitofwork.CurrentUnitOfWork;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.axonframework.monitoring.MessageMonitor;
import org.axonframework.monitoring.NoOpMessageMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

import static org.axonframework.common.BuilderUtils.assertNonNull;
import static org.axonframework.messaging.unitofwork.UnitOfWork.Phase.AFTER_COMMIT;
import static org.axonframework.messaging.unitofwork.UnitOfWork.Phase.COMMIT;
import static org.axonframework.messaging.unitofwork.UnitOfWork.Phase.PREPARE_COMMIT;

/**
 * Base class for the Event Bus. In case events are published while a Unit of Work is active the Unit of Work root
 * coordinates the timing and order of the publication.
 * <p>
 * This implementation of the {@link EventBus} directly forwards all published events (in the callers' thread) to
 * subscribed event processors.
 *
 * @author Allard Buijze
 * @author René de Waele
 * @since 3.0
 */
public abstract class AbstractEventBus implements EventBus {

    private static final Logger logger = LoggerFactory.getLogger(AbstractEventBus.class);

    private final String eventsKey = this + "_EVENTS";
    private final Set<Consumer<List<? extends EventMessage<?>>>> eventProcessors = new CopyOnWriteArraySet<>();
    private final MessageDispatchInterceptors<EventMessage<?>, Message<?>> dispatchInterceptors = new MessageDispatchInterceptors<>();

    /**
     * Instantiate an {@link AbstractEventBus} based on the fields contained in the {@link Builder}.
     *
     * @param builder the {@link Builder} used to instantiate an {@link AbstractEventBus} instance
     */
    protected AbstractEventBus(Builder builder) {
        builder.validate();
        MessageMonitor<? super EventMessage<?>> messageMonitor = builder.messageMonitor;
        dispatchInterceptors.registerDispatchInterceptor(new MessageMonitorDispatchInterceptor<>(messageMonitor));
    }

    @Override
    public Registration subscribe(Consumer<List<? extends EventMessage<?>>> eventProcessor) {
        if (this.eventProcessors.add(eventProcessor)) {
            if (logger.isDebugEnabled()) {
                logger.debug("EventProcessor [{}] subscribed successfully", eventProcessor);
            }
        } else {
            logger.info("EventProcessor [{}] not added. It was already subscribed", eventProcessor);
        }
        return () -> {
            if (eventProcessors.remove(eventProcessor)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("EventListener {} unsubscribed successfully", eventProcessor);
                }
                return true;
            } else {
                logger.info("EventListener {} not removed. It was already unsubscribed", eventProcessor);
                return false;
            }
        };
    }

    /**
     * {@inheritDoc}
     * <p/>
     * In case a Unit of Work is active, the {@code preprocessor} is not invoked by this Event Bus until the Unit
     * of Work root is committed.
     *
     * @param dispatchInterceptor
     */
    @Override
    public Registration registerDispatchInterceptor(
            MessageDispatchInterceptor<? super EventMessage<?>> dispatchInterceptor) {
        return dispatchInterceptors.registerDispatchInterceptor(dispatchInterceptor);
    }

    @Override
    public void publish(List<? extends EventMessage<?>> events) {
        List<EventMessage<?>> interceptedEvents = new ArrayList<>();
        List<PublicationResult> results = new ArrayList<>();
        events.forEach(m -> {
            dispatchInterceptors.intercept(m, (im, r) -> {
                interceptedEvents.add(im);
                results.add(new PublicationResult(im, r));
            });
        });
        if (CurrentUnitOfWork.isStarted()) {
            UnitOfWork<?> unitOfWork = CurrentUnitOfWork.get();
            Assert.state(!unitOfWork.phase().isAfter(PREPARE_COMMIT),
                         () -> "It is not allowed to publish events when the current Unit of Work has already been " +
                                 "committed. Please start a new Unit of Work before publishing events.");
            Assert.state(!unitOfWork.root().phase().isAfter(PREPARE_COMMIT),
                         () -> "It is not allowed to publish events when the root Unit of Work has already been " +
                                 "committed.");

            unitOfWork.afterCommit(u -> results.forEach(PublicationResult::onComplete));
            unitOfWork.onRollback(uow -> {
                EventPublicationFailedException error = new EventPublicationFailedException("Event Publication was cancelled due to Unit of Work rollback", uow.getExecutionResult().getExceptionResult());
                results.forEach(r -> r.onError(error));
            });
            eventsQueue(unitOfWork).addAll(interceptedEvents);
        } else {
            try {
                prepareCommit(intercept(interceptedEvents));
                commit(interceptedEvents);
                afterCommit(interceptedEvents);
                results.forEach(PublicationResult::onComplete);
            } catch (Exception e) {
                results.forEach(m -> m.onError(e));
                throw e;
            }
        }
    }

    private List<EventMessage<?>> eventsQueue(UnitOfWork<?> unitOfWork) {
        return unitOfWork.getOrComputeResource(eventsKey, r -> {

            List<EventMessage<?>> eventQueue = new ArrayList<>();

            unitOfWork.onPrepareCommit(u -> {
                if (u.parent().isPresent() && !u.parent().get().phase().isAfter(PREPARE_COMMIT)) {
                    eventsQueue(u.parent().get()).addAll(eventQueue);
                } else {
                    int processedItems = eventQueue.size();
                    doWithEvents(this::prepareCommit, intercept(eventQueue));
                    // Make sure events published during publication prepare commit phase are also published
                    while (processedItems < eventQueue.size()) {
                        List<? extends EventMessage<?>> newMessages =
                                intercept(eventQueue.subList(processedItems, eventQueue.size()));
                        processedItems = eventQueue.size();
                        doWithEvents(this::prepareCommit, newMessages);
                    }
                }
            });
            unitOfWork.onCommit(u -> {
                if (u.parent().isPresent() && !u.root().phase().isAfter(COMMIT)) {
                    u.root().onCommit(w -> doWithEvents(this::commit, eventQueue));
                } else {
                    doWithEvents(this::commit, eventQueue);
                }
            });
            unitOfWork.afterCommit(u -> {
                if (u.parent().isPresent() && !u.root().phase().isAfter(AFTER_COMMIT)) {
                    u.root().afterCommit(w -> doWithEvents(this::afterCommit, eventQueue));
                } else {
                    doWithEvents(this::afterCommit, eventQueue);
                }
            });
            unitOfWork.onCleanup(u -> u.resources().remove(eventsKey));
            return eventQueue;
        });
    }

    /**
     * Returns a list of all the events staged for publication in this Unit of Work. Changing this list will
     * not affect the publication of events.
     *
     * @return a list of all the events staged for publication
     */
    protected List<EventMessage<?>> queuedMessages() {
        if (!CurrentUnitOfWork.isStarted()) {
            return Collections.emptyList();
        }
        List<EventMessage<?>> messages = new ArrayList<>();
        for (UnitOfWork<?> uow = CurrentUnitOfWork.get(); uow != null; uow = uow.parent().orElse(null)) {
            messages.addAll(0, uow.getOrDefaultResource(eventsKey, Collections.emptyList()));
        }
        return messages;
    }

    /**
     * Invokes all the dispatch interceptors.
     *
     * @param events The original events being published
     * @return The events to actually publish
     */
    protected List<? extends EventMessage<?>> intercept(List<? extends EventMessage<?>> events) {
        return dispatchInterceptors.preProcess(events);
    }

    private void doWithEvents(Consumer<List<? extends EventMessage<?>>> eventsConsumer,
                              List<? extends EventMessage<?>> events) {
        eventsConsumer.accept(events);
    }

    /**
     * Process given {@code events} while the Unit of Work root is preparing for commit. The default implementation
     * signals the registered {@link MessageMonitor} that the given events are ingested and passes the events to each
     * registered event processor.
     *
     * @param events Events to be published by this Event Bus
     */
    protected void prepareCommit(List<? extends EventMessage<?>> events) {
        eventProcessors.forEach(eventProcessor -> eventProcessor.accept(events));
    }

    /**
     * Process given {@code events} while the Unit of Work root is being committed. The default implementation does
     * nothing.
     *
     * @param events Events to be published by this Event Bus
     */
    protected void commit(List<? extends EventMessage<?>> events) {
    }

    /**
     * Process given {@code events} after the Unit of Work has been committed. The default implementation does
     * nothing.
     *
     * @param events Events to be published by this Event Bus
     */
    protected void afterCommit(List<? extends EventMessage<?>> events) {
    }

    /**
     * Abstract Builder class to instantiate {@link AbstractEventBus} implementations.
     * <p>
     * The {@link MessageMonitor} is defaulted to an {@link NoOpMessageMonitor}.
     */
    public abstract static class Builder {

        private MessageMonitor<? super EventMessage<?>> messageMonitor = NoOpMessageMonitor.INSTANCE;

        /**
         * Sets the {@link MessageMonitor} to monitor ingested {@link EventMessage}s. Defaults to a
         * {@link NoOpMessageMonitor}.
         *
         * @param messageMonitor a {@link MessageMonitor} to monitor ingested {@link EventMessage}s
         * @return the current Builder instance, for fluent interfacing
         */
        public Builder messageMonitor(MessageMonitor<? super EventMessage<?>> messageMonitor) {
            assertNonNull(messageMonitor, "MessageMonitor may not be null");
            this.messageMonitor = messageMonitor;
            return this;
        }

        /**
         * Validates whether the fields contained in this Builder are set accordingly.
         *
         * @throws AxonConfigurationException if one field is asserted to be incorrect according to the Builder's
         *                                    specifications
         */
        protected void validate() throws AxonConfigurationException {
            // Kept to be overridden
        }
    }

    private static class MessageMonitorDispatchInterceptor<M extends Message<?>> implements ResultAwareMessageDispatchInterceptor<M, Message<?>> {

        private final MessageMonitor<? super M> messageMonitor;

        public MessageMonitorDispatchInterceptor(MessageMonitor<? super M> messageMonitor) {
            this.messageMonitor = messageMonitor;
        }

        @Override
        public void dispatch(M message, ResultHandler<M, Message<?>> resultHandler, MessageDispatchInterceptorChain chain) {
            MessageMonitor.MonitorCallback callback = messageMonitor.onMessageIngested(message);
            chain.proceed(message, resultHandler.andOnError((m, e) -> callback.reportFailure(e))
                                                .andOnComplete(m -> callback.reportSuccess())
            );
        }
    }

    private static class PublicationResult {
        private final EventMessage<?> publishedMessage;
        private final ResultHandler<EventMessage<?>, Message<?>> resultHandler;

        public PublicationResult(EventMessage<?> publishedMessage, ResultHandler<EventMessage<?>, Message<?>> resultHandler) {

            this.publishedMessage = publishedMessage;
            this.resultHandler = resultHandler;
        }

        public void onComplete() {
            resultHandler.onComplete(publishedMessage);
        }

        public void onError(Throwable error) {
            resultHandler.onError(publishedMessage, error);
        }
    }
}
