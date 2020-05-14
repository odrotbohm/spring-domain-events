/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.events.support;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.function.Supplier;

import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.PayloadApplicationEvent;
import org.springframework.context.event.AbstractApplicationEventMulticaster;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.core.ResolvableType;
import org.springframework.events.EventPublication;
import org.springframework.events.EventPublicationRegistry;
import org.springframework.events.PublicationTargetIdentifier;
import org.springframework.transaction.event.TransactionalEventListenerMetadata;
import org.springframework.transaction.event.TransactionalEventListenerMetadata.ErrorHandler;
import org.springframework.transaction.event.TransactionalEventListeners;

/**
 * An {@link ApplicationEventMulticaster} to register {@link EventPublication}s in an {@link EventPublicationRegistry}
 * so that potentially failing transactional event listeners can get re-invoked upon application restart or via a
 * schedule.
 * <p>
 * Republication is handled in {@link #afterSingletonsInstantiated()} inspecting the {@link EventPublicationRegistry}
 * for incomplete publications and
 *
 * @author Oliver Drotbohm
 * @see CompletionRegisteringBeanPostProcessor
 */
@Slf4j
@RequiredArgsConstructor
public class PersistentApplicationEventMulticaster extends AbstractApplicationEventMulticaster
		implements SmartInitializingSingleton {

	private final @NonNull Supplier<EventPublicationRegistry> registry;
	private ErrorHandler errorHander;

	public void setErrorHander(ErrorHandler errorHander) {
		this.errorHander = errorHander;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.event.ApplicationEventMulticaster#multicastEvent(org.springframework.context.ApplicationEvent)
	 */
	@Override
	public void multicastEvent(ApplicationEvent event) {
		multicastEvent(event, ResolvableType.forInstance(event));
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.event.ApplicationEventMulticaster#multicastEvent(org.springframework.context.ApplicationEvent, org.springframework.core.ResolvableType)
	 */
	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void multicastEvent(ApplicationEvent event, ResolvableType eventType) {

		ResolvableType type = eventType == null ? ResolvableType.forInstance(event) : eventType;
		Collection<ApplicationListener<?>> listeners = getApplicationListeners(event, type);

		if (listeners.isEmpty()) {
			return;
		}

		TransactionalEventListeners txListeners = new TransactionalEventListeners(listeners);
		Object eventToPersist = getEventToPersist(event);

		registry.get().store(eventToPersist, txListeners.stream() //
				.map(TransactionalEventListenerMetadata::getId) //
				.map(PublicationTargetIdentifier::of));

		for (ApplicationListener listener : listeners) {
			listener.onApplicationEvent(event);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.SmartInitializingSingleton#afterSingletonsInstantiated()
	 */
	@Override
	public void afterSingletonsInstantiated() {

		EventPublicationRegistry eventPublicationRegistry = registry.get();
		TransactionalEventListeners listeners = new TransactionalEventListeners(getApplicationListeners());

		listeners.stream().forEach(it -> {

			it.registerCompletionCallback((event, id) -> {
				eventPublicationRegistry.markCompleted(getEventToPersist(event), PublicationTargetIdentifier.of(id));
			});

			it.registerErrorCallback(getDefaultedErrorHandler());
		});

		for (EventPublication publication : eventPublicationRegistry.findIncompletePublications()) {
			invokeTargetListener(publication, listeners);
		}
	}

	private void invokeTargetListener(EventPublication publication, TransactionalEventListeners listeners) {

		listeners.doWithListener(publication.getTargetIdentifier().toString(), it -> {

			it.processEvent(publication.getApplicationEvent());
			registry.get().markCompleted(publication);
		});

		log.debug("Listener {} not found!", publication.getTargetIdentifier());
	}

	private ErrorHandler getDefaultedErrorHandler() {

		return errorHander != null //
				? errorHander //
				: (event, exception) -> {
					log.info("Failure during transaction event processing of {}. {}", event, exception.getMessage());
					return null;
				};
	}

	private static Object getEventToPersist(ApplicationEvent event) {

		return PayloadApplicationEvent.class.isInstance(event) //
				? ((PayloadApplicationEvent<?>) event).getPayload() //
				: event;
	}
}
