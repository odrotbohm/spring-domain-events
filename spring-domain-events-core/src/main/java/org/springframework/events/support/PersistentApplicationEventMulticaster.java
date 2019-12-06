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
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.PayloadApplicationEvent;
import org.springframework.context.event.AbstractApplicationEventMulticaster;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.context.event.ApplicationListenerMethodAdapter;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.events.CompletableEventPublication;
import org.springframework.events.EventPublication;
import org.springframework.events.EventPublicationRegistry;
import org.springframework.events.PublicationTargetIdentifier;
import org.springframework.transaction.event.TransactionalEventListener;
import org.springframework.util.ConcurrentReferenceHashMap;
import org.springframework.util.ReflectionUtils;

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

	private static final Map<Class<?>, Boolean> TX_EVENT_LISTENERS = new ConcurrentReferenceHashMap<>();
	private static final Field LISTENER_METHOD_FIELD;
	private static final Map<ListenerAndEventTypeKey, Boolean> DECLARED_EVENT_TYPES = new ConcurrentReferenceHashMap<>();
	private static final Field DECLARED_EVENT_TYPES_FIELD;

	static {

		LISTENER_METHOD_FIELD = ReflectionUtils.findField(ApplicationListenerMethodAdapter.class, "method");
		ReflectionUtils.makeAccessible(LISTENER_METHOD_FIELD);
		
		DECLARED_EVENT_TYPES_FIELD = ReflectionUtils.findField(ApplicationListenerMethodAdapter.class, "declaredEventTypes");
		ReflectionUtils.makeAccessible(DECLARED_EVENT_TYPES_FIELD);
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
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void multicastEvent(ApplicationEvent event, ResolvableType eventType) {

		ResolvableType type = eventType == null ? ResolvableType.forInstance(event) : eventType;
		Collection<ApplicationListener<?>> listeners = getApplicationListeners(event, type);

		if (listeners.isEmpty()) {
			return;
		}

		/*
		 * getApplicationListeners(event, type) also returns some transactional event listeners
		 * for events they don't want to handle (e.g. spring-data-mongodb's mapping events).
		 * Normally the ApplicationListenerMethodAdapter ignores the unsupported events, but if
		 * spring-data-mongodb is used to persist domain events this causes a lot of events to be persisted
		 * unnecessarily.
		 * Also the storing of mapping events causes recursive publishing more events as the stored EventPublications
		 * themself also publish mapping events! This results in OutOfMemoryErrors!
		 * 
		 * Solution is to check transactionalListeners here a second time of they support the event.
		 */
		List<ApplicationListener<?>> transactionalListeners = listeners.stream() //
				.filter(PersistentApplicationEventMulticaster::isTransactionalApplicationEventListener) //
				.filter(l -> hasDeclaredEventType(l, type)) //
				.collect(Collectors.toList());

		if (!transactionalListeners.isEmpty()) {

			Object eventToPersist = getEventToPersist(event);

			registry.get().store(eventToPersist, transactionalListeners);
		}

		for (ApplicationListener listener : listeners) {

		    if (transactionalListeners.contains(listener)) {
		        
		        // Handle event publication as before
		        EventPublication publication = CompletableEventPublication.of(event,
	                    PublicationTargetIdentifier.forListener(listener));

	            executeTransactionalListener(publication, listener);
		    } else {
		        
		        // Simply forward event to listener
		        listener.onApplicationEvent(event);
		        
		        /*
		         * For non-transactional listeners reliable event publication is not necessary.
		         * Relevant use-case for persistent events are domain events, and those will always (?)
		         * be handled by a transactional event listener marked with @TransactionalEventListener
		         * 
		         * Apart from that 'PublicationTargetIdentifier.forListener(listener)' will throw an Exception
		         * for all listeners that are not an instance of 'ApplicationListenerMethodAdapter'.
		         * When using spring-domain-events in a 'full' spring-boot application using spring-web, spring-data etc.
		         * the following listeners are 'unsupported' (throw an exception):
		         * 
		         *     org.springframework.boot.devtools.restart.RestartApplicationListener
                 *     org.springframework.boot.context.config.DelegatingApplicationListener
                 *     org.springframework.data.mongodb.core.index.MongoPersistentEntityIndexCreator
                 *     org.springframework.security.context.DelegatingApplicationListener
                 *     org.springframework.boot.devtools.autoconfigure.LocalDevToolsAutoConfiguration$LiveReloadServerEventListener
                 *     org.springframework.boot.autoconfigure.logging.ConditionEvaluationReportLoggingListener$ConditionEvaluationReportListener
                 *     org.springframework.boot.ClearCachesApplicationListener@e9a95de
                 *     org.springframework.boot.autoconfigure.SharedMetadataReaderFactoryContextInitializer$SharedMetadataReaderFactoryBean
                 *     org.springframework.plugin.core.support.PluginRegistryFactoryBean
                 *     org.springframework.web.servlet.resource.ResourceUrlProvider
                 *     org.springframework.plugin.core.support.PluginRegistryFactoryBean
                 *     org.springframework.plugin.core.support.PluginRegistryFactoryBean
                 *     org.springframework.boot.web.context.ServerPortInfoApplicationContextInitializer
                 *     org.springframework.boot.autoconfigure.BackgroundPreinitializer
                 *     org.springframework.boot.devtools.autoconfigure.ConditionEvaluationDeltaLoggingListener
		         */
		        
		        
		    }
		    
			
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.SmartInitializingSingleton#afterSingletonsInstantiated()
	 */
	@Override
	public void afterSingletonsInstantiated() {

		for (EventPublication publication : registry.get().findIncompletePublications()) {
			invokeTargetListener(publication);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void invokeTargetListener(EventPublication publication) {

		for (ApplicationListener listener : getApplicationListeners()) {

		    /*
		     *  Ensure listener is transactional event listener. Otherwise
		     *  PublicationTargetIdentifier.forListener(listener) may throw an exception
		     *  for listeners that are no instance of ApplicationListenerMethodAdapter.
		     *  
		     *  This additional check is perfectly fine as we only save EventPublications
		     *  for transactional listeners anyway (see: multicastEvent(...)).
		     */
			if (isTransactionalApplicationEventListener(listener)
			        && publication.isIdentifiedBy(PublicationTargetIdentifier.forListener(listener))) {

				executeTransactionalListener(publication, listener);
				return;
			}
		}

		log.debug("Listener {} not found!", publication.getTargetIdentifier());
	}

	private void executeTransactionalListener(EventPublication publication,
			ApplicationListener<ApplicationEvent> listener) {

		try {

			listener.onApplicationEvent(publication.getApplicationEvent());
			/*
			 * Do NOT mark as completed here. All transactional event listeners are represented as
			 * an ApplicationListenerMethodTransactionalAdapter which only registers a TransactionSynchronization
			 * for the event to execute the listener later, but not executes immediately.
			 * 
			 * The event publication will be marked complete by the CompletionRegisteringMethodInterceptor
			 * registered through the CompletionRegisteringBeanPostProcessor.
			 */

		} catch (Exception e) {
			// Log
		    // TODO Do we really need to log here since no listener is actually executed.
		}
	}

	private static boolean isTransactionalApplicationEventListener(ApplicationListener<?> listener) {

		Class<?> targetClass = AopUtils.getTargetClass(listener);

		return TX_EVENT_LISTENERS.computeIfAbsent(targetClass, it -> {

			if (!ApplicationListenerMethodAdapter.class.isAssignableFrom(targetClass)) {
				return false;
			}

			Method method = (Method) ReflectionUtils.getField(LISTENER_METHOD_FIELD, listener);

			return AnnotatedElementUtils.hasAnnotation(method, TransactionalEventListener.class);
		});
	}
	
	private static boolean hasDeclaredEventType(ApplicationListener<?> listener, ResolvableType eventType) {
	    
	    Class<?> targetClass = AopUtils.getTargetClass(listener);
	    
	    ListenerAndEventTypeKey key = ListenerAndEventTypeKey.of(targetClass, eventType);
	    
	    return DECLARED_EVENT_TYPES.computeIfAbsent(key, it -> {
	        
	        if (!ApplicationListenerMethodAdapter.class.isAssignableFrom(targetClass)) {
                return false;
            }
	        
	        @SuppressWarnings("unchecked")
            List<ResolvableType> declaredEventTypes = (List<ResolvableType>)
	                ReflectionUtils.getField(DECLARED_EVENT_TYPES_FIELD, listener);
	        
	        return containsEventType(declaredEventTypes, eventType);
	    });
	}
	
	private static boolean containsEventType(List<ResolvableType> eventTypes, ResolvableType type) {
	    if (ResolvableType.forClass(PayloadApplicationEvent.class).isAssignableFrom(type)) {

	        // Custom 'contains' implementation to check against payload event type
            for (ResolvableType declaredType : eventTypes) {
                
                ResolvableType payloadEventType = type.getGeneric(0);
                if (declaredType.isAssignableFrom(payloadEventType)) {
                    return true;
                }
            }
        }
        
        return eventTypes.contains(type);
	}

	private static Object getEventToPersist(ApplicationEvent event) {

		return PayloadApplicationEvent.class.isInstance(event) //
				? ((PayloadApplicationEvent<?>) event).getPayload() //
				: event;
	}
	
	@Value(staticConstructor = "of")
	private static class ListenerAndEventTypeKey {
	    
	    private Class<?> listenerClass;
	    private ResolvableType eventType;
	}
}
