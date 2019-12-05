/*
 * Copyright 2017-2019 the original author or authors.
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
package org.springframework.events.mongodb;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.ApplicationListener;
import org.springframework.context.PayloadApplicationEvent;
import org.springframework.events.CompletableEventPublication;
import org.springframework.events.EventPublication;
import org.springframework.events.EventPublicationRegistry;
import org.springframework.events.EventSerializer;
import org.springframework.events.PublicationTargetIdentifier;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * MongoDB based {@link EventPublicationRegistry}.
 * 
 * @author Felix Jordan
 */
@Slf4j
@RequiredArgsConstructor
public class MongoEventPublicationRegistry implements EventPublicationRegistry, DisposableBean {
    
    private final @NonNull MongoEventPublicationRepository events;
    private final @NonNull EventSerializer serializer;
    
    /*
     * (non-Javadoc)
     * @see org.springframework.events.EventPublicationRegistry#store(java.lang.Object, java.util.Collection)
     */
    @Override
    public void store(Object event, Collection<ApplicationListener<?>> listeners) {
        
        listeners.stream() //
                .map(it -> PublicationTargetIdentifier.forListener(it)) //
                .map(it -> CompletableEventPublication.of(event, it)) //
                .map(this::map) //
                .forEach(it -> events.save(it));
        
        log.info("Stored event publication successfully");
    }
    
    /*
     * (non-Javadoc)
     * @see org.springframework.events.EventPublicationRegistry#findIncompletePublications()
     */
    @Override
    public Iterable<EventPublication> findIncompletePublications() {
        
        List<EventPublication> result = events.findByCompletionDateIsNull().stream() //
                .map(it -> MongoEventPublicationAdapter.of(it, serializer)) //
                .collect(Collectors.toList());
        
        return result;
    }
    
    /*
     * (non-Javadoc)
     * @see org.springframework.events.EventPublicationRegistry#markCompleted(java.lang.Object, org.springframework.events.ListenerId)
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markCompleted(Object event, PublicationTargetIdentifier listener) {
        
        Assert.notNull(event, "Domain event must not be null!");
        Assert.notNull(listener, "Listener identifier must not be null!");
        
        /*
         *  TODO Move this code to a higher layer (PersistentApplicationEventMulticaster or
         *  EventPublicationRegistry), since this problem is not specific for MongoDB.
         */
        /*
         * They stored event is always the actual event, so we have to extract the payload in case we
         * have a PayloadApplicationEvent here!
         */
        Object actualEvent = event instanceof PayloadApplicationEvent
                ? ((PayloadApplicationEvent<?>) event).getPayload()
                : event;
        
        events.findBySerializedEventAndListenerId(serializer.serialize(actualEvent), listener.toString()) //
                .map(MongoEventPublicationRegistry::logCompleted) //
                .ifPresent(it -> events.save(it.markCompleted()));
    }
    
    /*
     * (non-Javadoc)
     * @see org.springframework.beans.factory.DisposableBean#destroy()
     */
    @Override
    public void destroy() throws Exception {
        
        List<MongoEventPublication> outstandingPublications = events.findByCompletionDateIsNull();
        
        if (outstandingPublications.isEmpty()) {
            
            log.debug("No publications outstanding!");
            return;
        }
        
        log.debug("Shutting down with the following publications left unfinished:");
        
        outstandingPublications
                .forEach(it -> log.debug("\t{} - {} - {}", it.getId(), it.getEventType(), it.getListenerId()));
    }
    
    private MongoEventPublication map(EventPublication publication) {
        
        MongoEventPublication result = MongoEventPublication.builder() //
                .eventType(publication.getEvent().getClass().getName()) //
                .publicationDate(publication.getPublicationDate()) //
                .listenerId(publication.getTargetIdentifier().toString()) //
                .serializedEvent(serializer.serialize(publication.getEvent())) //
                .build();
        
        log.debug("Registering publication of {} with id {} for {}.", //
                result.getEventType(), result.getId(), result.getListenerId());
        
        return result;
    }
    
    private static MongoEventPublication logCompleted(MongoEventPublication publication) {
        
        log.debug("Marking publication of event {} with id {} to listener {} completed.", //
                publication.getEventType(), publication.getId(), publication.getListenerId());
        
        return publication;
    }
    
    @EqualsAndHashCode
    @RequiredArgsConstructor(staticName = "of")
    static class MongoEventPublicationAdapter implements EventPublication {
        
        private final MongoEventPublication publication;
        private final EventSerializer serializer;
        
        /*
         * (non-Javadoc)
         * @see org.springframework.events.EventPublication#getEvent()
         */
        @Override
        public Object getEvent() {
            
            
            return serializer.deserialize(publication.getSerializedEvent(), getEventType());
        }
        
        private Class<?> getEventType() {
            String eventClassName = publication.getEventType();
            try {
                /*
                 * TODO Do we get some problems with class loaders here?
                 */
                return Class.forName(eventClassName);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Cannot find event class: " + eventClassName, e);
            }
        }
        
        /*
         * (non-Javadoc)
         * @see org.springframework.events.EventPublication#getListenerId()
         */
        @Override
        public PublicationTargetIdentifier getTargetIdentifier() {
            return PublicationTargetIdentifier.of(publication.getListenerId());
        }
        
        /*
         * (non-Javadoc)
         * @see org.springframework.events.EventPublication#getPublicationDate()
         */
        @Override
        public Instant getPublicationDate() {
            return publication.getPublicationDate();
        }
        
    }
    
}
