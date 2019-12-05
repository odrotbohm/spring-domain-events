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

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceConstructor;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

/**
 * @author Felix Jordan
 */
@Data
@NoArgsConstructor(force = true)
@RequiredArgsConstructor(onConstructor_={@PersistenceConstructor}, access = AccessLevel.PRIVATE)
public class MongoEventPublication {
    
    private final @Id ObjectId id;
    private final Instant publicationDate;
    private final String listenerId;
    /*
     * We do not limit ourselves here to String representation so that
     * also the possibility exists to store events as embedded documents.
     */
    private final Object serializedEvent;
    /*
     * When using Class<?> we would need to register a custom converter
     * which causes some trouble since there can be only one MongoCustomConversions bean.
     */
    private final String eventType;
    
    private Instant completionDate;
    
    @Builder
    static MongoEventPublication of(Instant publicationDate, String listenerId, Object serializedEvent,
            String eventType) {
        return new MongoEventPublication(ObjectId.get(), publicationDate, listenerId, serializedEvent,
                eventType);
    }
    
    MongoEventPublication markCompleted() {

        this.completionDate = Instant.now();
        return this;
    }
}
