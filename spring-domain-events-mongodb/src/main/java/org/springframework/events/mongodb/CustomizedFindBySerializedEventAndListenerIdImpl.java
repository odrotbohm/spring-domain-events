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

import java.util.Optional;

import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MongoWriter;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.stereotype.Component;

/**
 * Provides a custom implementation of
 * {@link MongoEventPublicationRepository#findBySerializedEventAndListenerId(Object, String)}.<br>
 * <br>
 * The query needs to do an exact match of the given event and the stored
 * serializedEvent. The serializedEvent in the database contain a '_class'
 * field to store type information. But the normal Spring Data
 * implementation of the query method does not include this field in the
 * query.<br>
 * <br>
 * In this implementation we force the field to be included by passing
 * {@code ClassTypeInformation.OBJECT} to the
 * {@link MongoWriter#convertToMongoType(Object, org.springframework.data.util.TypeInformation)}
 * method. Since Object type will always differ from the actual event type the
 * {@code MongoWriter} adds type information ('_class' field) per definition.
 * 
 * @author Felix Jordan
 */
@Component
public class CustomizedFindBySerializedEventAndListenerIdImpl implements CustomizedFindBySerializedEventAndListenerId {
    
    private MongoOperations mongoOperations;
    private MongoWriter<?> writer;
    
    public CustomizedFindBySerializedEventAndListenerIdImpl(MongoOperations mongoOperations, MongoWriter<?> writer) {
        this.mongoOperations = mongoOperations;
        this.writer = writer;
    }
    
    @Override
    public Optional<MongoEventPublication> findBySerializedEventAndListenerId(Object event, String listenerId) {
        
        /*
         *  Pass typeInformation for java.lang.Object here. This forces MongoDB to add '_class' attribute
         *  to query which is necessary for exact object match. 
         */
        Object convertedEvent = this.writer.convertToMongoType(event, ClassTypeInformation.OBJECT);
        
        Query query = new Query();
        query.addCriteria(Criteria.where("serializedEvent").is(convertedEvent));
        query.addCriteria(Criteria.where("listenerId").is(listenerId));
        
        return this.mongoOperations.query(MongoEventPublication.class).matching(query).one();
    }
    
}
