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

import org.springframework.beans.factory.ObjectFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.events.EventSerializer;
import org.springframework.events.mongodb.support.NoEventSerializerBeanCondition;

import lombok.RequiredArgsConstructor;

/**
 * @author Felix Jordan
 */
@Configuration(proxyBeanMethods = false)
@RequiredArgsConstructor
@EnableMongoRepositories
public class MongoEventPublicationConfiguration {
    
    private final MongoEventPublicationRepository repository;
    private final ObjectFactory<EventSerializer> serializer;
    
    @Bean
    public MongoEventPublicationRegistry mongoEventPublicationRegistry() {
        return new MongoEventPublicationRegistry(repository, serializer.getObject());
    }
    
    @Bean
    @Conditional(NoEventSerializerBeanCondition.class)
    public EventSerializer mongoEventSerializer() {
        return new MongoEventSerializer();
    }
    
}
