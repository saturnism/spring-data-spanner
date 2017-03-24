/*
 * Copyright 2017 Google Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.spanner.repository.support;

import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.querydsl.QueryDslPredicateExecutor;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.spanner.core.SpannerOperations;
import org.springframework.data.spanner.core.mapping.BasicSpannerPersistentEntity;
import org.springframework.data.spanner.core.mapping.SpannerMappingContext;
import org.springframework.data.spanner.core.mapping.SpannerPersistentEntity;

import java.io.Serializable;

import static org.springframework.data.querydsl.QueryDslUtils.QUERY_DSL_PRESENT;

/**
 * Created by rayt on 3/23/17.
 */
public class SpannerRepositoryFactory extends RepositoryFactorySupport {
  private final SpannerOperations operations;
  private final SpannerMappingContext mappingContext;

  public SpannerRepositoryFactory(SpannerOperations operations) {
    this.operations = operations;
    this.mappingContext = operations.getMappingContext();
  }

  @Override
  protected Object getTargetRepository(RepositoryInformation information) {
    SpannerEntityInformation<?, Serializable> entityInformation = getEntityInformation(information.getDomainType(),
        information);
    return getTargetRepositoryViaReflection(information, entityInformation, operations);
  }

  @Override
  public <T, ID extends Serializable> SpannerEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
    return getEntityInformation(domainClass, null);
  }

  @SuppressWarnings("unchecked")
  private <T, ID extends Serializable> SpannerEntityInformation<T, ID> getEntityInformation(Class<T> domainClass,
                                                                                            RepositoryInformation information) {
    SpannerPersistentEntity<?> entity = mappingContext.getPersistentEntity(domainClass);

    if (entity == null) {
      throw new MappingException(
          String.format("Could not lookup mapping metadata for domain class %s!", domainClass.getName()));
    }

    BasicSpannerPersistentEntity<T> persistentEntity = (BasicSpannerPersistentEntity<T>) mappingContext.getPersistentEntity(domainClass);
    return new MappingSpannerEntityInformation<T, ID>(persistentEntity);
  }


  @Override
  protected Class<?> getRepositoryBaseClass(RepositoryMetadata repositoryMetadata) {
    boolean isQueryDslRepository = QUERY_DSL_PRESENT
        && QueryDslPredicateExecutor.class.isAssignableFrom(repositoryMetadata.getRepositoryInterface());

//    return isQueryDslRepository ? QueryDslSpannerRepository.class : SimpleSpannerRepository.class;
    return SimpleSpannerRepository.class;

  }
}
