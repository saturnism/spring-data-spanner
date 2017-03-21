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

package org.springframework.data.spanner.core;

import com.google.cloud.spanner.*;
import org.springframework.data.spanner.core.mapping.BasicSpannerPersistentEntity;
import org.springframework.data.spanner.core.mapping.SpannerMappingContext;
import org.springframework.data.spanner.core.mapping.SpannerMutationFactory;

import java.util.List;

/**
 * Created by rayt on 3/20/17.
 */
public class SpannerTransactionContext {
  private final TransactionContext transactionContext;
  private final SpannerReadContextTemplate readContextTemplate;
  private final SpannerMappingContext mappingContext;
  private final SpannerMutationFactory mutationFactory;

  public SpannerTransactionContext(TransactionContext transactionContext, SpannerReadContextTemplate readContextTemplate, SpannerMappingContext mappingContext, SpannerMutationFactory mutationFactory) {
    this.transactionContext = transactionContext;
    this.readContextTemplate = readContextTemplate;
    this.mappingContext = mappingContext;
    this.mutationFactory = mutationFactory;
  }

  public <T> List<T> find(Class<T> entityClass, Statement statement, Options.QueryOption ... options) {
    return this.readContextTemplate.find(this.transactionContext, entityClass, statement, options);
  }

  public void insert(Object object) {
    Mutation mutation = mutationFactory.insert(object);
    this.transactionContext.buffer(mutation);
  }

  public void update(Object object, String ... properties) {
    Mutation mutation = mutationFactory.update(object, properties);
    this.transactionContext.buffer(mutation);
  }

  public void upsert(Object object) {
    Mutation mutation = mutationFactory.upsert(object);
    this.transactionContext.buffer(mutation);
  }

  public void delete(Class<?> entityClass, Key key) {
    BasicSpannerPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entityClass);
    String tableName = persistentEntity.tableName();
    Mutation mutation = Mutation.delete(tableName, key);
    this.transactionContext.buffer(mutation);
  }

  public void delete(Object object) {
    Mutation mutation = this.mutationFactory.delete(object);
    this.transactionContext.buffer(mutation);
  }

}
