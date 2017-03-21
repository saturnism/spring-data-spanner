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
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.spanner.core.mapping.BasicSpannerPersistentEntity;
import org.springframework.data.spanner.core.mapping.SpannerMappingContext;
import org.springframework.data.spanner.core.mapping.SpannerMutationFactory;
import org.springframework.data.spanner.core.mapping.SpannerStructObjectMapper;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by rayt on 3/20/17.
 */
public class SpannerTemplate implements SpannerOperations, ApplicationContextAware {
  private ApplicationContext applicationContext;

  private final DatabaseClient databaseClient;
  private final SpannerMappingContext mappingContext;
  private final SpannerStructObjectMapper objectMapper;
  private final SpannerMutationFactory mutationFactory;

  public SpannerTemplate(DatabaseClient databaseClient, SpannerMappingContext mappingContext) {
    this.databaseClient = databaseClient;
    this.mappingContext = mappingContext;

    this.objectMapper = new SpannerStructObjectMapper(mappingContext);
    this.mutationFactory = new SpannerMutationFactory(mappingContext);
  }

  public DatabaseClient getDatabaseClient() {
    return this.databaseClient;
  }

  @Override
  public <T> List<T> find(Class<T> entityClass, Statement statement, Options.QueryOption... options) {
    ResultSet rs = this.databaseClient.readOnlyTransaction().executeQuery(statement, options);
    List<T> list = new LinkedList<T>();
    while (rs.next()) {
      T object = BeanUtils.instantiate(entityClass);
      objectMapper.map(rs.getCurrentRowAsStruct(), object);
      list.add(object);
    }
    return Collections.unmodifiableList(list);
  }

  @Override
  public <T> List<T> findAll(Class<T> entityClass, Options.QueryOption ... options) {
    BasicSpannerPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entityClass);
    String tableName = persistentEntity.tableName();

    Statement statement = Statement.of(String.format("select * from %s", tableName));
    return this.find(entityClass, statement, options);
  }

  @Override
  public void insert(Object object) {
    Mutation mutation = mutationFactory.insert(object);
    this.databaseClient.write(Arrays.asList(mutation));
  }

  @Override
  public void update(Object object) {
    Mutation mutation = mutationFactory.update(object);
    this.databaseClient.write(Arrays.asList(mutation));
  }

  @Override
  public void upsert(Object object) {
    Mutation mutation = mutationFactory.upsert(object);
    this.databaseClient.write(Arrays.asList(mutation));
  }

  @Override
  public void delete(Class<?> entityClass, Key key) {
    BasicSpannerPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entityClass);
    String tableName = persistentEntity.tableName();
    Mutation mutation = Mutation.delete(tableName, key);
    this.databaseClient.write(Arrays.asList(mutation));
  }

  @Override
  public void delete(Object object) {
    Mutation mutation = this.mutationFactory.delete(object);
    this.databaseClient.write(Arrays.asList(mutation));
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.applicationContext = applicationContext;
  }
}
