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
import org.springframework.data.spanner.core.mapping.BasicSpannerPersistentEntity;
import org.springframework.data.spanner.core.mapping.SpannerMappingContext;
import org.springframework.data.spanner.core.mapping.SpannerResultSetMapper;
import org.springframework.data.spanner.core.mapping.SpannerStructObjectMapper;

import java.util.List;

/**
 * Created by rayt on 3/20/17.
 */
public class SpannerReadContextTemplate {
  private final SpannerMappingContext mappingContext;
  private final SpannerStructObjectMapper objectMapper;
  private final SpannerResultSetMapper resultSetMapper;

  public SpannerReadContextTemplate(SpannerMappingContext mappingContext, SpannerStructObjectMapper objectMapper) {
    this.mappingContext = mappingContext;
    this.objectMapper = objectMapper;
    this.resultSetMapper = new SpannerResultSetMapper(objectMapper);
  }

  public <T> List<T> findAll(ReadContext readContext, Class<T> entityClass, Options.ReadOption... options) {
    return this.find(readContext, entityClass, KeySet.all(), options);
  }

  public <T> List<T> find(ReadContext readContext, Class<T> entityClass, Statement statement, Options.QueryOption... options) {
    ResultSet resultSet = readContext.executeQuery(statement, options);
    return this.resultSetMapper.mapToUnmodifiableList(resultSet, entityClass);
  }

  public <T> List<T> find(ReadContext readContext, Class<T> entityClass, KeySet keys, Options.ReadOption... options) {
    BasicSpannerPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entityClass);
    ResultSet resultSet = readContext.read(persistentEntity.tableName(), keys, persistentEntity.columns(), options);
    return this.resultSetMapper.mapToUnmodifiableList(resultSet, entityClass);
  }

  public <T> T find(ReadContext readContext, Class<T> entityClass, Key key) {
    BasicSpannerPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entityClass);
    T object = BeanUtils.instantiate(entityClass);
    Struct row = readContext.readRow(persistentEntity.tableName(), key, persistentEntity.columns());
    objectMapper.map(row, object);
    return object;
  }
}
