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

package org.springframework.data.spanner.core.mapping;

import com.google.cloud.spanner.*;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PropertyHandler;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by rayt on 3/14/17.
 */
public class SpannerMutationFactory {
  private final SpannerMappingContext mappingContext;

  public SpannerMutationFactory(SpannerMappingContext mappingContext) {
    this.mappingContext = mappingContext;
  }

  public <T> Mutation insert(T object) {
    return createMutation(Mutation.Op.INSERT, object);
  }

  public <T> Mutation upsert(T object) {
    return createMutation(Mutation.Op.INSERT_OR_UPDATE, object);
  }

  public <T> Mutation replace(T object) {
    return createMutation(Mutation.Op.REPLACE, object);
  }

  public <T> Mutation update(T object, String... properties) {
    return createMutation(Mutation.Op.UPDATE, object, properties);
  }

  public <T> Mutation delete(Class<T> entityClass, Iterable<? extends T> entities) {
    final BasicSpannerPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(entityClass);
    KeySet.Builder builder = KeySet.newBuilder();
    for (T entity : entities) {
      final PersistentPropertyAccessor accessor = persistentEntity.getPropertyAccessor(entity);
      SpannerPersistentProperty idProperty = persistentEntity.getIdProperty();
      Object value = accessor.getProperty(idProperty);
      builder.addKey(Key.of(value));
    }
    return Mutation.delete(persistentEntity.tableName(), builder.build());
  }

  public <T> Mutation delete(T object) {
    final Class<?> entityType = object.getClass();
    final BasicSpannerPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(object.getClass());
    final PersistentPropertyAccessor accessor = persistentEntity.getPropertyAccessor(object);

    final SpannerPersistentProperty idProperty = persistentEntity.getIdProperty();
    Class<?> propertyType = idProperty.getType();
    Object value = accessor.getProperty(idProperty);
    Key key = Key.of(value);

    final Mutation mutation = Mutation.delete(persistentEntity.tableName(), key);
    return mutation;
  }

  public <T> Mutation createMutation(Mutation.Op op, T object, String... properties) {
    final Set<String> includeProperties = new HashSet<>(Arrays.asList(properties));
    final Class<?> entityType = object.getClass();
    final BasicSpannerPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(object.getClass());
    final Mutation.WriteBuilder writeBuilder = writeBuilder(op, persistentEntity.tableName());
    final PersistentPropertyAccessor accessor = persistentEntity.getPropertyAccessor(object);
    persistentEntity.doWithProperties(new PropertyHandler<SpannerPersistentProperty>() {
      @Override
      public void doWithPersistentProperty(SpannerPersistentProperty spannerPersistentProperty) {
        if (!spannerPersistentProperty.isIdProperty() && op == Mutation.Op.UPDATE && !includeProperties.contains(spannerPersistentProperty.getName())) {
          return;
        }
        Object value = accessor.getProperty(spannerPersistentProperty);
        Class<?> propertyType = spannerPersistentProperty.getType();
        ValueBinder<Mutation.WriteBuilder> set = writeBuilder.set(spannerPersistentProperty.getColumnName());
        if (String.class.isAssignableFrom(propertyType)) {
          set.to((String) value);
        } else if (Boolean.class.isAssignableFrom(propertyType)) {
          set.to((Boolean) value);
        } else if (Date.class.isAssignableFrom(propertyType)) {
          set.to((Date) value);
        } else if (Double.class.isAssignableFrom(propertyType)) {
          set.to((Double) value);
        } else if (Long.class.isAssignableFrom(propertyType)) {
          set.to((Long) value);
        } else if (Timestamp.class.isAssignableFrom(propertyType)) {
          set.to((Timestamp) value);
        } else {
          throw new SpannerDataException(String.format("Unsupported mapping for type: %s", value.getClass()));
        }
      }
    });
    return writeBuilder.build();
  }

  protected Mutation.WriteBuilder writeBuilder(Mutation.Op op, String tableName) {
    switch (op) {
      case INSERT:
        return Mutation.newInsertBuilder(tableName);
      case INSERT_OR_UPDATE:
        return Mutation.newInsertOrUpdateBuilder(tableName);
      case UPDATE:
        return Mutation.newUpdateBuilder(tableName);
      case REPLACE:
        return Mutation.newReplaceBuilder(tableName);
    }
    throw new IllegalArgumentException("Unknown Mutation Operation: " + op);
  }
}
