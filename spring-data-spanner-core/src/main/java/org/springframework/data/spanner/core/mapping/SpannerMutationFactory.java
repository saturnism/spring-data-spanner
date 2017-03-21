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

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.*;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PropertyHandler;

/**
 * Created by rayt on 3/14/17.
 */
public class SpannerMutationFactory {
  private final SpannerMappingContext mappingContext;
  public SpannerMutationFactory(SpannerMappingContext mappingContext) {
    this.mappingContext = mappingContext;
  }

  public Mutation insert(Object object) {
    return createMutation(Mutation.Op.INSERT, object);
  }

  public Mutation upsert(Object object) {
    return createMutation(Mutation.Op.INSERT_OR_UPDATE, object);
  }

  public Mutation replace(Object object) {
    return createMutation(Mutation.Op.REPLACE, object);
  }

  public Mutation update(Object object) {
    return createMutation(Mutation.Op.UPDATE, object);
  }

  public Mutation delete(Object object) {
    final Class<?> entityType = object.getClass();
    final BasicSpannerPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(object.getClass());
    final PersistentPropertyAccessor accessor = persistentEntity.getPropertyAccessor(object);

    final SpannerPersistentProperty idProperty = persistentEntity.getIdProperty();
    Class<?> propertyType = idProperty.getType();
    Object value = accessor.getProperty(idProperty);
    Key key = Key.newBuilder().appendObject(value).build();

    final Mutation mutation = Mutation.delete(persistentEntity.tableName(), key);
    return mutation;
  }

  public Mutation createMutation(Mutation.Op op, Object object) {
    final Class<?> entityType = object.getClass();
    final BasicSpannerPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(object.getClass());
    final Mutation.WriteBuilder writeBuilder = writeBuilder(op, persistentEntity.tableName());
    final PersistentPropertyAccessor accessor = persistentEntity.getPropertyAccessor(object);
    persistentEntity.doWithProperties(new PropertyHandler<SpannerPersistentProperty>() {
      @Override
      public void doWithPersistentProperty(SpannerPersistentProperty spannerPersistentProperty) {
        Object value = accessor.getProperty(spannerPersistentProperty);
        ValueBinder<Mutation.WriteBuilder> set = writeBuilder.set(spannerPersistentProperty.getFieldName());
        if (value instanceof String) {
          set.to((String) value);
        } else if (value instanceof Boolean) {
          set.to((Boolean) value);
        } else if (value instanceof Date) {
          set.to((Date) value);
        } else if (value instanceof Double) {
          set.to((Double) value);
        } else if (value instanceof Long) {
          set.to((Long) value);
        } else if (value instanceof Timestamp) {
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
