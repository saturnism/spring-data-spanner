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

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.springframework.data.mapping.PersistentPropertyAccessor;

/**
 * Created by rayt on 3/14/17.
 */
public class SpannerStructObjectMapper {
  private final SpannerMappingContext mappingContext;

  public SpannerStructObjectMapper(SpannerMappingContext mappingContext) {
    this.mappingContext = mappingContext;
  }

  public void map(Struct s, Object target) {
    Class<?> entityType = target.getClass();
    BasicSpannerPersistentEntity<?> persistentEntity = mappingContext.getPersistentEntity(target.getClass());
    PersistentPropertyAccessor accessor = persistentEntity.getPropertyAccessor(target);

    for (Type.StructField field : s.getType().getStructFields()) {
      String name = field.getName();
      SpannerPersistentProperty property = persistentEntity.getPersistentPropertyByColumnName(name);
      if (s.isNull(name)) {
        if (property.getType().isPrimitive()) {
          // TODO probably not good to do this anyways...
          // TODO or maybe throw an error?
        }
        else { accessor.setProperty(property, null); }
        continue;
      }
      switch (field.getType().getCode()) {
        case ARRAY:
          switch (field.getType().getArrayElementType().getCode()) {
            // TODO: implement all ARRAY types
            case BOOL:
              if (property.getActualType().isArray())
                accessor.setProperty(property, s.getBooleanArray(name));
              else
                accessor.setProperty(property, s.getBooleanList(name));
              break;
          }
          break;
        case BOOL:
          accessor.setProperty(property, s.getBoolean(name));
          break;
        case BYTES:
          break;
        case DATE:
          accessor.setProperty(property, s.getDate(name));
          break;
        case FLOAT64:
          accessor.setProperty(property, s.getDouble(name));
          break;
        case INT64:
          accessor.setProperty(property, s.getLong(name));
          break;
        case STRING:
          accessor.setProperty(property, s.getString(name));
          break;
        case TIMESTAMP:
          accessor.setProperty(property, s.getTimestamp(name));
          break;
        case STRUCT:
          break;
      }
    }
  }
}
