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

import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.model.*;
import org.springframework.util.StringUtils;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;

public class BasicSpannerPersistentProperty extends AnnotationBasedPersistentProperty<SpannerPersistentProperty> implements
    SpannerPersistentProperty {

  private FieldNamingStrategy fieldNamingStrategy;

  public BasicSpannerPersistentProperty(Field field, PropertyDescriptor propertyDescriptor, PersistentEntity<?, SpannerPersistentProperty> owner, SimpleTypeHolder simpleTypeHolder, FieldNamingStrategy fieldNamingStrategy) {
    super(field, propertyDescriptor, owner, simpleTypeHolder);
    this.fieldNamingStrategy = fieldNamingStrategy == null ? PropertyNameFieldNamingStrategy.INSTANCE
        : fieldNamingStrategy;
  }

  @Override
  protected Association<SpannerPersistentProperty> createAssociation() {
    return new Association<SpannerPersistentProperty>(this, null);
  }

  @Override
  public String getColumnName() {
    if (hasExplicitColumnName()) {
      return getAnnotatedColumnName();
    }

    String fieldName = fieldNamingStrategy.getFieldName(this);

    if (!StringUtils.hasText(fieldName)) {
      throw new MappingException(String.format("Invalid (null or empty) field name returned for property %s by %s!",
          this, fieldNamingStrategy.getClass()));
    }

    return fieldName;
  }

  protected boolean hasExplicitColumnName() {
    return StringUtils.hasText(getAnnotatedColumnName());
  }

  private String getAnnotatedColumnName() {

    Column annotation = findAnnotation(Column.class);

    if (annotation != null && StringUtils.hasText(annotation.name())) {
      return annotation.name();
    }

    return null;
  }
}
