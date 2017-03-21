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

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.mapping.context.AbstractMappingContext;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.TypeInformation;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;

/**
 * Created by rayt on 3/14/17.
 */
public class SpannerMappingContext extends AbstractMappingContext<BasicSpannerPersistentEntity<?>, SpannerPersistentProperty>
    implements ApplicationContextAware {

  private static final FieldNamingStrategy DEFAULT_NAMING_STRATEGY = PropertyNameFieldNamingStrategy.INSTANCE;
  private FieldNamingStrategy fieldNamingStrategy = DEFAULT_NAMING_STRATEGY;

  private ApplicationContext context;

  public SpannerMappingContext() {

  }

  public void setFieldNamingStrategy(FieldNamingStrategy fieldNamingStrategy) {
    this.fieldNamingStrategy = fieldNamingStrategy == null ? DEFAULT_NAMING_STRATEGY : fieldNamingStrategy;
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.context = applicationContext;
  }

  @Override
  protected <T> BasicSpannerPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {
    BasicSpannerPersistentEntity<T> entity = new BasicSpannerPersistentEntity<T>(typeInformation);

    if (context != null) {
      entity.setApplicationContext(context);
    }

    return entity;
  }

  @Override
  protected SpannerPersistentProperty createPersistentProperty(Field field, PropertyDescriptor propertyDescriptor, BasicSpannerPersistentEntity<?> basicSpannerPersistentEntity, SimpleTypeHolder simpleTypeHolder) {
    return new BasicSpannerPersistentProperty(field, propertyDescriptor, basicSpannerPersistentEntity, simpleTypeHolder, fieldNamingStrategy);
  }
}
