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
import org.springframework.data.spanner.core.mapping.BasicSpannerPersistentEntity;
import org.springframework.data.spanner.core.mapping.SpannerMappingContext;
import org.springframework.data.spanner.core.mapping.SpannerMutationFactory;
import org.springframework.data.spanner.core.mapping.SpannerStructObjectMapper;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by rayt on 3/20/17.
 */
public class SpannerReadContextTemplate {
  private final SpannerStructObjectMapper objectMapper;

  public SpannerReadContextTemplate(SpannerStructObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  public <T> List<T> find(ReadContext readContext, Class<T> entityClass, Statement statement, Options.QueryOption... options) {
    ResultSet rs = readContext.executeQuery(statement, options);
    List<T> list = new LinkedList<T>();
    while (rs.next()) {
      T object = BeanUtils.instantiate(entityClass);
      objectMapper.map(rs.getCurrentRowAsStruct(), object);
      list.add(object);
    }
    return Collections.unmodifiableList(list);
  }
}
