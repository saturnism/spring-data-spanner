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

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mapping.PersistentPropertyAccessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by rayt on 3/14/17.
 */
public class SpannerResultSetMapper {
  private final SpannerStructObjectMapper objectMapper;

  public SpannerResultSetMapper(SpannerStructObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  public <T> void map(ResultSet resultSet, Class<T> entityClass, List<T> target) {
    while (resultSet.next()) {
      T object = BeanUtils.instantiate(entityClass);
      objectMapper.map(resultSet.getCurrentRowAsStruct(), object);
      target.add(object);
    }
  }

  public <T> List<T> mapToUnmodifiableList(ResultSet resultSet, Class<T> entityClass) {
    ArrayList<T> result = new ArrayList<T>();
    this.map(resultSet, entityClass, result);
    return Collections.unmodifiableList(result);
  }

}
