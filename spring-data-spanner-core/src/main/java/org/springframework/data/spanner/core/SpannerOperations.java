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

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;

import java.util.List;

/**
 * Created by rayt on 3/20/17.
 */
public interface SpannerOperations {
  <T> List<T> find(Class<T> entityClass, Statement statement, Options.QueryOption... options);

  <T> List<T> findAll(Class<T> entityClass, Options.QueryOption ... options);

  void delete(Class<?> entityClass, Key key);
  void delete(Object object);

  void insert(Object object);
  void update(Object object);
  void upsert(Object object);
}
