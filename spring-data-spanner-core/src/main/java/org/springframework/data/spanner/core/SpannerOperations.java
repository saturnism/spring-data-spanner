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

import java.util.List;

/**
 * Created by rayt on 3/20/17.
 */
public interface SpannerOperations {
  DatabaseClient getDatabaseClient();
  <T> T find(Class<T> entityClass, Key key);
  <T> List<T> find(Class<T> entityClass, KeySet keys, Options.ReadOption... options);
  <T> List<T> find(Class<T> entityClass, Statement statement, Options.QueryOption... options);

  <T> List<T> findAll(Class<T> entityClass, Options.ReadOption ... options);

  <T> void delete(Class<T> entityClass, Key key);
  <T> void delete(T object);
  <T> void delete(Class<T> entityClass, Iterable<? extends T> objects);
  <T> void delete(Class<T> entityClass, KeySet keys);

  <T> void insert(T object);
  <T> void update(T object, String ... properties);
  <T> void upsert(T object);
  <T> long count(Class<T> entityClass);
}
