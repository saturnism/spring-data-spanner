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

package org.springframework.data.spanner.repository.support;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import org.springframework.data.spanner.core.SpannerOperations;
import org.springframework.data.spanner.repository.SpannerRepository;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rayt on 3/23/17.
 */
public class SimpleSpannerRepository<T, ID extends Serializable> implements SpannerRepository<T, ID> {
  private final SpannerEntityInformation<T, ID> entityInformation;
  private final SpannerOperations spannerOperations;

  public SimpleSpannerRepository(SpannerEntityInformation<T, ID> entityInformation, SpannerOperations spannerOperations) {
    this.entityInformation = entityInformation;
    this.spannerOperations = spannerOperations;
  }

  @Override
  public SpannerOperations getSpannerOperations() {
    return spannerOperations;
  }

  @Override
  public <S extends T> S save(S entity) {
    spannerOperations.upsert(entity);
    return entity;
  }

  @Override
  public <S extends T> Iterable<S> save(Iterable<S> entities) {
    List<S> result = new ArrayList<>();
    for (S entity : entities) {
      // TODO fix me, this can be better in Spanner
      save(entity);
    }
    return result;
  }

  @Override
  public T findOne(ID id) {
    return spannerOperations.find(entityInformation.getJavaType(), Key.of(id));
  }

  @Override
  public boolean exists(ID id) {
    return findOne(id) != null;
  }

  @Override
  public Iterable<T> findAll() {
    return spannerOperations.findAll(entityInformation.getJavaType());
  }

  @Override
  public Iterable<T> findAll(Iterable<ID> iterable) {
    KeySet.Builder builder = KeySet.newBuilder();
    for (ID id : iterable) {
      builder.addKey(Key.of(id));
    }

    return spannerOperations.find(entityInformation.getJavaType(), builder.build());
  }

  @Override
  public long count() {
    return spannerOperations.count(entityInformation.getJavaType());
  }

  @Override
  public void delete(ID id) {
    spannerOperations.delete(entityInformation.getJavaType(), Key.of(id));
  }

  @Override
  public void delete(T t) {
    spannerOperations.delete(t);
  }

  @Override
  public void delete(Iterable<? extends T> entities) {
    spannerOperations.delete(entityInformation.getJavaType(), entities);
  }

  @Override
  public void deleteAll() {
    spannerOperations.delete(entityInformation.getJavaType(), KeySet.all());
  }
}
