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

import org.springframework.data.repository.core.support.PersistentEntityInformation;
import org.springframework.data.spanner.core.mapping.SpannerPersistentEntity;

import java.io.Serializable;

/**
 * Created by rayt on 3/23/17.
 */
public class MappingSpannerEntityInformation<T, ID extends Serializable> extends PersistentEntityInformation<T, ID>
    implements SpannerEntityInformation<T, ID> {
  public MappingSpannerEntityInformation(SpannerPersistentEntity<T> entity) {
    super(entity);
  }
}
