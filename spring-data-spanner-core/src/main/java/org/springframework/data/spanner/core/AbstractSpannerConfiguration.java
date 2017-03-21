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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.spanner.core.SpannerTemplate;
import org.springframework.data.spanner.core.mapping.SpannerMappingContext;

/**
 * Created by rayt on 3/20/17.
 */
@Configuration
public abstract class AbstractSpannerConfiguration {
  protected abstract DatabaseId getDatabaseId();

  @Bean
  public SpannerOptions spannerOptions() {
    return SpannerOptions.newBuilder().build();
  }

  @Bean
  public Spanner spanner(SpannerOptions spannerOptions) {
    return spannerOptions.getService();
  }

  @Bean
  public DatabaseClient spannerDatabaseClient(Spanner spanner) {
    return spanner.getDatabaseClient(getDatabaseId());
  }

  @Bean
  public SpannerMappingContext spannerMappingContext() {
    return new SpannerMappingContext();
  }

  @Bean
  public SpannerTemplate spannerTemplate(DatabaseClient databaseClient, SpannerMappingContext mappingContext) {
    return new SpannerTemplate(databaseClient, mappingContext);
  }

}
