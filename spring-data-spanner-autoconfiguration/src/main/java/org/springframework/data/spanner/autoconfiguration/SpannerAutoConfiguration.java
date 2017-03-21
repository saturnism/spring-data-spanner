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

package org.springframework.data.spanner.autoconfiguration;

import com.google.cloud.spanner.DatabaseId;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.spanner.core.AbstractSpannerConfiguration;

/**
 * Created by rayt on 3/20/17.
 */
@Configuration
@EnableConfigurationProperties(SpannerProperties.class)
public class SpannerAutoConfiguration extends AbstractSpannerConfiguration {
  private final SpannerProperties properties;

  public SpannerAutoConfiguration(SpannerProperties properties) {
    this.properties = properties;
  }

  @Override
  protected DatabaseId getDatabaseId() {
    return DatabaseId.of(properties.getProjectId(), properties.getInstanceId(), properties.getDatabase());
  }
}
