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

package org.springframework.data.spanner.repository.config;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.data.config.ParsingUtils;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.config.XmlRepositoryConfigurationSource;
import org.springframework.data.spanner.repository.support.SpannerRepositoryFactoryBean;
import org.w3c.dom.Element;

/**
 * Created by rayt on 3/23/17.
 */
public class SpannerRepositoryConfigurationExtension extends RepositoryConfigurationExtensionSupport {
  private static final String SPANNER_TEMPLATE_REF = "spanner-template-ref";

  @Override
  protected String getModulePrefix() {
    return "spanner";
  }

  @Override
  public String getRepositoryFactoryClassName() {
    return SpannerRepositoryFactoryBean.class.getName();
  }

  @Override
  public void postProcess(BeanDefinitionBuilder builder, AnnotationRepositoryConfigurationSource config) {
    AnnotationAttributes attributes = config.getAttributes();

    builder.addPropertyReference("spannerOperations", attributes.getString("spannerTemplateRef"));
  }

  @Override
  public void postProcess(BeanDefinitionBuilder builder, XmlRepositoryConfigurationSource config) {
    Element element = config.getElement();

    ParsingUtils.setPropertyReference(builder, element, SPANNER_TEMPLATE_REF, "spannerOperations");
  }
}
