/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.filter;

import com.google.common.collect.ImmutableList;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.jupiter.api.extension.support.TypeBasedParameterResolver;

import java.util.List;
import java.util.stream.Stream;

public class Sample1
{
  public static class MyContextProvider implements TestTemplateInvocationContextProvider{

    @Override
    public boolean supportsTestTemplate(ExtensionContext context)
    {
      return true;
    }

    @Override
    public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context)
    {
      return Stream.of(
        featureDisabledContext(
      ));    }

  }

  private static TestTemplateInvocationContext featureDisabledContext(

      ) {
        return new TestTemplateInvocationContext() {
            @Override
            public String getDisplayName(int invocationIndex) {
                return "name"+invocationIndex;
            }

            @Override
            public List<Extension> getAdditionalExtensions() {
                return ImmutableList.of(
                  new TypeBasedParameterResolver<DimFilter>() {

                    @Override
                    public DimFilter resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
                        throws ParameterResolutionException
                    {
                      return new AndDimFilter();
                    }

                  },
                  new BeforeTestExecutionCallback() {
                      @Override
                      public void beforeTestExecution(ExtensionContext extensionContext) {
                          System.out.println("BeforeTestExecutionCallback:Disabled context");
                      }
                  },
                  new AfterTestExecutionCallback() {
                      @Override
                      public void afterTestExecution(ExtensionContext extensionContext) {
                          System.out.println("AfterTestExecutionCallback:Disabled context");
                      }
                  }
                );
            }
        };
    }

  @TestTemplate
  @ExtendWith(MyContextProvider.class)
  public void a () {

  }

}
