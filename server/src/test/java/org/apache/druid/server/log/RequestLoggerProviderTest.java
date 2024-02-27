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

package org.apache.druid.server.log;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.ProvisionException;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import javax.validation.Validation;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestLoggerProviderTest
{
  private final DefaultObjectMapper mapper = new DefaultObjectMapper();

  public RequestLoggerProviderTest()
  {
    mapper.registerSubtypes(
        NoopRequestLoggerProvider.class,
        TestRequestLoggerProvider.class
    );

    final InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(ObjectMapper.class, mapper);
    mapper.setInjectableValues(injectableValues);
  }

  @Test
  public void testNoLoggerAtAll()
  {
    final Properties properties = new Properties();
    properties.setProperty("dummy", "unrelated");
    final JsonConfigurator configurator = new JsonConfigurator(
        mapper,
        Validation.buildDefaultValidatorFactory()
                  .getValidator()
    );

    final RequestLoggerProvider provider = configurator.configurate(
        properties,
        "log",
        RequestLoggerProvider.class,
        NoopRequestLoggerProvider.class
    );
    assertThat(provider, CoreMatchers.instanceOf(NoopRequestLoggerProvider.class));
  }

  @Test
  public void testLoggerPropertiesWithNoType()
  {
    Throwable exception = assertThrows(ProvisionException.class, () -> {
      final Properties properties = new Properties();
      properties.setProperty("dummy", "unrelated");
      properties.setProperty("log.foo", "bar");
      final JsonConfigurator configurator = new JsonConfigurator(
          mapper,
          Validation.buildDefaultValidatorFactory()
              .getValidator()
      );

      configurator.configurate(
          properties,
          "log",
          RequestLoggerProvider.class,
          NoopRequestLoggerProvider.class
      );
    });
    assertTrue(exception.getMessage().contains("missing type id property 'type'"));
  }
}
