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

package org.apache.druid.data.input.impl;

import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.systemfield.SystemField;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HttpInputSourceTest
{

  @Test
  public void testSerde() throws IOException
  {
    HttpInputSourceConfig httpInputSourceConfig = new HttpInputSourceConfig(null);
    final ObjectMapper mapper = new ObjectMapper();
    mapper.setInjectableValues(new Std().addValue(HttpInputSourceConfig.class, httpInputSourceConfig));
    final HttpInputSource source = new HttpInputSource(
        ImmutableList.of(URI.create("http://test.com/http-test")),
        "myName",
        new DefaultPasswordProvider("myPassword"),
        new SystemFields(EnumSet.of(SystemField.URI)),
        httpInputSourceConfig
    );
    final byte[] json = mapper.writeValueAsBytes(source);
    final HttpInputSource fromJson = (HttpInputSource) mapper.readValue(json, InputSource.class);
    Assertions.assertEquals(source, fromJson);
  }

  @Test
  public void testConstructorAllowsOnlyDefaultProtocols()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      new HttpInputSource(
          ImmutableList.of(URI.create("http:///")),
          "myName",
          new DefaultPasswordProvider("myPassword"),
          null,
          new HttpInputSourceConfig(null)
      );

      new HttpInputSource(
          ImmutableList.of(URI.create("https:///")),
          "myName",
          new DefaultPasswordProvider("myPassword"),
          null,
          new HttpInputSourceConfig(null)
      );
      new HttpInputSource(
          ImmutableList.of(URI.create("my-protocol:///")),
          "myName",
          new DefaultPasswordProvider("myPassword"),
          null,
          new HttpInputSourceConfig(null)
      );
    });
    assertTrue(exception.getMessage().contains("Only [http, https] protocols are allowed"));
  }

  @Test
  public void testConstructorAllowsOnlyCustomProtocols()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      final HttpInputSourceConfig customConfig = new HttpInputSourceConfig(ImmutableSet.of("druid"));
      new HttpInputSource(
          ImmutableList.of(URI.create("druid:///")),
          "myName",
          new DefaultPasswordProvider("myPassword"),
          null,
          customConfig
      );
      new HttpInputSource(
          ImmutableList.of(URI.create("https:///")),
          "myName",
          new DefaultPasswordProvider("myPassword"),
          null,
          customConfig
      );
    });
    assertTrue(exception.getMessage().contains("Only [druid] protocols are allowed"));
  }

  @Test
  public void testSystemFields()
  {
    HttpInputSourceConfig httpInputSourceConfig = new HttpInputSourceConfig(null);
    final HttpInputSource inputSource = new HttpInputSource(
        ImmutableList.of(URI.create("http://test.com/http-test")),
        "myName",
        new DefaultPasswordProvider("myPassword"),
        new SystemFields(EnumSet.of(SystemField.URI, SystemField.PATH)),
        httpInputSourceConfig
    );

    Assertions.assertEquals(
        EnumSet.of(SystemField.URI, SystemField.PATH),
        inputSource.getConfiguredSystemFields()
    );

    final HttpEntity entity = new HttpEntity(URI.create("https://example.com/foo"), null, null);

    Assertions.assertEquals("https://example.com/foo", inputSource.getSystemFieldValue(entity, SystemField.URI));
    Assertions.assertEquals("/foo", inputSource.getSystemFieldValue(entity, SystemField.PATH));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(HttpInputSource.class)
                  .usingGetClass()
                  .verify();
  }
}
