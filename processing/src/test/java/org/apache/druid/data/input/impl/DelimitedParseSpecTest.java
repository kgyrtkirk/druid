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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class DelimitedParseSpecTest
{
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testSerde() throws IOException
  {
    DelimitedParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec("abc", "iso", null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Collections.singletonList("abc"))),
        "\u0001",
        "\u0002",
        Collections.singletonList("abc"),
        false,
        0
    );
    final DelimitedParseSpec serde = jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec),
        DelimitedParseSpec.class
    );
    Assertions.assertEquals("abc", serde.getTimestampSpec().getTimestampColumn());
    Assertions.assertEquals("iso", serde.getTimestampSpec().getTimestampFormat());

    Assertions.assertEquals(Collections.singletonList("abc"), serde.getColumns());
    Assertions.assertEquals("\u0001", serde.getDelimiter());
    Assertions.assertEquals("\u0002", serde.getListDelimiter());
    Assertions.assertEquals(Collections.singletonList("abc"), serde.getDimensionsSpec().getDimensionNames());
  }

  @Test
  public void testComma()
  {
    assertThrows(IllegalArgumentException.class, () -> {
      @SuppressWarnings("unused") // expected exception
      final ParseSpec spec = new DelimitedParseSpec(
          new TimestampSpec(
              "timestamp",
              "auto",
              null
          ),
          new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("a,", "b"))),
          ",",
          null,
          Collections.singletonList("a,"),
          false,
          0
      );
    });
  }

  @Test
  public void testDefaultColumnList()
  {
    assertThrows(IllegalArgumentException.class, () -> {
      @SuppressWarnings("unused") // expected exception
      final DelimitedParseSpec spec = new DelimitedParseSpec(
          new TimestampSpec(
              "timestamp",
              "auto",
              null
          ),
          new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("a", "b"))),
          ",",
          null,
          null,
          false,
          0
      );
    });
  }
}
