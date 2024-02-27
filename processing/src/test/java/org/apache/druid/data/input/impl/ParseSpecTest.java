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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ParseSpecTest
{

  private ObjectMapper mapper;

  @BeforeEach
  public void setUp()
  {
    // Similar to configs from DefaultObjectMapper, which we cannot use here since we're in druid-api.
    mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(MapperFeature.AUTO_DETECT_GETTERS, false);
    mapper.configure(MapperFeature.AUTO_DETECT_FIELDS, false);
    mapper.configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
    mapper.configure(MapperFeature.AUTO_DETECT_SETTERS, false);
    mapper.configure(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS, false);
  }

  @Test
  public void testDuplicateNames()
  {
    assertThrows(ParseException.class, () -> {
      @SuppressWarnings("unused") // expected exception
      final ParseSpec spec = new DelimitedParseSpec(
          new TimestampSpec(
              "timestamp",
              "auto",
              null
          ),
          new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("a", "b", "a"))),
          ",",
          " ",
          Arrays.asList("a", "b"),
          false,
          0
      );
    });
  }

  @Test
  public void testDimAndDimExcluOverlap()
  {
    assertThrows(IllegalArgumentException.class, () -> {
      @SuppressWarnings("unused") // expected exception
      final ParseSpec spec = new DelimitedParseSpec(
          new TimestampSpec(
              "timestamp",
              "auto",
              null
          ),
          DimensionsSpec.builder()
              .setDimensions(DimensionsSpec.getDefaultSchemas(Arrays.asList("a", "B")))
              .setDimensionExclusions(Collections.singletonList("B"))
              .build(),
          ",",
          null,
          Arrays.asList("a", "B"),
          false,
          0
      );
    });
  }

  @Test
  public void testDimExclusionDuplicate()
  {
    @SuppressWarnings("unused") // expected exception
    final ParseSpec spec = new DelimitedParseSpec(
        new TimestampSpec(
            "timestamp",
            "auto",
            null
        ),
        DimensionsSpec.builder()
                      .setDimensions(DimensionsSpec.getDefaultSchemas(Collections.singletonList("a")))
                      .setDimensionExclusions(Lists.newArrayList("B", "B"))
                      .build(),
        ",",
        null,
        Arrays.asList("a", "B"),
        false,
        0
    );
  }

  @Test
  public void testDefaultTimestampSpec()
  {
    Throwable exception = assertThrows(NullPointerException.class, () -> {
      @SuppressWarnings("unused") // expected exception
      final ParseSpec spec = new DelimitedParseSpec(
          null,
          DimensionsSpec.builder()
              .setDimensions(DimensionsSpec.getDefaultSchemas(Collections.singletonList("a")))
              .setDimensionExclusions(Lists.newArrayList("B", "B"))
              .build(),
          ",",
          null,
          Arrays.asList("a", "B"),
          false,
          0
      );
    });
    assertTrue(exception.getMessage().contains("parseSpec requires timestampSpec"));
  }

  @Test
  public void testDimensionSpecRequired()
  {
    Throwable exception = assertThrows(NullPointerException.class, () -> {
      @SuppressWarnings("unused") // expected exception
      final ParseSpec spec = new DelimitedParseSpec(
          new TimestampSpec(
              "timestamp",
              "auto",
              null
          ),
          null,
          ",",
          null,
          Arrays.asList("a", "B"),
          false,
          0
      );
    });
    assertTrue(exception.getMessage().contains("parseSpec requires dimensionSpec"));
  }

  @Test
  public void testSerde() throws IOException
  {
    final String json = "{"
                        + "\"format\":\"timeAndDims\", "
                        + "\"timestampSpec\": {\"column\":\"timestamp\"}, "
                        + "\"dimensionsSpec\":{}"
                        + "}";

    final Object mapValue = mapper.readValue(json, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
    final ParseSpec parseSpec = mapper.convertValue(mapValue, ParseSpec.class);

    Assertions.assertEquals(TimeAndDimsParseSpec.class, parseSpec.getClass());
    Assertions.assertEquals("timestamp", parseSpec.getTimestampSpec().getTimestampColumn());
    Assertions.assertEquals(ImmutableList.of(), parseSpec.getDimensionsSpec().getDimensionNames());

    // Test round-trip.
    Assertions.assertEquals(
        parseSpec,
        mapper.readValue(mapper.writeValueAsString(parseSpec), ParseSpec.class)
    );
  }

  @Test
  public void testBadTypeSerde() throws IOException
  {
    final String json = "{"
                        + "\"format\":\"foo\", "
                        + "\"timestampSpec\": {\"column\":\"timestamp\"}, "
                        + "\"dimensionsSpec\":{}"
                        + "}";

    final Object mapValue = mapper.readValue(json, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectCause(CoreMatchers.instanceOf(JsonMappingException.class));
    expectedException.expectMessage("Could not resolve type id 'foo' as a subtype");
    mapper.convertValue(mapValue, ParseSpec.class);
  }
}
