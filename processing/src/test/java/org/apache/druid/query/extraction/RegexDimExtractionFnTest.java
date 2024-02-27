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

package org.apache.druid.query.extraction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 */
public class RegexDimExtractionFnTest
{
  private static final String[] PATHS = {
      "/druid/prod/historical",
      "/druid/prod/broker",
      "/druid/prod/coordinator",
      "/druid/demo/historical",
      "/druid/demo/broker",
      "/druid/demo/coordinator",
      "/dash/aloe",
      "/dash/baloo"
  };

  private static final String[] TEST_STRINGS = {
      "apple",
      "awesome",
      "asylum",
      "business",
      "be",
      "cool"
  };

  @Test
  public void testPathExtraction()
  {
    String regex = "/([^/]+)/";
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex, false, null);
    Set<String> extracted = new LinkedHashSet<>();

    for (String path : PATHS) {
      extracted.add(extractionFn.apply(path));
    }

    Set<String> expected = Sets.newLinkedHashSet(ImmutableList.of("druid", "dash"));
    Assertions.assertEquals(expected, extracted);
  }

  @Test
  public void testDeeperPathExtraction()
  {
    String regex = "^/([^/]+/[^/]+)(/|$)";
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex, false, null);
    Set<String> extracted = new LinkedHashSet<>();

    for (String path : PATHS) {
      extracted.add(extractionFn.apply(path));
    }

    Set<String> expected = Sets.newLinkedHashSet(
        ImmutableList.of(
            "druid/prod", "druid/demo",
            "dash/aloe", "dash/baloo"
        )
    );
    Assertions.assertEquals(expected, extracted);
  }

  @Test
  public void testIndexZero()
  {
    String regex = "/([^/]{4})/";
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex, 0, true, null);
    Set<String> extracted = new LinkedHashSet<>();

    for (String path : PATHS) {
      extracted.add(extractionFn.apply(path));
    }

    Set<String> expected = Sets.newLinkedHashSet(
        ImmutableList.of("/prod/", "/demo/", "/dash/")
    );
    Assertions.assertEquals(expected, extracted);
  }

  @Test
  public void testIndexTwo()
  {
    String regex = "^/([^/]+)/([^/]+)";
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex, 2, true, null);
    Set<String> extracted = new LinkedHashSet<>();

    for (String path : PATHS) {
      extracted.add(extractionFn.apply(path));
    }

    Set<String> expected = Sets.newLinkedHashSet(
        ImmutableList.of(
            "prod", "demo",
            "aloe", "baloo"
        )
    );
    Assertions.assertEquals(expected, extracted);
  }

  @Test
  public void testStringExtraction()
  {
    String regex = "(.)";
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex, false, null);
    Set<String> extracted = new LinkedHashSet<>();

    for (String testString : TEST_STRINGS) {
      extracted.add(extractionFn.apply(testString));
    }

    Set<String> expected = Sets.newLinkedHashSet(ImmutableList.of("a", "b", "c"));
    Assertions.assertEquals(expected, extracted);
  }

  @Test
  public void testNullAndEmpty()
  {
    String regex = "(.*)/.*/.*";
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex, false, null);
    // no match, map empty input value to null
    Assertions.assertEquals(NullHandling.replaceWithDefault() ? null : "", extractionFn.apply(""));
    // null value, returns null
    Assertions.assertEquals(null, extractionFn.apply(null));
    // empty match, map empty result to null
    Assertions.assertEquals(NullHandling.replaceWithDefault() ? null : "", extractionFn.apply("/a/b"));
  }

  @Test
  public void testMissingValueReplacementWhenPatternDoesNotMatchNull()
  {
    String regex = "(bob)";
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex, true, "NO MATCH");
    Assertions.assertEquals("NO MATCH", extractionFn.apply(""));
    Assertions.assertEquals("NO MATCH", extractionFn.apply(null));
    Assertions.assertEquals("NO MATCH", extractionFn.apply("abc"));
    Assertions.assertEquals("bob", extractionFn.apply("bob"));
  }

  @Test
  public void testMissingValueReplacementWhenPatternMatchesNull()
  {
    String regex = "^()$";
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex, true, "NO MATCH");
    Assertions.assertEquals(NullHandling.replaceWithDefault() ? null : "", extractionFn.apply(""));
    Assertions.assertEquals(NullHandling.replaceWithDefault() ? null : "NO MATCH", extractionFn.apply(null));
    Assertions.assertEquals("NO MATCH", extractionFn.apply("abc"));
  }

  @Test
  public void testMissingValueReplacementToEmpty()
  {
    String regex = "(bob)";
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex, true, "");
    Assertions.assertEquals(NullHandling.replaceWithDefault() ? null : "", extractionFn.apply(null));
    Assertions.assertEquals(NullHandling.replaceWithDefault() ? null : "", extractionFn.apply(""));
    Assertions.assertEquals(NullHandling.replaceWithDefault() ? null : "", extractionFn.apply("abc"));
    Assertions.assertEquals(NullHandling.replaceWithDefault() ? null : "", extractionFn.apply("123"));
    Assertions.assertEquals("bob", extractionFn.apply("bobby"));
  }

  @Test
  public void testMissingValueReplacement()
  {
    String regex = "(a\\w*)";
    ExtractionFn extractionFn = new RegexDimExtractionFn(regex, true, "foobar");
    Set<String> extracted = new LinkedHashSet<>();

    for (String testString : TEST_STRINGS) {
      extracted.add(extractionFn.apply(testString));
    }

    Set<String> expected = Sets.newLinkedHashSet(ImmutableList.of("apple", "awesome", "asylum", "foobar"));
    Assertions.assertEquals(expected, extracted);

    byte[] cacheKey = extractionFn.getCacheKey();
    byte[] expectedCacheKey = new byte[]{
        0x01, 0x28, 0x61, 0x5C, 0x77, 0x2A, 0x29, (byte) 0xFF, // expr
        0x00, 0x00, 0x00, 0x01, // index
        0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, (byte) 0xFF, // replaceMissingValueWith
        0x01 // replaceMissingValue
    };
    Assertions.assertArrayEquals(expectedCacheKey, cacheKey);

    ExtractionFn nullExtractionFn = new RegexDimExtractionFn(regex, true, null);
    Set<String> extracted2 = new LinkedHashSet<>();

    for (String testString : TEST_STRINGS) {
      extracted2.add(nullExtractionFn.apply(testString));
    }

    Set<String> expected2 = Sets.newLinkedHashSet(ImmutableList.of("apple", "awesome", "asylum"));
    expected2.add(null);
    Assertions.assertEquals(expected2, extracted2);

    cacheKey = nullExtractionFn.getCacheKey();
    expectedCacheKey = new byte[]{
        0x01, 0x28, 0x61, 0x5C, 0x77, 0x2A, 0x29, (byte) 0xFF, // expr
        0x00, 0x00, 0x00, 0x01, // index
        (byte) 0xFF, // replaceMissingValueWith
        0x01 // replaceMissingValue
    };
    Assertions.assertArrayEquals(expectedCacheKey, cacheKey);
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final String json = "{ \"type\" : \"regex\", \"expr\" : \".(...)?\" , " +
                        "\"replaceMissingValue\": true, \"replaceMissingValueWith\":\"foobar\"}";
    RegexDimExtractionFn extractionFn = (RegexDimExtractionFn) objectMapper.readValue(json, ExtractionFn.class);

    Assertions.assertEquals(".(...)?", extractionFn.getExpr());
    Assertions.assertTrue(extractionFn.isReplaceMissingValue());
    Assertions.assertEquals("foobar", extractionFn.getReplaceMissingValueWith());

    // round trip
    Assertions.assertEquals(
        extractionFn,
        objectMapper.readValue(
            objectMapper.writeValueAsBytes(extractionFn),
            ExtractionFn.class
        )
    );
  }
}
