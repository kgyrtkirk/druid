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

package org.apache.druid.query.filter;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.apache.druid.query.ordering.StringComparators;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;

public class BoundDimFilterTest
{
  private static final ExtractionFn EXTRACTION_FN = new RegexDimExtractionFn(".*", false, null);

  public static Iterable<Object[]> constructorFeeder()
  {

    return ImmutableList.of(
        new Object[]{new BoundDimFilter("dimension", "12", "15", null, null, null, null,
                                        StringComparators.LEXICOGRAPHIC)},
        new Object[]{new BoundDimFilter("dimension", "12", "15", null, true, false, null,
                                        StringComparators.LEXICOGRAPHIC)},
        new Object[]{new BoundDimFilter("dimension", "12", "15", null, null, true, null,
                                        StringComparators.ALPHANUMERIC)},
        new Object[]{new BoundDimFilter("dimension", null, "15", null, true, true, null,
                                        StringComparators.ALPHANUMERIC)},
        new Object[]{new BoundDimFilter("dimension", "12", "15", true, null, null, null,
                                        StringComparators.LEXICOGRAPHIC)},
        new Object[]{new BoundDimFilter("dimension", "12", null, true, null, true, null,
                                        StringComparators.ALPHANUMERIC)},
        new Object[]{new BoundDimFilter("dimension", "12", "15", true, true, true, null,
                                        StringComparators.ALPHANUMERIC)},
        new Object[]{new BoundDimFilter("dimension", "12", "15", true, true, false, null,
                                        StringComparators.LEXICOGRAPHIC)},
        new Object[]{new BoundDimFilter("dimension", null, "15", null, true, true, EXTRACTION_FN,
                                        StringComparators.ALPHANUMERIC)}
    );
  }

  private BoundDimFilter boundDimFilter;

  public void initBoundDimFilterTest(BoundDimFilter boundDimFilter)
  {
    this.boundDimFilter = boundDimFilter;
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest
  public void testSerDesBoundFilter(BoundDimFilter boundDimFilter) throws IOException
  {
    initBoundDimFilterTest(boundDimFilter);
    Injector defaultInjector = GuiceInjectors.makeStartupInjector();
    ObjectMapper mapper = defaultInjector.getInstance(Key.get(ObjectMapper.class, Json.class));
    String serBetweenDimFilter = mapper.writeValueAsString(boundDimFilter);
    BoundDimFilter actualBoundDimFilter = mapper.readerFor(DimFilter.class).readValue(serBetweenDimFilter);
    Assertions.assertEquals(boundDimFilter, actualBoundDimFilter);
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest
  public void testGetCacheKey(BoundDimFilter boundDimFilter)
  {
    initBoundDimFilterTest(boundDimFilter);
    BoundDimFilter boundDimFilter = new BoundDimFilter("dimension", "12", "15", null, null, true, null, StringComparators.ALPHANUMERIC);
    BoundDimFilter boundDimFilterCopy = new BoundDimFilter("dimension", "12", "15", false, false, true, null, StringComparators.ALPHANUMERIC);
    Assertions.assertArrayEquals(boundDimFilter.getCacheKey(), boundDimFilterCopy.getCacheKey());
    BoundDimFilter anotherBoundDimFilter = new BoundDimFilter("dimension", "12", "15", true, null, false, null, StringComparators.LEXICOGRAPHIC);
    Assertions.assertFalse(Arrays.equals(anotherBoundDimFilter.getCacheKey(), boundDimFilter.getCacheKey()));

    BoundDimFilter boundDimFilterWithExtract = new BoundDimFilter("dimension", "12", "15", null, null, true, EXTRACTION_FN, StringComparators.ALPHANUMERIC);
    BoundDimFilter boundDimFilterWithExtractCopy = new BoundDimFilter("dimension", "12", "15", false, false, true, EXTRACTION_FN, StringComparators.ALPHANUMERIC);
    Assertions.assertFalse(Arrays.equals(boundDimFilter.getCacheKey(), boundDimFilterWithExtract.getCacheKey()));
    Assertions.assertArrayEquals(boundDimFilterWithExtract.getCacheKey(), boundDimFilterWithExtractCopy.getCacheKey());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest
  public void testHashCode(BoundDimFilter boundDimFilter)
  {
    initBoundDimFilterTest(boundDimFilter);
    BoundDimFilter boundDimFilter = new BoundDimFilter("dimension", "12", "15", null, null, true, null, StringComparators.ALPHANUMERIC);
    BoundDimFilter boundDimFilterWithExtract = new BoundDimFilter("dimension", "12", "15", null, null, true, EXTRACTION_FN, StringComparators.ALPHANUMERIC);

    Assertions.assertNotEquals(boundDimFilter.hashCode(), boundDimFilterWithExtract.hashCode());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest
  public void testGetRequiredColumns(BoundDimFilter boundDimFilter)
  {
    initBoundDimFilterTest(boundDimFilter);
    BoundDimFilter boundDimFilter = new BoundDimFilter("dimension", "12", "15", null, null, true, null, StringComparators.ALPHANUMERIC);
    Assertions.assertEquals(boundDimFilter.getRequiredColumns(), Sets.newHashSet("dimension"));
  }
}
