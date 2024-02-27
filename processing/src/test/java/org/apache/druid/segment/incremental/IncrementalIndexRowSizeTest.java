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

package org.apache.druid.segment.incremental;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.CloserRule;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class IncrementalIndexRowSizeTest extends InitializedNullHandlingTest
{
  public IncrementalIndexCreator indexCreator;

  @Rule
  public final CloserRule closer = new CloserRule(false);

  public void initIncrementalIndexRowSizeTest(String indexType) throws JsonProcessingException
  {
    indexCreator = closer.closeLater(
        new IncrementalIndexCreator(indexType, (builder, args) -> builder
            .setSimpleTestingIndexSchema(new CountAggregatorFactory("cnt"))
            .setMaxRowCount(10_000)
            .setMaxBytesInMemory(1_000)
            .setUseMaxMemoryEstimates(true)
            .build())
    );
  }

  public static Collection<?> constructorFeeder()
  {
    return IncrementalIndexCreator.getAppendableIndexTypes();
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{index}: {0}")
  public void testIncrementalIndexRowSizeBasic(String indexType)
  {
    initIncrementalIndexRowSizeTest(indexType);
    IncrementalIndex index = indexCreator.createIndex();
    long time = System.currentTimeMillis();
    IncrementalIndex.IncrementalIndexRowResult tndResult = index.toIncrementalIndexRow(toMapRow(
        time,
        "billy",
        "A", // 50 Bytes
        "joe",
        "B"  // 50 Bytes
    ));
    IncrementalIndexRow td1 = tndResult.getIncrementalIndexRow();
    // 32 (timestamp + dims array + dimensionDescList) + 50 ("A") + 50 ("B")
    Assertions.assertEquals(132, td1.estimateBytesInMemory());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{index}: {0}")
  public void testIncrementalIndexRowSizeArr(String indexType)
  {
    initIncrementalIndexRowSizeTest(indexType);
    IncrementalIndex index = indexCreator.createIndex();
    long time = System.currentTimeMillis();
    IncrementalIndex.IncrementalIndexRowResult tndResult = index.toIncrementalIndexRow(toMapRow(
        time + 1,
        "billy",
        "A", // 50 Bytes
        "joe",
        Arrays.asList("A", "B") // 100 Bytes
    ));
    IncrementalIndexRow td1 = tndResult.getIncrementalIndexRow();
    // 32 (timestamp + dims array + dimensionDescList) + 50 ("A") + 100 ("A", "B")
    Assertions.assertEquals(182, td1.estimateBytesInMemory());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{index}: {0}")
  public void testIncrementalIndexRowSizeComplex(String indexType)
  {
    initIncrementalIndexRowSizeTest(indexType);
    IncrementalIndex index = indexCreator.createIndex();
    long time = System.currentTimeMillis();
    IncrementalIndex.IncrementalIndexRowResult tndResult = index.toIncrementalIndexRow(toMapRow(
        time + 1,
        "billy",
        "nelson", // 60 Bytes
        "joe",
        Arrays.asList("123", "abcdef") // 54 + 60 Bytes
    ));
    IncrementalIndexRow td1 = tndResult.getIncrementalIndexRow();
    // 32 (timestamp + dims array + dimensionDescList) + 60 ("nelson") + 114 ("123", "abcdef")
    Assertions.assertEquals(206, td1.estimateBytesInMemory());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{index}: {0}")
  public void testIncrementalIndexRowSizeEmptyString(String indexType)
  {
    initIncrementalIndexRowSizeTest(indexType);
    IncrementalIndex index = indexCreator.createIndex();
    long time = System.currentTimeMillis();
    IncrementalIndex.IncrementalIndexRowResult tndResult = index.toIncrementalIndexRow(toMapRow(
        time + 1,
        "billy",
        "" // NullHandling.sqlCompatible() ? 48 Bytes : 4 Bytes
    ));
    IncrementalIndexRow td1 = tndResult.getIncrementalIndexRow();
    // 28 (timestamp + dims array + dimensionDescList) + 4 OR 48 depending on NullHandling.sqlCompatible()
    Assertions.assertEquals(NullHandling.sqlCompatible() ? 76 : 32, td1.estimateBytesInMemory());
  }

  private MapBasedInputRow toMapRow(long time, Object... dimAndVal)
  {
    Map<String, Object> data = new HashMap<>();
    for (int i = 0; i < dimAndVal.length; i += 2) {
      data.put((String) dimAndVal[i], dimAndVal[i + 1]);
    }
    return new MapBasedInputRow(time, Lists.newArrayList(data.keySet()), data);
  }
}
