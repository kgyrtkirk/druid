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

package org.apache.druid.query.timeseries;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChestTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.last.StringLastAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;

public class TimeseriesQueryQueryToolChestTest
{
  private static final String TIMESTAMP_RESULT_FIELD_NAME = "d0";
  private static final TimeseriesQueryQueryToolChest TOOL_CHEST = new TimeseriesQueryQueryToolChest(null);

  @BeforeAll
  public static void setUpClass()
  {
    NullHandling.initializeForTests();
  }

  public static Iterable<Object[]> constructorFeeder()
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(Arrays.asList(false, true));
  }

  private boolean descending;

  public void initTimeseriesQueryQueryToolChestTest(boolean descending)
  {
    this.descending = descending;
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "descending={0}")
  public void testCacheStrategy(boolean descending) throws Exception
  {
    initTimeseriesQueryQueryToolChestTest(descending);
    CacheStrategy<Result<TimeseriesResultValue>, Object, TimeseriesQuery> strategy =
        TOOL_CHEST.getCacheStrategy(
            new TimeseriesQuery(
                new TableDataSource("dummy"),
                new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2015-01-01/2015-01-02"))),
                descending,
                VirtualColumns.EMPTY,
                null,
                Granularities.ALL,
                ImmutableList.of(
                    new CountAggregatorFactory("metric1"),
                    new LongSumAggregatorFactory("metric0", "metric0"),
                    new StringLastAggregatorFactory("complexMetric", "test", null, null)
                ),
                ImmutableList.of(new ConstantPostAggregator("post", 10)),
                0,
                null
            )
        );

    final Result<TimeseriesResultValue> result1 = new Result<>(
        // test timestamps that result in integer size millis
        DateTimes.utc(123L),
        new TimeseriesResultValue(
            ImmutableMap.of(
                "metric1", 2,
                "metric0", 3,
                "complexMetric", new SerializablePairLongString(123L, "val1")
            )
        )
    );

    Object preparedValue = strategy.prepareForSegmentLevelCache().apply(result1);

    ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    Object fromCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedValue),
        strategy.getCacheObjectClazz()
    );

    Result<TimeseriesResultValue> fromCacheResult = strategy.pullFromSegmentLevelCache().apply(fromCacheValue);

    Assertions.assertEquals(result1, fromCacheResult);

    final Result<TimeseriesResultValue> result2 = new Result<>(
        // test timestamps that result in integer size millis
        DateTimes.utc(123L),
        new TimeseriesResultValue(
            ImmutableMap.of(
                "metric1", 2,
                "metric0", 3,
                "complexMetric", "val1",
                "post", 10
            )
        )
    );

    Object preparedResultLevelCacheValue = strategy.prepareForCache(true).apply(result2);
    Object fromResultLevelCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedResultLevelCacheValue),
        strategy.getCacheObjectClazz()
    );

    Result<TimeseriesResultValue> fromResultLevelCacheRes = strategy.pullFromCache(true)
                                                                    .apply(fromResultLevelCacheValue);
    Assertions.assertEquals(result2, fromResultLevelCacheRes);

    final Result<TimeseriesResultValue> result3 = new Result<>(
        // null timestamp similar to grandTotal
        null,
        new TimeseriesResultValue(
            ImmutableMap.of("metric1", 2, "metric0", 3, "complexMetric", "val1", "post", 10)
        )
    );

    preparedResultLevelCacheValue = strategy.prepareForCache(true).apply(result3);
    fromResultLevelCacheValue = objectMapper.readValue(
        objectMapper.writeValueAsBytes(preparedResultLevelCacheValue),
        strategy.getCacheObjectClazz()
    );

    fromResultLevelCacheRes = strategy.pullFromCache(true).apply(fromResultLevelCacheValue);
    Assertions.assertEquals(result3, fromResultLevelCacheRes);
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "descending={0}")
  public void testCacheKey(boolean descending)
  {
    initTimeseriesQueryQueryToolChestTest(descending);
    final TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                         .dataSource("dummy")
                                         .intervals("2015-01-01/2015-01-02")
                                         .descending(descending)
                                         .granularity(Granularities.ALL)
                                         .aggregators(
                                             ImmutableList.of(
                                                 new CountAggregatorFactory("metric1"),
                                                 new LongSumAggregatorFactory("metric0", "metric0")
                                             )
                                         )
                                         .build();

    final TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                         .dataSource("dummy")
                                         .intervals("2015-01-01/2015-01-02")
                                         .descending(descending)
                                         .granularity(Granularities.ALL)
                                         .aggregators(
                                             ImmutableList.of(
                                                 new LongSumAggregatorFactory("metric0", "metric0"),
                                                 new CountAggregatorFactory("metric1")
                                             )
                                         )
                                         .build();

    // Test for https://github.com/apache/druid/issues/4093.
    Assertions.assertFalse(
        Arrays.equals(
            TOOL_CHEST.getCacheStrategy(query1).computeCacheKey(query1),
            TOOL_CHEST.getCacheStrategy(query2).computeCacheKey(query2)
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "descending={0}")
  public void testResultLevelCacheKey(boolean descending)
  {
    initTimeseriesQueryQueryToolChestTest(descending);
    final TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                         .dataSource("dummy")
                                         .intervals("2015-01-01/2015-01-02")
                                         .descending(descending)
                                         .granularity(Granularities.ALL)
                                         .aggregators(
                                             ImmutableList.of(
                                                 new LongSumAggregatorFactory("metric0", "metric0"),
                                                 new CountAggregatorFactory("metric1")
                                             )
                                         )
                                         .postAggregators(
                                             ImmutableList.of(
                                                 new ArithmeticPostAggregator(
                                                     "post",
                                                     "+",
                                                     ImmutableList.of(
                                                         new FieldAccessPostAggregator(
                                                             null,
                                                             "metric1"
                                                         ),
                                                         new FieldAccessPostAggregator(
                                                             null,
                                                             "metric0"
                                                         )
                                                     )
                                                 )
                                             )
                                         )
                                         .build();

    final TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                         .dataSource("dummy")
                                         .intervals("2015-01-01/2015-01-02")
                                         .descending(descending)
                                         .granularity(Granularities.ALL)
                                         .aggregators(
                                             ImmutableList.of(
                                                 new LongSumAggregatorFactory("metric0", "metric0"),
                                                 new CountAggregatorFactory("metric1")
                                             )
                                         )
                                         .postAggregators(
                                             ImmutableList.of(
                                                 new ArithmeticPostAggregator(
                                                     "post",
                                                     "/",
                                                     ImmutableList.of(
                                                         new FieldAccessPostAggregator(
                                                             null,
                                                             "metric1"
                                                         ),
                                                         new FieldAccessPostAggregator(
                                                             null,
                                                             "metric0"
                                                         )
                                                     )
                                                 )
                                             )
                                         )
                                         .build();

    Assertions.assertTrue(
        Arrays.equals(
            TOOL_CHEST.getCacheStrategy(query1).computeCacheKey(query1),
            TOOL_CHEST.getCacheStrategy(query2).computeCacheKey(query2)
        )
    );
    Assertions.assertFalse(
        Arrays.equals(
            TOOL_CHEST.getCacheStrategy(query1).computeResultLevelCacheKey(query1),
            TOOL_CHEST.getCacheStrategy(query2).computeResultLevelCacheKey(query2)
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "descending={0}")
  public void testResultLevelCacheKeyWithGrandTotal(boolean descending)
  {
    initTimeseriesQueryQueryToolChestTest(descending);
    final TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                         .dataSource("dummy")
                                         .intervals("2015-01-01/2015-01-02")
                                         .descending(descending)
                                         .granularity(Granularities.ALL)
                                         .aggregators(
                                             ImmutableList.of(
                                                 new LongSumAggregatorFactory("metric0", "metric0"),
                                                 new CountAggregatorFactory("metric1")
                                             )
                                         )
                                         .postAggregators(
                                             ImmutableList.of(
                                                 new ArithmeticPostAggregator(
                                                     "post",
                                                     "+",
                                                     ImmutableList.of(
                                                         new FieldAccessPostAggregator(
                                                             null,
                                                             "metric1"
                                                         ),
                                                         new FieldAccessPostAggregator(
                                                             null,
                                                             "metric0"
                                                         )
                                                     )
                                                 )
                                             )
                                         )
                                         .context(ImmutableMap.of(TimeseriesQuery.CTX_GRAND_TOTAL, true))
                                         .build();

    final TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                         .dataSource("dummy")
                                         .intervals("2015-01-01/2015-01-02")
                                         .descending(descending)
                                         .granularity(Granularities.ALL)
                                         .aggregators(
                                             ImmutableList.of(
                                                 new LongSumAggregatorFactory("metric0", "metric0"),
                                                 new CountAggregatorFactory("metric1")
                                             )
                                         )
                                         .postAggregators(
                                             ImmutableList.of(
                                                 new ArithmeticPostAggregator(
                                                     "post",
                                                     "/",
                                                     ImmutableList.of(
                                                         new FieldAccessPostAggregator(
                                                             null,
                                                             "metric1"
                                                         ),
                                                         new FieldAccessPostAggregator(
                                                             null,
                                                             "metric0"
                                                         )
                                                     )
                                                 )
                                             )
                                         )
                                         .context(ImmutableMap.of(TimeseriesQuery.CTX_GRAND_TOTAL, true))
                                         .build();

    Assertions.assertTrue(
        Arrays.equals(
            TOOL_CHEST.getCacheStrategy(query1).computeCacheKey(query1),
            TOOL_CHEST.getCacheStrategy(query2).computeCacheKey(query2)
        )
    );
    Assertions.assertFalse(
        Arrays.equals(
            TOOL_CHEST.getCacheStrategy(query1).computeResultLevelCacheKey(query1),
            TOOL_CHEST.getCacheStrategy(query2).computeResultLevelCacheKey(query2)
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "descending={0}")
  public void testResultArraySignatureWithoutTimestampResultField(boolean descending)
  {
    initTimeseriesQueryQueryToolChestTest(descending);
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .descending(descending)
              .granularity(Granularities.HOUR)
              .aggregators(QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS)
              .postAggregators(QueryRunnerTestHelper.CONSTANT)
              .build();

    Assertions.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("rows", ColumnType.LONG)
                    .add("index", ColumnType.DOUBLE)
                    .add("uniques", null)
                    .add("const", ColumnType.LONG)
                    .build(),
        TOOL_CHEST.resultArraySignature(query)
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "descending={0}")
  public void testResultArraySignatureWithTimestampResultField(boolean descending)
  {
    initTimeseriesQueryQueryToolChestTest(descending);
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .descending(descending)
              .granularity(Granularities.HOUR)
              .aggregators(QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS)
              .postAggregators(QueryRunnerTestHelper.CONSTANT)
              .context(ImmutableMap.of(TimeseriesQuery.CTX_TIMESTAMP_RESULT_FIELD, TIMESTAMP_RESULT_FIELD_NAME))
              .build();

    Assertions.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add(TIMESTAMP_RESULT_FIELD_NAME, ColumnType.LONG)
                    .add("rows", ColumnType.LONG)
                    .add("index", ColumnType.DOUBLE)
                    .add("uniques", null)
                    .add("const", ColumnType.LONG)
                    .build(),
        TOOL_CHEST.resultArraySignature(query)
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "descending={0}")
  public void testResultsAsArrays(boolean descending)
  {
    initTimeseriesQueryQueryToolChestTest(descending);
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .descending(descending)
              .granularity(Granularities.HOUR)
              .aggregators(QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS)
              .postAggregators(QueryRunnerTestHelper.CONSTANT)
              .build();

    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.of(
            new Object[]{DateTimes.of("2000").getMillis(), 1L, 2L, 3L, 1L},
            new Object[]{DateTimes.of("2000T01").getMillis(), 4L, 5L, 6L, 1L}
        ),
        TOOL_CHEST.resultsAsArrays(
            query,
            Sequences.simple(
                ImmutableList.of(
                    new Result<>(
                        DateTimes.of("2000"),
                        new TimeseriesResultValue(
                            ImmutableMap.of("rows", 1L, "index", 2L, "uniques", 3L, "const", 1L)
                        )
                    ),
                    new Result<>(
                        DateTimes.of("2000T01"),
                        new TimeseriesResultValue(
                            ImmutableMap.of("rows", 4L, "index", 5L, "uniques", 6L, "const", 1L)
                        )
                    )
                )
            )
        )
    );
  }
}
