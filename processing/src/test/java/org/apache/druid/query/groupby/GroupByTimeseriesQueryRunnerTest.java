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

package org.apache.druid.query.groupby;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.aggregation.ExpressionLambdaAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerTest;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This class is for testing both timeseries and groupBy queries with the same set of queries.
 */
public class GroupByTimeseriesQueryRunnerTest extends TimeseriesQueryRunnerTest
{
  private static TestGroupByBuffers BUFFER_POOLS = null;

  @BeforeAll
  public static void setUpClass()
  {
    BUFFER_POOLS = TestGroupByBuffers.createDefault();
  }

  @AfterAll
  public static void tearDownClass()
  {
    BUFFER_POOLS.close();
    BUFFER_POOLS = null;
  }

  @SuppressWarnings("unchecked")
  public static Iterable<Object[]> constructorFeeder()
  {
    setUpClass();
    GroupByQueryConfig config = new GroupByQueryConfig();
    final GroupByQueryRunnerFactory factory = GroupByQueryRunnerTest.makeQueryRunnerFactory(config, BUFFER_POOLS);

    final List<Object[]> constructors = new ArrayList<>();

    for (QueryRunner<ResultRow> runner : QueryRunnerTestHelper.makeQueryRunnersToMerge(factory)) {
      final QueryRunner modifiedRunner = new QueryRunner()
      {
        @Override
        public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
        {
          final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
          TimeseriesQuery tsQuery = (TimeseriesQuery) queryPlus.getQuery();

          final String timeDimension = tsQuery.getTimestampResultField();
          final List<VirtualColumn> virtualColumns = new ArrayList<>(
              Arrays.asList(tsQuery.getVirtualColumns().getVirtualColumns())
          );
          Map<String, Object> theContext = tsQuery.getContext();
          if (timeDimension != null) {
            theContext = new HashMap<>(tsQuery.getContext());
            final PeriodGranularity granularity = (PeriodGranularity) tsQuery.getGranularity();
            virtualColumns.add(
                new ExpressionVirtualColumn(
                    "v0",
                    StringUtils.format("timestamp_floor(__time, '%s')", granularity.getPeriod()),
                    ColumnType.LONG,
                    TestExprMacroTable.INSTANCE
                )
            );

            theContext.put(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD, timeDimension);
            try {
              theContext.put(
                  GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_GRANULARITY,
                  jsonMapper.writeValueAsString(granularity)
              );
            }
            catch (JsonProcessingException e) {
              throw new RuntimeException(e);
            }
            theContext.put(GroupByQuery.CTX_TIMESTAMP_RESULT_FIELD_INDEX, 0);
          }

          GroupByQuery newQuery = GroupByQuery
              .builder()
              .setDataSource(tsQuery.getDataSource())
              .setQuerySegmentSpec(tsQuery.getQuerySegmentSpec())
              .setGranularity(tsQuery.getGranularity())
              .setDimFilter(tsQuery.getDimensionsFilter())
              .setDimensions(
                  timeDimension == null
                  ? ImmutableList.of()
                  : ImmutableList.of(new DefaultDimensionSpec("v0", timeDimension, ColumnType.LONG))
              )
              .setAggregatorSpecs(tsQuery.getAggregatorSpecs())
              .setPostAggregatorSpecs(tsQuery.getPostAggregatorSpecs())
              .setVirtualColumns(VirtualColumns.create(virtualColumns))
              .setLimit(tsQuery.getLimit())
              .setContext(theContext)
              .build();

          return Sequences.map(
              runner.run(queryPlus.withQuery(newQuery), responseContext),
              new Function<ResultRow, Result<TimeseriesResultValue>>()
              {
                @Override
                public Result<TimeseriesResultValue> apply(final ResultRow input)
                {
                  final MapBasedRow mapBasedRow = input.toMapBasedRow(newQuery);

                  return new Result<>(
                      mapBasedRow.getTimestamp(),
                      new TimeseriesResultValue(mapBasedRow.getEvent())
                  );
                }
              }
          );
        }

        @Override
        public String toString()
        {
          return runner.toString();
        }
      };

      for (boolean vectorize : ImmutableList.of(false, true)) {
        // Add vectorization tests for any indexes that support it.
        if (!vectorize || QueryRunnerTestHelper.isTestRunnerVectorizable(runner)) {
          constructors.add(new Object[]{modifiedRunner, vectorize});
        }
      }
    }

    return constructors;
  }

  public void initGroupByTimeseriesQueryRunnerTest(QueryRunner runner, boolean vectorize)
  {
    super(runner, false, vectorize, QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS);
  }

  // GroupBy handles timestamps differently when granularity is ALL
  @MethodSource("constructorFeeder")
  @Override
  @ParameterizedTest(name = "{0}, vectorize = {1}")
  public void testFullOnTimeseriesMaxMin(QueryRunner runner, boolean vectorize)
  {
    initGroupByTimeseriesQueryRunnerTest(runner, vectorize);
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(Granularities.ALL)
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .aggregators(
                                      new DoubleMaxAggregatorFactory("maxIndex", "index"),
                                      new DoubleMinAggregatorFactory("minIndex", "index")
                                  )
                                  .descending(descending)
                                  .build();

    DateTime expectedEarliest = DateTimes.of("1970-01-01");
    DateTime expectedLast = DateTimes.of("2011-04-15");

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    Result<TimeseriesResultValue> result = results.iterator().next();

    Assertions.assertEquals(expectedEarliest, result.getTimestamp());
    Assertions.assertFalse(
        result.getTimestamp().isAfter(expectedLast),
        StringUtils.format("Timestamp[%s] > expectedLast[%s]", result.getTimestamp(), expectedLast)
    );

    final TimeseriesResultValue value = result.getValue();

    Assertions.assertEquals(1870.061029, value.getDoubleMetric("maxIndex"), 1870.061029 * 1e-6, result.toString());
    Assertions.assertEquals(59.021022, value.getDoubleMetric("minIndex"), 59.021022 * 1e-6, result.toString());
  }

  // GroupBy handles timestamps differently when granularity is ALL
  @MethodSource("constructorFeeder")
  @Override
  @ParameterizedTest(name = "{0}, vectorize = {1}")
  public void testFullOnTimeseriesMinMaxAggregators(QueryRunner runner, boolean vectorize)
  {
    initGroupByTimeseriesQueryRunnerTest(runner, vectorize);
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(Granularities.ALL)
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .aggregators(
                                      QueryRunnerTestHelper.INDEX_LONG_MIN,
                                      QueryRunnerTestHelper.INDEX_LONG_MAX,
                                      QueryRunnerTestHelper.INDEX_DOUBLE_MIN,
                                      QueryRunnerTestHelper.INDEX_DOUBLE_MAX,
                                      QueryRunnerTestHelper.INDEX_FLOAT_MIN,
                                      QueryRunnerTestHelper.INDEX_FLOAT_MAX
                                  )
                                  .descending(descending)
                                  .build();

    DateTime expectedEarliest = DateTimes.of("1970-01-01");
    DateTime expectedLast = DateTimes.of("2011-04-15");


    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    Result<TimeseriesResultValue> result = results.iterator().next();

    Assertions.assertEquals(expectedEarliest, result.getTimestamp());
    Assertions.assertFalse(
        result.getTimestamp().isAfter(expectedLast),
        StringUtils.format("Timestamp[%s] > expectedLast[%s]", result.getTimestamp(), expectedLast)
    );
    Assertions.assertEquals(59L, (long) result.getValue().getLongMetric(QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC));
    Assertions.assertEquals(1870, (long) result.getValue().getLongMetric(QueryRunnerTestHelper.LONG_MAX_INDEX_METRIC));
    Assertions.assertEquals(
        59.021022D,
        result.getValue().getDoubleMetric(QueryRunnerTestHelper.DOUBLE_MIN_INDEX_METRIC),
        0
    );
    Assertions.assertEquals(
        1870.061029D,
        result.getValue().getDoubleMetric(QueryRunnerTestHelper.DOUBLE_MAX_INDEX_METRIC),
        0
    );
    Assertions.assertEquals(59.021023F, result.getValue().getFloatMetric(QueryRunnerTestHelper.FLOAT_MIN_INDEX_METRIC), 0);
    Assertions.assertEquals(1870.061F, result.getValue().getFloatMetric(QueryRunnerTestHelper.FLOAT_MAX_INDEX_METRIC), 0);
  }

  @MethodSource("constructorFeeder")
  @Override
  @ParameterizedTest(name = "{0}, vectorize = {1}")
  public void testEmptyTimeseries(QueryRunner runner, boolean vectorize)
  {
    initGroupByTimeseriesQueryRunnerTest(runner, vectorize);
    // Skip this test because the timeseries test expects the empty range to have one entry, but group by
    // does not expect anything
  }

  @MethodSource("constructorFeeder")
  @Override
  @ParameterizedTest(name = "{0}, vectorize = {1}")
  public void testFullOnTimeseries(QueryRunner runner, boolean vectorize)
  {
    initGroupByTimeseriesQueryRunnerTest(runner, vectorize);
    // Skip this test because the timeseries test expects a skipped day to be filled in, but group by doesn't
    // fill anything in.
  }

  @MethodSource("constructorFeeder")
  @Override
  @ParameterizedTest(name = "{0}, vectorize = {1}")
  public void testFullOnTimeseriesWithFilter(QueryRunner runner, boolean vectorize)
  {
    initGroupByTimeseriesQueryRunnerTest(runner, vectorize);
    // Skip this test because the timeseries test expects a skipped day to be filled in, but group by doesn't
    // fill anything in.
  }

  @MethodSource("constructorFeeder")
  @Override
  @ParameterizedTest(name = "{0}, vectorize = {1}")
  public void testTimeseriesQueryZeroFilling(QueryRunner runner, boolean vectorize)
  {
    initGroupByTimeseriesQueryRunnerTest(runner, vectorize);
    // Skip this test because the timeseries test expects skipped hours to be filled in, but group by doesn't
    // fill anything in.
  }

  @MethodSource("constructorFeeder")
  @Override
  @ParameterizedTest(name = "{0}, vectorize = {1}")
  public void testTimeseriesWithNonExistentFilter(QueryRunner runner, boolean vectorize)
  {
    initGroupByTimeseriesQueryRunnerTest(runner, vectorize);
    // Skip this test because the timeseries test expects a day that doesn't have a filter match to be filled in,
    // but group by just doesn't return a value if the filter doesn't match.
  }

  @MethodSource("constructorFeeder")
  @Override
  @ParameterizedTest(name = "{0}, vectorize = {1}")
  public void testTimeseriesWithNonExistentFilterAndMultiDim(QueryRunner runner, boolean vectorize)
  {
    initGroupByTimeseriesQueryRunnerTest(runner, vectorize);
    // Skip this test because the timeseries test expects a day that doesn't have a filter match to be filled in,
    // but group by just doesn't return a value if the filter doesn't match.
  }

  @MethodSource("constructorFeeder")
  @Override
  @ParameterizedTest(name = "{0}, vectorize = {1}")
  public void testTimeseriesWithFilterOnNonExistentDimension(QueryRunner runner, boolean vectorize)
  {
    initGroupByTimeseriesQueryRunnerTest(runner, vectorize);
    // Skip this test because the timeseries test expects a day that doesn't have a filter match to be filled in,
    // but group by just doesn't return a value if the filter doesn't match.
  }

  @MethodSource("constructorFeeder")
  @Override
  @ParameterizedTest(name = "{0}, vectorize = {1}")
  public void testTimeseriesWithExpressionAggregatorTooBig(QueryRunner runner, boolean vectorize)
  {
    Throwable exception = assertThrows(Exception.class, () -> {
      initGroupByTimeseriesQueryRunnerTest(runner, vectorize);
      cannotVectorize();
      if (!vectorize) {
        // size bytes when it overshoots varies slightly between algorithms
        expectedException.expectMessage("Unable to serialize [ARRAY<STRING>]");
      }
      TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
          .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
          .granularity(Granularities.DAY)
          .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
          .aggregators(
              Collections.singletonList(
                  new ExpressionLambdaAggregatorFactory(
                      "array_agg_distinct",
                      ImmutableSet.of(QueryRunnerTestHelper.MARKET_DIMENSION),
                      "acc",
                      "[]",
                      null,
                      null,
                      true,
                      false,
                      "array_set_add(acc, market)",
                      "array_set_add_all(acc, array_agg_distinct)",
                      null,
                      null,
                      HumanReadableBytes.valueOf(10),
                      TestExprMacroTable.INSTANCE
                  )
              )
          )
          .descending(descending)
          .context(makeContext())
          .build();

      runner.run(QueryPlus.wrap(query)).toList();
    });
    assertTrue(exception.getMessage().contains("Unable to serialize [ARRAY<STRING>]"));
  }

  @MethodSource("constructorFeeder")
  @Override
  @ParameterizedTest(name = "{0}, vectorize = {1}")
  public void testTimeseriesWithInvertedFilterOnNonExistentDimension(QueryRunner runner, boolean vectorize)
  {
    initGroupByTimeseriesQueryRunnerTest(runner, vectorize);
    if (NullHandling.replaceWithDefault()) {
      super.testTimeseriesWithInvertedFilterOnNonExistentDimension();
      return;
    }
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(new NotDimFilter(new SelectorDimFilter("bobby", "sally", null)))
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
                                  .build();


    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query))
                                                            .toList();
    // group by query results are empty instead of day bucket results with zeros and nulls
    Assertions.assertEquals(Collections.emptyList(), results);
  }
}
