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

package org.apache.druid.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.FunctionalIterable;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.timeline.SegmentId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests designed to exercise changing column types, adding columns, removing columns, etc.
 */
public class SchemaEvolutionTest
{
  private static final String DATA_SOURCE = "foo";
  private static final String TIMESTAMP_COLUMN = "t";
  private static final double THIRTY_ONE_POINT_ONE = 31.1d;

  public static Object[] doVectorize()
  {
    return Lists.newArrayList(true, false).toArray();
  }

  public static List<Result<TimeseriesResultValue>> timeseriesResult(final Map<String, ?> map)
  {
    return ImmutableList.of(new Result<>(DateTimes.of("2000"), new TimeseriesResultValue((Map<String, Object>) map)));
  }

  public static List<InputRow> inputRowsWithDimensions(final List<String> dimensions)
  {
    final MapInputRowParser parser = new MapInputRowParser(
        new TimeAndDimsParseSpec(
            new TimestampSpec(TIMESTAMP_COLUMN, "iso", null),
            DimensionsSpec.builder()
                          .setDimensions(DimensionsSpec.getDefaultSchemas(dimensions))
                          .setDimensionExclusions(dimensions.isEmpty() ? ImmutableList.of("t", "c1", "c2") : null)
                          .build()
        )
    );
    return ImmutableList.of(
        parser.parseBatch(ImmutableMap.of("t", "2000-01-01", "c1", "9", "c2", ImmutableList.of("a"))).get(0),
        parser.parseBatch(ImmutableMap.of("t", "2000-01-02", "c1", "10.1", "c2", ImmutableList.of())).get(0),
        parser.parseBatch(ImmutableMap.of("t", "2000-01-03", "c1", "2", "c2", ImmutableList.of(""))).get(0),
        parser.parseBatch(ImmutableMap.of("t", "2001-01-01", "c1", "1", "c2", ImmutableList.of("a", "c"))).get(0),
        parser.parseBatch(ImmutableMap.of("t", "2001-01-02", "c1", "4", "c2", ImmutableList.of("abc"))).get(0),
        parser.parseBatch(ImmutableMap.of("t", "2001-01-03", "c1", "5")).get(0)
    );
  }

  public static <T, QueryType extends Query<T>> List<T> runQuery(
      final QueryType query,
      final QueryRunnerFactory<T, QueryType> factory,
      final List<QueryableIndex> indexes
  )
  {
    final Sequence<T> results = new FinalizeResultsQueryRunner<>(
        factory.getToolchest().mergeResults(
            factory.mergeRunners(
                Execs.directExecutor(),
                FunctionalIterable
                    .create(indexes)
                    .transform(
                        index -> factory.createRunner(new QueryableIndexSegment(index, SegmentId.dummy("xxx")))
                    )
            )
        ),
        (QueryToolChest<T, Query<T>>) factory.getToolchest()
    ).run(QueryPlus.wrap(query));
    return results.toList();
  }

  @TempDir
  public File temporaryFolder;

  // Index1: c1 is a string, c2 nonexistent, "uniques" nonexistent
  private QueryableIndex index1 = null;

  // Index2: c1 is a long, c2 is a string, "uniques" is uniques on c2
  private QueryableIndex index2 = null;

  // Index3: c1 is a float, c2 is a string, "uniques" is uniques on c2
  private QueryableIndex index3 = null;

  // Index4: c1 is nonexistent, c2 is uniques on c2
  private QueryableIndex index4 = null;

  @BeforeEach
  public void setUp() throws IOException
  {
    NullHandling.initializeForTests();

    // Index1: c1 is a string, c2 nonexistent, "uniques" nonexistent
    index1 = IndexBuilder.create()
                         .tmpDir(newFolder(temporaryFolder, "junit"))
                         .schema(
                             new IncrementalIndexSchema.Builder()
                                 .withMetrics(new CountAggregatorFactory("cnt"))
                                 .withRollup(false)
                                 .build()
                         )
                         .rows(inputRowsWithDimensions(ImmutableList.of("c1")))
                         .buildMMappedIndex();

    // Index2: c1 is a long, c2 is a string, "uniques" is uniques on c2, "longmin" is min on c1
    index2 = IndexBuilder.create()
                         .tmpDir(newFolder(temporaryFolder, "junit"))
                         .schema(
                             new IncrementalIndexSchema.Builder()
                                 .withMetrics(
                                     new CountAggregatorFactory("cnt"),
                                     new LongSumAggregatorFactory("c1", "c1"),
                                     new HyperUniquesAggregatorFactory("uniques", "c2"),
                                     new LongMinAggregatorFactory("longmin", "c1")
                                 )
                                 .withRollup(false)
                                 .build()
                         )
                         .rows(inputRowsWithDimensions(ImmutableList.of("c2")))
                         .buildMMappedIndex();

    // Index3: c1 is a float, c2 is a string, "uniques" is uniques on c2
    index3 = IndexBuilder.create()
                         .tmpDir(newFolder(temporaryFolder, "junit"))
                         .schema(
                             new IncrementalIndexSchema.Builder()
                                 .withMetrics(
                                     new CountAggregatorFactory("cnt"),
                                     new DoubleSumAggregatorFactory("c1", "c1"),
                                     new HyperUniquesAggregatorFactory("uniques", "c2")
                                 )
                                 .withRollup(false)
                                 .build()
                         )
                         .rows(inputRowsWithDimensions(ImmutableList.of("c2")))
                         .buildMMappedIndex();

    // Index4: c1 is nonexistent, c2 is uniques on c2
    index4 = IndexBuilder.create()
                         .tmpDir(newFolder(temporaryFolder, "junit"))
                         .schema(
                             new IncrementalIndexSchema.Builder()
                                 .withMetrics(new HyperUniquesAggregatorFactory("c2", "c2"))
                                 .withRollup(false)
                                 .build()
                         )
                         .rows(inputRowsWithDimensions(ImmutableList.of()))
                         .buildMMappedIndex();

    if (index4.getAvailableDimensions().size() != 0) {
      // Just double-checking that the exclusions are working properly
      throw new ISE("Expected no dimensions in index4");
    }
  }

  @AfterEach
  public void tearDown() throws IOException
  {
    Closeables.close(index1, false);
    Closeables.close(index2, false);
    Closeables.close(index3, false);
    Closeables.close(index4, false);
  }

  @ParameterizedTest
  @MethodSource("doVectorize")
  public void testHyperUniqueEvolutionTimeseries(boolean doVectorize)
  {
    final TimeseriesQueryRunnerFactory factory = QueryRunnerTestHelper.newTimeseriesQueryRunnerFactory();

    final TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(DATA_SOURCE)
        .intervals("1000/3000")
        .aggregators(
            ImmutableList.of(
                new HyperUniquesAggregatorFactory("uniques", "uniques")
            )
        )
        .context(ImmutableMap.of(QueryContexts.VECTORIZE_KEY, doVectorize))
        .build();

    // index1 has no "uniques" column
    Assertions.assertEquals(
        timeseriesResult(ImmutableMap.of("uniques", 0d)),
        runQuery(query, factory, ImmutableList.of(index1))
    );

    // index1 (no uniques) + index2 and index3 (yes uniques); we should be able to combine
    Assertions.assertEquals(
        timeseriesResult(ImmutableMap.of("uniques", 4.003911343725148d)),
        runQuery(query, factory, ImmutableList.of(index1, index2, index3))
    );
  }

  @ParameterizedTest
  @MethodSource("doVectorize")
  public void testNumericEvolutionTimeseriesAggregation(boolean doVectorize)
  {
    final TimeseriesQueryRunnerFactory factory = QueryRunnerTestHelper.newTimeseriesQueryRunnerFactory();

    // "c1" changes from string(1) -> long(2) -> float(3) -> nonexistent(4)
    // test behavior of longSum/doubleSum with/without expressions
    final TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(DATA_SOURCE)
        .intervals("1000/3000")
        .aggregators(
            ImmutableList.of(
                new LongSumAggregatorFactory("a", "c1"),
                new DoubleSumAggregatorFactory("b", "c1"),
                new LongSumAggregatorFactory("c", null, "c1 * 1", TestExprMacroTable.INSTANCE),
                new DoubleSumAggregatorFactory("d", null, "c1 * 1", TestExprMacroTable.INSTANCE)
            )
        )
        .context(ImmutableMap.of(QueryContexts.VECTORIZE_KEY, doVectorize))
        .build();

    // Only string(1)
    // Note: Expressions implicitly cast strings to numbers, leading to the a/b vs c/d difference.
    Assertions.assertEquals(
        timeseriesResult(ImmutableMap.of("a", 31L, "b", THIRTY_ONE_POINT_ONE, "c", 31L, "d", THIRTY_ONE_POINT_ONE)),
        runQuery(query, factory, ImmutableList.of(index1))
    );

    // Only long(2)
    Assertions.assertEquals(
        timeseriesResult(ImmutableMap.of("a", 31L, "b", 31.0, "c", 31L, "d", 31.0)),
        runQuery(query, factory, ImmutableList.of(index2))
    );

    // Only float(3)
    Assertions.assertEquals(
        timeseriesResult(ImmutableMap.of("a", 31L, "b", THIRTY_ONE_POINT_ONE, "c", 31L, "d", THIRTY_ONE_POINT_ONE)),
        runQuery(query, factory, ImmutableList.of(index3))
    );

    // Only nonexistent(4)
    Map<String, Object> result = new HashMap<>();
    result.put("a", NullHandling.defaultLongValue());
    result.put("b", NullHandling.defaultDoubleValue());
    result.put("c", NullHandling.defaultLongValue());
    result.put("d", NullHandling.defaultDoubleValue());
    Assertions.assertEquals(
        timeseriesResult(result),
        runQuery(query, factory, ImmutableList.of(index4))
    );

    // string(1) + long(2) + float(3) + nonexistent(4)
    // Note: Expressions implicitly cast strings to numbers, leading to the a/b vs c/d difference.
    Assertions.assertEquals(
        timeseriesResult(ImmutableMap.of(
            "a", 31L * 3,
            "b", THIRTY_ONE_POINT_ONE * 2 + 31,
            "c", 31L * 3,
            "d", THIRTY_ONE_POINT_ONE * 2 + 31
        )),
        runQuery(query, factory, ImmutableList.of(index1, index2, index3, index4))
    );

    // long(2) + float(3) + nonexistent(4)
    Assertions.assertEquals(
        timeseriesResult(ImmutableMap.of(
            "a", 31L * 2,
            "b", THIRTY_ONE_POINT_ONE + 31,
            "c", 31L * 2,
            "d", THIRTY_ONE_POINT_ONE + 31
        )),
        runQuery(query, factory, ImmutableList.of(index2, index3, index4))
    );
  }

  @ParameterizedTest
  @MethodSource("doVectorize")
  public void testNumericEvolutionFiltering(boolean doVectorize)
  {
    final TimeseriesQueryRunnerFactory factory = QueryRunnerTestHelper.newTimeseriesQueryRunnerFactory();

    // "c1" changes from string(1) -> long(2) -> float(3) -> nonexistent(4)
    // test behavior of filtering
    final TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(DATA_SOURCE)
        .intervals("1000/3000")
        .filters(new BoundDimFilter("c1", "9", "11", false, false, null, null, StringComparators.NUMERIC))
        .aggregators(
            ImmutableList.of(
                new LongSumAggregatorFactory("a", "c1"),
                new DoubleSumAggregatorFactory("b", "c1"),
                new FloatSumAggregatorFactory("d", "c1"),
                new LongMinAggregatorFactory("e", "c1"),
                new CountAggregatorFactory("c")
            )
        )
        .context(ImmutableMap.of(QueryContexts.VECTORIZE_KEY, doVectorize))
        .build();

    // Only string(1) -- which we can filter but not aggregate
    Assertions.assertEquals(
        timeseriesResult(ImmutableMap.of("a", 19L, "b", 19.1, "c", 2L, "d", 19.1f, "e", 9L)),
        runQuery(query, factory, ImmutableList.of(index1))
    );

     // Only long(2) -- which we can filter and aggregate
    Assertions.assertEquals(
        timeseriesResult(ImmutableMap.of("a", 19L, "b", 19.0, "c", 2L, "d", 19.0f, "e", 9L)),
        runQuery(query, factory, ImmutableList.of(index2))
    );

    // Only float(3) -- which we can't filter, but can aggregate
    Assertions.assertEquals(
        timeseriesResult(ImmutableMap.of("a", 19L, "b", 19.1, "c", 2L, "d", 19.1f, "e", 9L)),
        runQuery(query, factory, ImmutableList.of(index3))
    );

    // Only nonexistent(4)
    Assertions.assertEquals(
        timeseriesResult(TestHelper.makeMap(
            "a",
            NullHandling.defaultLongValue(),
            "b",
            NullHandling.defaultDoubleValue(),
            "c",
            0L,
            "d",
            NullHandling.defaultFloatValue(),
            "e",
            NullHandling.sqlCompatible() ? null : Long.MAX_VALUE
        )),
        runQuery(query, factory, ImmutableList.of(index4))
    );

    // string(1) + long(2) + float(3) + nonexistent(4)
    Assertions.assertEquals(
        timeseriesResult(ImmutableMap.of(
            "a", 57L,
            "b", 57.2,
            "c", 6L,
            "d", 57.20000076293945,
            "e", 9L
        )),
        runQuery(query, factory, ImmutableList.of(index1, index2, index3, index4))
    );
  }

  private static File newFolder(File root, String... subDirs) throws IOException {
    String subFolder = String.join("/", subDirs);
    File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }
}
