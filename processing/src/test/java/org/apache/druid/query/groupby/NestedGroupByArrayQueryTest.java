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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ComparableList;
import org.apache.druid.segment.data.ComparableStringArray;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class NestedGroupByArrayQueryTest
{
  private static final Logger LOG = new Logger(NestedDataGroupByQueryTest.class);

  @TempDir
  public File tempFolder;

  private Closer closer;
  private QueryContexts.Vectorize vectorize;
  private AggregationTestHelper helper;
  private BiFunction<File, Closer, List<Segment>> segmentsGenerator;

  public void initNestedGroupByArrayQueryTest(
      GroupByQueryConfig config,
      BiFunction<File, Closer, List<Segment>> segmentGenerator,
      String vectorize
  )
  {
    NestedDataModule.registerHandlersAndSerde();
    this.vectorize = QueryContexts.Vectorize.fromString(vectorize);
    this.helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        NestedDataModule.getJacksonModulesList(),
        config,
        tempFolder
    );
    this.segmentsGenerator = segmentGenerator;
    this.closer = Closer.create();
  }

  public Map<String, Object> getContext()
  {
    return ImmutableMap.of(
        QueryContexts.VECTORIZE_KEY, vectorize.toString(),
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize.toString()
    );
  }

  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    final List<BiFunction<File, Closer, List<Segment>>> segmentsGenerators =
        NestedDataTestUtils.getSegmentGenerators(NestedDataTestUtils.ARRAY_TYPES_DATA_FILE);

    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      for (BiFunction<File, Closer, List<Segment>> generatorFn : segmentsGenerators) {
        // skip force because arrays don't really support vectorize engine, but we want the coverage for once they do...
        for (String vectorize : new String[]{"false", "true"}) {
          constructors.add(new Object[]{config, generatorFn, vectorize});
        }
      }
    }
    return constructors;
  }

  @BeforeEach
  public void setup()
  {
  }

  @AfterEach
  public void teardown() throws IOException
  {
    closer.close();
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "config = {0}, segments = {1}, vectorize = {2}")
  public void testGroupByRootArrayString(GroupByQueryConfig config, BiFunction<File, Closer, List<Segment>> segmentGenerator, String vectorize)
  {
    initNestedGroupByArrayQueryTest(config, segmentGenerator, vectorize);
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("arrayString", ColumnType.STRING_ARRAY))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{null, 8L},
            new Object[]{ComparableStringArray.of("a", "b"), 8L},
            new Object[]{ComparableStringArray.of("a", "b", "c"), 4L},
            new Object[]{ComparableStringArray.of("b", "c"), 4L},
            new Object[]{ComparableStringArray.of("d", "e"), 4L}
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "config = {0}, segments = {1}, vectorize = {2}")
  public void testGroupByRootArrayLong(GroupByQueryConfig config, BiFunction<File, Closer, List<Segment>> segmentGenerator, String vectorize)
  {
    initNestedGroupByArrayQueryTest(config, segmentGenerator, vectorize);
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("arrayLong", ColumnType.LONG_ARRAY))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{null, 8L},
            new Object[]{asComparableList(1L, 2L, 3L), 8L},
            new Object[]{asComparableList(1L, 2L, 3L, 4L), 4L},
            new Object[]{asComparableList(1L, 4L), 4L},
            new Object[]{asComparableList(2L, 3L), 4L}
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "config = {0}, segments = {1}, vectorize = {2}")
  public void testGroupByRootArrayDouble(GroupByQueryConfig config, BiFunction<File, Closer, List<Segment>> segmentGenerator, String vectorize)
  {
    initNestedGroupByArrayQueryTest(config, segmentGenerator, vectorize);
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("arrayDouble", ColumnType.DOUBLE_ARRAY))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{null, 8L},
            new Object[]{asComparableList(1.1, 2.2, 3.3), 8L},
            new Object[]{asComparableList(1.1, 3.3), 4L},
            new Object[]{asComparableList(2.2, 3.3, 4.0), 4L},
            new Object[]{asComparableList(3.3, 4.4, 5.5), 4L}
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "config = {0}, segments = {1}, vectorize = {2}")
  public void testGroupByRootArrayStringElement(GroupByQueryConfig config, BiFunction<File, Closer, List<Segment>> segmentGenerator, String vectorize)
  {
    initNestedGroupByArrayQueryTest(config, segmentGenerator, vectorize);
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0", ColumnType.STRING))
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn(
                                                  "arrayString",
                                                  "$[2]",
                                                  "v0",
                                                  ColumnType.STRING
                                              )
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{null, 24L},
            new Object[]{"c", 4L}
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "config = {0}, segments = {1}, vectorize = {2}")
  public void testGroupByRootArrayStringElementDouble(GroupByQueryConfig config, BiFunction<File, Closer, List<Segment>> segmentGenerator, String vectorize)
  {
    initNestedGroupByArrayQueryTest(config, segmentGenerator, vectorize);
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0", ColumnType.DOUBLE))
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn(
                                                  "arrayString",
                                                  "$[2]",
                                                  "v0",
                                                  ColumnType.DOUBLE
                                              )
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{NullHandling.defaultDoubleValue(), 28L}
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "config = {0}, segments = {1}, vectorize = {2}")
  public void testGroupByRootArrayStringElementLong(GroupByQueryConfig config, BiFunction<File, Closer, List<Segment>> segmentGenerator, String vectorize)
  {
    initNestedGroupByArrayQueryTest(config, segmentGenerator, vectorize);
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0", ColumnType.LONG))
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn(
                                                  "arrayString",
                                                  "$[2]",
                                                  "v0",
                                                  ColumnType.LONG
                                              )
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{NullHandling.defaultLongValue(), 28L}
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "config = {0}, segments = {1}, vectorize = {2}")
  public void testGroupByRootArrayStringElementFloat(GroupByQueryConfig config, BiFunction<File, Closer, List<Segment>> segmentGenerator, String vectorize)
  {
    initNestedGroupByArrayQueryTest(config, segmentGenerator, vectorize);
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0", ColumnType.FLOAT))
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn(
                                                  "arrayString",
                                                  "$[2]",
                                                  "v0",
                                                  ColumnType.FLOAT
                                              )
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{NullHandling.defaultFloatValue(), 28L}
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "config = {0}, segments = {1}, vectorize = {2}")
  public void testGroupByRootArrayLongElement(GroupByQueryConfig config, BiFunction<File, Closer, List<Segment>> segmentGenerator, String vectorize)
  {
    initNestedGroupByArrayQueryTest(config, segmentGenerator, vectorize);
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0", ColumnType.LONG))
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn(
                                                  "arrayLong",
                                                  "$[2]",
                                                  "v0",
                                                  ColumnType.LONG
                                              )
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{NullHandling.defaultLongValue(), 16L},
            new Object[]{3L, 12L}
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "config = {0}, segments = {1}, vectorize = {2}")
  public void testGroupByRootArrayLongElementDouble(GroupByQueryConfig config, BiFunction<File, Closer, List<Segment>> segmentGenerator, String vectorize)
  {
    initNestedGroupByArrayQueryTest(config, segmentGenerator, vectorize);
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0", ColumnType.DOUBLE))
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn(
                                                  "arrayLong",
                                                  "$[2]",
                                                  "v0",
                                                  ColumnType.DOUBLE
                                              )
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{NullHandling.defaultDoubleValue(), 16L},
            new Object[]{3.0, 12L}
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "config = {0}, segments = {1}, vectorize = {2}")
  public void testGroupByRootArrayLongElementFloat(GroupByQueryConfig config, BiFunction<File, Closer, List<Segment>> segmentGenerator, String vectorize)
  {
    initNestedGroupByArrayQueryTest(config, segmentGenerator, vectorize);
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0", ColumnType.FLOAT))
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn(
                                                  "arrayLong",
                                                  "$[2]",
                                                  "v0",
                                                  ColumnType.FLOAT
                                              )
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{NullHandling.defaultFloatValue(), 16L},
            new Object[]{3.0f, 12L}
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "config = {0}, segments = {1}, vectorize = {2}")
  public void testGroupByRootArrayLongElementString(GroupByQueryConfig config, BiFunction<File, Closer, List<Segment>> segmentGenerator, String vectorize)
  {
    initNestedGroupByArrayQueryTest(config, segmentGenerator, vectorize);
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("v0", ColumnType.STRING))
                                          .setVirtualColumns(
                                              new NestedFieldVirtualColumn(
                                                  "arrayLong",
                                                  "$[2]",
                                                  "v0",
                                                  ColumnType.STRING
                                              )
                                          )
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{null, 16L},
            new Object[]{"3", 12L}
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "config = {0}, segments = {1}, vectorize = {2}")
  public void testGroupByEmptyIshArrays(GroupByQueryConfig config, BiFunction<File, Closer, List<Segment>> segmentGenerator, String vectorize)
  {
    initNestedGroupByArrayQueryTest(config, segmentGenerator, vectorize);
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(DefaultDimensionSpec.of("arrayNoType", ColumnType.LONG_ARRAY))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();


    runResults(
        groupQuery,
        ImmutableList.of(
            new Object[]{null, 4L},
            new Object[]{new ComparableList<>(Collections.emptyList()), 18L},
            new Object[]{new ComparableList<>(Collections.singletonList(null)), 4L},
            new Object[]{new ComparableList<>(Arrays.asList(null, null)), 2L}
        )
    );
  }

  private void runResults(
      GroupByQuery groupQuery,
      List<Object[]> expectedResults
  )
  {
    Supplier<List<ResultRow>> runner =
        () -> helper.runQueryOnSegmentsObjs(segmentsGenerator.apply(tempFolder, closer), groupQuery).toList();

    List<ResultRow> results = runner.get();
    verifyResults(
        groupQuery,
        results,
        expectedResults
    );
  }

  private static void verifyResults(GroupByQuery query, List<ResultRow> results, List<Object[]> expected)
  {
    RowSignature rowSignature = query.getResultRowSignature();
    List<ResultRow> serdeAndBack =
        results.stream()
               .peek(
                   row -> GroupingEngine.convertRowTypesToOutputTypes(
                       query.getDimensions(),
                       row,
                       query.getResultRowDimensionStart()
                   )
               )
               .collect(Collectors.toList());
    LOG.info("results:\n%s", serdeAndBack.stream().map(ResultRow::toString).collect(Collectors.joining("\n")));
    Assertions.assertEquals(expected.size(), serdeAndBack.size());
    for (int i = 0; i < expected.size(); i++) {
      final Object[] resultRow = serdeAndBack.get(i).getArray();
      Assertions.assertEquals(expected.get(i).length, resultRow.length);
      for (int j = 0; j < resultRow.length; j++) {
        if (expected.get(i)[j] == null) {
          Assertions.assertNull(resultRow[j]);
        } else if (rowSignature.getColumnType(j).map(t -> t.is(ValueType.DOUBLE)).orElse(false)) {
          Assertions.assertEquals((Double) expected.get(i)[j], (Double) resultRow[j], 0.01);
        } else if (rowSignature.getColumnType(j).map(t -> t.is(ValueType.FLOAT)).orElse(false)) {
          Assertions.assertEquals((Float) expected.get(i)[j], (Float) resultRow[j], 0.01);
        } else {
          Assertions.assertEquals(expected.get(i)[j], resultRow[j]);
        }
      }
    }
  }

  public static <T extends Comparable> ComparableList<T> asComparableList(T... objects)
  {
    return new ComparableList<>(Arrays.asList(objects));
  }
}
