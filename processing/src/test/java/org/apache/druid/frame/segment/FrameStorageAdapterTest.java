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

package org.apache.druid.frame.segment;

import com.google.common.collect.ImmutableList;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.extraction.TimeFormatExtractionFn;
import org.apache.druid.query.extraction.UpperExtractionFn;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Interval;
import org.junit.experimental.runners.Enclosed;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runner.RunWith;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;



public class FrameStorageAdapterTest
{
  /**
   * Basic tests: everything except makeCursors, makeVectorCursor.
   */
  @Nested
  public class BasicTests extends InitializedNullHandlingTest
  {
    private FrameType frameType;

    private StorageAdapter queryableAdapter;
    private FrameSegment frameSegment;
    private StorageAdapter frameAdapter;

    public void initBasicTests(final FrameType frameType)
    {
      this.frameType = frameType;
    }

    public static Iterable<Object[]> constructorFeeder()
    {
      final List<Object[]> constructors = new ArrayList<>();

      for (FrameType frameType : FrameType.values()) {
        constructors.add(new Object[]{frameType});
      }

      return constructors;
    }

    @BeforeEach
    public void setUp()
    {

      queryableAdapter = new QueryableIndexStorageAdapter(TestIndex.getMMappedTestIndex());
      frameSegment = FrameTestUtil.adapterToFrameSegment(queryableAdapter, frameType);
      frameAdapter = frameSegment.asStorageAdapter();
    }

    @AfterEach
    public void tearDown()
    {
      if (frameSegment != null) {
        frameSegment.close();
      }
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "frameType = {0}")
    public void test_getInterval(final FrameType frameType)
    {
      initBasicTests(frameType);
      Assertions.assertEquals(queryableAdapter.getInterval(), frameAdapter.getInterval());
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "frameType = {0}")
    public void test_getRowSignature(final FrameType frameType)
    {
      initBasicTests(frameType);
      Assertions.assertEquals(queryableAdapter.getRowSignature(), frameAdapter.getRowSignature());
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "frameType = {0}")
    public void test_getAvailableDimensions(final FrameType frameType)
    {
      initBasicTests(frameType);
      // All columns are dimensions to the frameAdapter.
      Assertions.assertEquals(
          queryableAdapter.getRowSignature().getColumnNames(),
          ImmutableList.copyOf(frameAdapter.getAvailableDimensions())
      );
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "frameType = {0}")
    public void test_getAvailableMetrics(final FrameType frameType)
    {
      initBasicTests(frameType);
      // All columns are dimensions to the frameAdapter.
      Assertions.assertEquals(Collections.emptyList(), frameAdapter.getAvailableMetrics());
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "frameType = {0}")
    public void test_getDimensionCardinality_knownColumns(final FrameType frameType)
    {
      initBasicTests(frameType);
      for (final String columnName : frameAdapter.getRowSignature().getColumnNames()) {
        Assertions.assertEquals(
            DimensionDictionarySelector.CARDINALITY_UNKNOWN,
            frameAdapter.getDimensionCardinality(columnName),
            columnName
        );
      }
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "frameType = {0}")
    public void test_getDimensionCardinality_unknownColumn(final FrameType frameType)
    {
      initBasicTests(frameType);
      Assertions.assertEquals(
          DimensionDictionarySelector.CARDINALITY_UNKNOWN,
          frameAdapter.getDimensionCardinality("nonexistent")
      );
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "frameType = {0}")
    public void test_getColumnCapabilities_typeOfKnownColumns(final FrameType frameType)
    {
      initBasicTests(frameType);
      for (final String columnName : frameAdapter.getRowSignature().getColumnNames()) {
        final ColumnCapabilities expectedCapabilities = queryableAdapter.getColumnCapabilities(columnName);
        final ColumnCapabilities actualCapabilities = frameAdapter.getColumnCapabilities(columnName);

        Assertions.assertEquals(
            expectedCapabilities.toColumnType(),
            actualCapabilities.toColumnType(),
            StringUtils.format("column [%s] type", columnName)
        );

        if (frameType == FrameType.COLUMNAR) {
          // Columnar frames retain fine-grained hasMultipleValues information
          Assertions.assertEquals(
              expectedCapabilities.hasMultipleValues(),
              actualCapabilities.hasMultipleValues(),
              StringUtils.format("column [%s] hasMultipleValues", columnName)
          );
        } else {
          // Row-based frames do not retain fine-grained hasMultipleValues information
          Assertions.assertEquals(
              expectedCapabilities.getType() == ValueType.STRING
              ? ColumnCapabilities.Capable.UNKNOWN
              : ColumnCapabilities.Capable.FALSE,
              actualCapabilities.hasMultipleValues(),
              StringUtils.format("column [%s] hasMultipleValues", columnName)
          );
        }
      }
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "frameType = {0}")
    public void test_getColumnCapabilities_unknownColumn(final FrameType frameType)
    {
      initBasicTests(frameType);
      Assertions.assertNull(frameAdapter.getColumnCapabilities("nonexistent"));
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "frameType = {0}")
    public void test_getMinTime(final FrameType frameType)
    {
      initBasicTests(frameType);
      Assertions.assertEquals(queryableAdapter.getInterval().getStart(), frameAdapter.getMinTime());
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "frameType = {0}")
    public void test_getMaxTime(final FrameType frameType)
    {
      initBasicTests(frameType);
      Assertions.assertEquals(queryableAdapter.getInterval().getEnd().minus(1), frameAdapter.getMaxTime());
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "frameType = {0}")
    public void test_getNumRows(final FrameType frameType)
    {
      initBasicTests(frameType);
      Assertions.assertEquals(queryableAdapter.getNumRows(), frameAdapter.getNumRows());
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "frameType = {0}")
    public void test_getMetadata(final FrameType frameType)
    {
      initBasicTests(frameType);
      Assertions.assertNull(frameAdapter.getMetadata());
    }
  }

  /**
   * CursorTests: matrix of tests of makeCursors, makeVectorCursor
   */
  @Nested
  public class CursorTests extends InitializedNullHandlingTest
  {
    private static final int VECTOR_SIZE = 7;

    private FrameType frameType;
    @Nullable
    private Filter filter;
    private Interval interval;
    private VirtualColumns virtualColumns;
    private boolean descending;

    private StorageAdapter queryableAdapter;
    private FrameSegment frameSegment;
    private StorageAdapter frameAdapter;

    public void initCursorTests(
        FrameType frameType,
        @Nullable DimFilter filter,
        Interval interval,
        VirtualColumns virtualColumns,
        boolean descending
    )
    {
      this.frameType = frameType;
      this.filter = Filters.toFilter(filter);
      this.interval = interval;
      this.virtualColumns = virtualColumns;
      this.descending = descending;
    }

    public static Iterable<Object[]> constructorFeeder()
    {
      final List<Object[]> constructors = new ArrayList<>();
      final List<Interval> intervals = Arrays.asList(
          TestIndex.getMMappedTestIndex().getDataInterval(),
          Intervals.ETERNITY,
          Intervals.of("2011-04-01T00:00:00.000Z/2011-04-02T00:00:00.000Z"),
          Intervals.of("3001/3002")
      );

      final List<Pair<DimFilter, VirtualColumns>> filtersAndVirtualColumns = new ArrayList<>();
      filtersAndVirtualColumns.add(Pair.of(null, VirtualColumns.EMPTY));
      filtersAndVirtualColumns.add(Pair.of(new SelectorDimFilter("quality", "automotive", null), VirtualColumns.EMPTY));
      filtersAndVirtualColumns.add(Pair.of(
          new SelectorDimFilter("expr", "1401", null),
          VirtualColumns.create(
              ImmutableList.of(
                  new ExpressionVirtualColumn(
                      "expr",
                      "qualityLong + 1",
                      ColumnType.LONG,
                      ExprMacroTable.nil()
                  )
              )
          )
      ));
      filtersAndVirtualColumns.add(Pair.of(new SelectorDimFilter("qualityLong", "1400", null), VirtualColumns.EMPTY));
      filtersAndVirtualColumns.add(Pair.of(
          new SelectorDimFilter("quality", "automotive", new UpperExtractionFn(null)),
          VirtualColumns.EMPTY
      ));
      filtersAndVirtualColumns.add(Pair.of(new SelectorDimFilter(
          ColumnHolder.TIME_COLUMN_NAME,
          "Friday",
          new TimeFormatExtractionFn("EEEE", null, null, null, false)
      ), VirtualColumns.EMPTY));
      filtersAndVirtualColumns.add(Pair.of(new SelectorDimFilter(
          ColumnHolder.TIME_COLUMN_NAME,
          "Friday",
          new TimeFormatExtractionFn("EEEE", null, null, null, false)
      ), VirtualColumns.EMPTY));

      for (FrameType frameType : FrameType.values()) {
        for (Pair<DimFilter, VirtualColumns> filterVirtualColumnsPair : filtersAndVirtualColumns) {
          for (Interval interval : intervals) {
            for (boolean descending : Arrays.asList(false, true)) {
              constructors.add(
                  new Object[]{
                      frameType,
                      filterVirtualColumnsPair.lhs,
                      interval,
                      filterVirtualColumnsPair.rhs,
                      descending
                  }
              );
            }
          }
        }
      }

      return constructors;
    }

    @BeforeEach
    public void setUp()
    {
      queryableAdapter = new QueryableIndexStorageAdapter(TestIndex.getMMappedTestIndex());
      frameSegment = FrameTestUtil.adapterToFrameSegment(queryableAdapter, frameType);
      frameAdapter = frameSegment.asStorageAdapter();
    }

    @AfterEach
    public void tearDown()
    {
      if (frameSegment != null) {
        frameSegment.close();
      }
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "frameType = {0}, "
        + "filter = {1}, "
        + "interval = {2}, "
        + "virtualColumns = {3}, "
        + "descending = {4}")
    public void test_makeCursors(FrameType frameType, @Nullable DimFilter filter, Interval interval, VirtualColumns virtualColumns, boolean descending)
    {
      initCursorTests(frameType, filter, interval, virtualColumns, descending);
      assertCursorsMatch(
          adapter ->
              adapter.makeCursors(
                  filter,
                  interval,
                  virtualColumns,
                  Granularities.ALL, // Frames only support Granularities.ALL: no point testing the others.
                  descending,
                  null
              )
      );
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "frameType = {0}, "
        + "filter = {1}, "
        + "interval = {2}, "
        + "virtualColumns = {3}, "
        + "descending = {4}")
    public void test_makeVectorCursor(FrameType frameType, @Nullable DimFilter filter, Interval interval, VirtualColumns virtualColumns, boolean descending)
    {
      initCursorTests(frameType, filter, interval, virtualColumns, descending);
      Assumptions.assumeTrue(frameAdapter.canVectorize(filter, virtualColumns, descending));

      assertVectorCursorsMatch(
          adapter ->
              adapter.makeVectorCursor(
                  filter,
                  interval,
                  virtualColumns,
                  descending,
                  VECTOR_SIZE,
                  null
              )
      );
    }

    private void assertCursorsMatch(final Function<StorageAdapter, Sequence<Cursor>> call)
    {
      final RowSignature signature = frameAdapter.getRowSignature();
      final Sequence<List<Object>> queryableRows =
          call.apply(queryableAdapter).flatMap(cursor -> FrameTestUtil.readRowsFromCursor(cursor, signature));
      final Sequence<List<Object>> frameRows =
          call.apply(frameAdapter)
              .flatMap(cursor -> FrameTestUtil.readRowsFromCursor(advanceAndReset(cursor), signature));
      FrameTestUtil.assertRowsEqual(queryableRows, frameRows);
    }

    private void assertVectorCursorsMatch(final Function<StorageAdapter, VectorCursor> call)
    {
      final RowSignature signature = frameAdapter.getRowSignature();
      final Sequence<List<Object>> queryableRows =
          FrameTestUtil.readRowsFromVectorCursor(call.apply(queryableAdapter), signature);
      final Sequence<List<Object>> frameRows =
          FrameTestUtil.readRowsFromVectorCursor(advanceAndReset(call.apply(frameAdapter)), signature);
      FrameTestUtil.assertRowsEqual(queryableRows, frameRows);
    }

    /**
     * Advance and reset a Cursor. Helps test that reset() works properly.
     */
    private static Cursor advanceAndReset(final Cursor cursor)
    {
      for (int i = 0; i < 3 && !cursor.isDone(); i++) {
        cursor.advance();
      }

      cursor.reset();
      return cursor;
    }

    /**
     * Advance and reset a VectorCursor. Helps test that reset() works properly.
     */
    private static VectorCursor advanceAndReset(final VectorCursor cursor)
    {
      for (int i = 0; i < 3 && !cursor.isDone(); i++) {
        cursor.advance();
      }

      cursor.reset();
      return cursor;
    }
  }
}
