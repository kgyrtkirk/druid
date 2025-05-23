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

package org.apache.druid.query.rowsandcols.semantic;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.operator.OffsetLimit;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.rowsandcols.concrete.ColumnBasedFrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.ColumnBasedFrameRowsAndColumnsTest;
import org.apache.druid.segment.ArrayListSegment;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

@SuppressWarnings({"unchecked", "rawtypes", "ConstantConditions", "SingleStatementInBlock", "VariableNotUsedInsideIf"})
public class RowsAndColumnsDecoratorTest extends SemanticTestBase
{
  public RowsAndColumnsDecoratorTest(
      String name,
      Function<MapOfColumnsRowsAndColumns, RowsAndColumns> fn
  )
  {
    super(name, fn);
  }

  @Test
  public void testDecoration()
  {
    Object[][] vals = new Object[][]{
        {1L, "a", 123L, 0L},
        {2L, "a", 456L, 1L},
        {3L, "b", 789L, 2L},
        {4L, "b", 123L, 3L},
        {5L, "c", 456L, 4L},
        {6L, "c", 789L, 5L},
        {7L, "c", 123L, 6L},
        {8L, "d", 456L, 7L},
        {9L, "e", 789L, 8L},
        {10L, "f", 123L, 9L},
        {11L, "f", 456L, 10L},
        {12L, "g", 789L, 11L},
        };

    RowSignature siggy = RowSignature.builder()
                                     .add("__time", ColumnType.LONG)
                                     .add("dim", ColumnType.STRING)
                                     .add("val", ColumnType.LONG)
                                     .add("arrayIndex", ColumnType.LONG)
                                     .build();

    final RowsAndColumns base = make(MapOfColumnsRowsAndColumns.fromRowObjects(vals, siggy));

    Interval[] intervals = new Interval[]{Intervals.utc(0, 6), Intervals.utc(6, 13), Intervals.utc(4, 8)};
    Filter[] filters = new Filter[]{
        new InDimFilter("dim", ImmutableSet.of("a", "b", "c", "e", "g")),
        new AndFilter(Arrays.asList(
            new InDimFilter("dim", ImmutableSet.of("a", "b", "g")),
            new SelectorFilter("val", "789")
        )),
        new OrFilter(Arrays.asList(
            new SelectorFilter("dim", "b"),
            new SelectorFilter("val", "789")
        )),
        new SelectorFilter("dim", "f")
    };
    int[] limits = new int[]{3, 6, 100};
    List<ColumnWithDirection>[] orderings = new List[]{
        Arrays.asList(ColumnWithDirection.descending("__time"), ColumnWithDirection.ascending("dim")),
        Collections.singletonList(ColumnWithDirection.ascending("val"))
    };

    // call the same method multiple times

    for (int i = 0; i <= intervals.length; ++i) {
      Interval interval = (i == 0 ? null : intervals[i - 1]);
      for (int j = 0; j <= filters.length; ++j) {
        Filter filter = (j == 0 ? null : filters[j - 1]);
        for (int k = 0; k <= limits.length; ++k) {
          int limit = (k == 0 ? -1 : limits[k - 1]);
          for (int l = 0; l <= orderings.length; ++l) {
            validateDecorated(base, siggy, vals, interval, filter, OffsetLimit.limit(limit), l == 0 ? null : orderings[l - 1]);
          }
        }
      }
    }
  }

  @Test
  public void testDecorationWithListOfResultRows()
  {
    ArrayList<ResultRow> resultRowArrayList = new ArrayList<>();

    resultRowArrayList.add(ResultRow.of(1L, 1L, 123L, 0L));
    resultRowArrayList.add(ResultRow.of(2L, 2L, 456L, 1L));
    resultRowArrayList.add(ResultRow.of(3L, 3L, 789L, 2L));
    resultRowArrayList.add(ResultRow.of(4L, 4L, 123L, 3L));
    resultRowArrayList.add(ResultRow.of(5L, 5L, 456L, 4L));
    resultRowArrayList.add(ResultRow.of(6L, 6L, 789L, 5L));
    resultRowArrayList.add(ResultRow.of(7L, 7L, 123L, 6L));
    resultRowArrayList.add(ResultRow.of(8L, 8L, 456L, 7L));
    resultRowArrayList.add(ResultRow.of(9L, 9L, 789L, 8L));
    resultRowArrayList.add(ResultRow.of(10L, 10L, 123L, 9L));
    resultRowArrayList.add(ResultRow.of(11L, 11L, 456L, 10L));
    resultRowArrayList.add(ResultRow.of(12L, 12L, 789L, 11L));

    RowSignature siggy = RowSignature.builder()
                                     .add("__time", ColumnType.LONG)
                                     .add("dim", ColumnType.LONG)
                                     .add("val", ColumnType.LONG)
                                     .add("arrayIndex", ColumnType.LONG)
                                     .build();

    final RowsAndColumns base = make(MapOfColumnsRowsAndColumns.fromResultRow(resultRowArrayList, siggy));

    Object[][] vals = new Object[][]{
        {1L, 1L, 123L, 0L},
        {2L, 2L, 456L, 1L},
        {3L, 3L, 789L, 2L},
        {4L, 4L, 123L, 3L},
        {5L, 5L, 456L, 4L},
        {6L, 6L, 789L, 5L},
        {7L, 7L, 123L, 6L},
        {8L, 8L, 456L, 7L},
        {9L, 9L, 789L, 8L},
        {10L, 10L, 123L, 9L},
        {11L, 11L, 456L, 10L},
        {12L, 12L, 789L, 11L},
        };

    Interval[] intervals = new Interval[]{Intervals.utc(0, 6), Intervals.utc(6, 13), Intervals.utc(4, 8)};
    Filter[] filters = new Filter[]{
        new InDimFilter("dim", ImmutableSet.of("a", "b", "c", "e", "g")),
        new AndFilter(Arrays.asList(
            new InDimFilter("dim", ImmutableSet.of("a", "b", "g")),
            new SelectorFilter("val", "789")
        )),
        new OrFilter(Arrays.asList(
            new SelectorFilter("dim", "b"),
            new SelectorFilter("val", "789")
        )),
        new SelectorFilter("dim", "f")
    };
    int[] limits = new int[]{3, 6, 100};
    List<ColumnWithDirection>[] orderings = new List[]{
        Arrays.asList(ColumnWithDirection.descending("__time"), ColumnWithDirection.ascending("dim")),
        Collections.singletonList(ColumnWithDirection.ascending("val"))
    };

    // call the same method multiple times

    for (int i = 0; i <= intervals.length; ++i) {
      Interval interval = (i == 0 ? null : intervals[i - 1]);
      for (int j = 0; j < filters.length; ++j) {
        Filter filter = (j == 0 ? null : filters[j - 1]);
        for (int k = 0; k <= limits.length; ++k) {
          int limit = (k == 0 ? -1 : limits[k - 1]);
          for (int l = 0; l <= orderings.length; ++l) {
            validateDecorated(
                base,
                siggy,
                vals,
                interval,
                filter,
                OffsetLimit.limit(limit),
                l == 0 ? null : orderings[l - 1]
            );
          }
        }
      }
    }
  }

  @Test
  public void testDecoratorWithColumnBasedFrameRAC()
  {
    RowSignature siggy = RowSignature.builder()
                                     .add("colA", ColumnType.LONG)
                                     .add("colB", ColumnType.LONG)
                                     .build();

    Object[][] vals = new Object[][]{
        {1L, 4L},
        {2L, -4L},
        {3L, 3L},
        {4L, -3L},
        {5L, 4L},
        {6L, 82L},
        {7L, -90L},
        {8L, 4L},
        {9L, 0L},
        {10L, 0L}
        };

    MapOfColumnsRowsAndColumns input = MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of(
            "colA", new IntArrayColumn(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
            "colB", new IntArrayColumn(new int[]{4, -4, 3, -3, 4, 82, -90, 4, 0, 0})
        )
    );

    ColumnBasedFrameRowsAndColumns frc = ColumnBasedFrameRowsAndColumnsTest.buildFrame(input);

    validateDecorated(frc, siggy, vals, null, null, OffsetLimit.NONE, null);
  }

  private void validateDecorated(
      RowsAndColumns base,
      RowSignature siggy,
      Object[][] originalVals,
      Interval interval,
      Filter filter,
      OffsetLimit limit,
      List<ColumnWithDirection> ordering
  )
  {
    String msg = StringUtils.format(
        "interval[%s], filter[%s], limit[%s], ordering[%s]",
        interval,
        filter,
        limit,
        ordering
    );
    RowsAndColumnsDecorator decor = RowsAndColumnsDecorator.fromRAC(base);
    List<Object[]> vals;

    if (interval == null && filter == null) {
      vals = Arrays.asList(originalVals);
    } else {
      decor.limitTimeRange(interval);
      decor.addFilter(filter);

      final ArrayListSegment<Object[]> seggy = new ArrayListSegment<>(
          new ArrayList<>(Arrays.asList(originalVals)),
          columnName -> {
            int index = siggy.indexOf(columnName);
            return arr -> arr[index];
          },
          siggy
      );
      final CursorBuildSpec.CursorBuildSpecBuilder builder = CursorBuildSpec.builder()
                                                                            .setFilter(filter);
      if (interval != null) {
        builder.setInterval(interval);
      }
      try (final CursorHolder cursorHolder = seggy.as(CursorFactory.class).makeCursorHolder(builder.build())) {
        final Cursor cursor = cursorHolder.asCursor();

        vals = new ArrayList<>();
        final ColumnValueSelector idSupplier = cursor.getColumnSelectorFactory().makeColumnValueSelector("arrayIndex");
        while (!cursor.isDone()) {
          vals.add(originalVals[(int) idSupplier.getLong()]);
          cursor.advance();
        }
      }
    }

    if (ordering != null) {
      decor.setOrdering(ordering);

      Comparator<Object[]> comparator = null;
      for (ColumnWithDirection order : ordering) {
        final int columnNum = siggy.indexOf(order.getColumn());
        final TypeStrategy<Object> strategy =
            siggy.getColumnType(columnNum).orElseThrow(() -> new UOE("debug me")).getStrategy();

        final Comparator<Object[]> newComp = (lhs, rhs) ->
            strategy.compare(lhs[columnNum], rhs[columnNum]) * order.getDirection().getDirectionInt();

        if (comparator == null) {
          comparator = newComp;
        } else {
          comparator = comparator.thenComparing(newComp);
        }
      }

      vals = new ArrayList<>(vals);

      vals.sort(comparator);
    }

    if (limit.isPresent()) {
      decor.setOffsetLimit(limit);
      int size = vals.size();
      vals = vals.subList((int) limit.getFromIndex(size), (int) limit.getToIndex(vals.size()));
    }

    if (ordering != null) {
      Assert.assertThrows(msg, ISE.class, () -> decor.toRowsAndColumns().numRows());
    } else {
      final RowsAndColumns rac = decor.toRowsAndColumns();
      Assert.assertEquals(msg, vals.size(), rac.numRows());

      ColumnAccessor[] accessors = new ColumnAccessor[siggy.size()];
      for (int i = 0; i < siggy.size(); ++i) {
        accessors[i] = rac.findColumn(siggy.getColumnName(i)).toAccessor();
      }

      for (int i = 0; i < vals.size(); ++i) {
        Object[] actuals = new Object[accessors.length];
        for (int j = 0; j < actuals.length; ++j) {
          actuals[j] = accessors[j].getObject(i);
          if (actuals[j] instanceof ByteBuffer) {
            actuals[j] = StringUtils.fromUtf8(((ByteBuffer) actuals[j]).asReadOnlyBuffer());
          }
        }
        Assert.assertArrayEquals(StringUtils.format("%s, row[%,d]", msg, i), vals.get(i), actuals);
      }
    }
  }
}
