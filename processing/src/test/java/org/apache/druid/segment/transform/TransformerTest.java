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

package org.apache.druid.segment.transform;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransformerTest extends InitializedNullHandlingTest
{

  @Test
  public void testTransformNullRowReturnNull()
  {
    final Transformer transformer = new Transformer(new TransformSpec(null, null));
    Assertions.assertNull(transformer.transform((InputRow) null));
    Assertions.assertNull(transformer.transform((InputRowListPlusRawValues) null));
  }

  @Test
  public void testTransformTimeColumn()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(
                new ExpressionTransform("__time", "timestamp_shift(__time, 'P1D', -2)", TestExprMacroTable.INSTANCE)
            )
        )
    );
    final DateTime now = DateTimes.nowUtc();
    final InputRow row = new MapBasedInputRow(
        now,
        ImmutableList.of("dim"),
        ImmutableMap.of("__time", now, "dim", false)
    );
    final InputRow actual = transformer.transform(row);
    Assertions.assertNotNull(actual);
    Assertions.assertEquals(now.minusDays(2), actual.getTimestamp());
  }

  @Test
  public void testTransformTimeColumnWithInvalidTimeValue()
  {
    Throwable exception = assertThrows(ParseException.class, () -> {
      final Transformer transformer = new Transformer(
          new TransformSpec(
              null,
              ImmutableList.of(
                  new ExpressionTransform("__time", "timestamp_parse(ts, null, 'UTC')", TestExprMacroTable.INSTANCE)
              )
          )
      );
      final DateTime now = DateTimes.nowUtc();
      final InputRow row = new MapBasedInputRow(
          now,
          ImmutableList.of("ts", "dim"),
          ImmutableMap.of("ts", "not_a_timestamp", "dim", false)
      );
      if (NullHandling.replaceWithDefault()) {
        final InputRow actual = transformer.transform(row);
        Assertions.assertNotNull(actual);
        Assertions.assertEquals(DateTimes.of("1970-01-01T00:00:00.000Z"), actual.getTimestamp());
      } else {
        expectedException.expectMessage("Could not transform value for __time.");
        expectedException.expect(ParseException.class);
        transformer.transform(row);
      }
    });
    assertTrue(exception.getMessage().contains("Could not transform value for __time."));
  }

  @Test
  public void testTransformTimeColumnWithInvalidTimeValueInputRowListPlusRawValues()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(
                new ExpressionTransform("__time", "timestamp_parse(ts, null, 'UTC')", TestExprMacroTable.INSTANCE)
            )
        )
    );
    final DateTime now = DateTimes.nowUtc();
    final InputRow row = new MapBasedInputRow(
        now,
        ImmutableList.of("ts", "dim"),
        ImmutableMap.of("ts", "not_a_timestamp", "dim", false)
    );
    final InputRowListPlusRawValues actual = transformer.transform(
        InputRowListPlusRawValues.of(
            row,
            ImmutableMap.of("ts", "not_a_timestamp", "dim", false)
        )
    );
    Assertions.assertNotNull(actual);
    Assertions.assertEquals(1, actual.getRawValuesList().size());
    if (NullHandling.replaceWithDefault()) {
      Assertions.assertEquals(1, actual.getInputRows().size());
      Assertions.assertEquals(DateTimes.of("1970-01-01T00:00:00.000Z"), actual.getInputRows().get(0).getTimestamp());
    } else {
      Assertions.assertNull(actual.getInputRows());
      Assertions.assertEquals("Could not transform value for __time.", actual.getParseException().getMessage());
    }
  }

  @Test
  public void testTransformWithStringTransformOnBooleanColumnTransformAfterCasting()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(new ExpressionTransform("dim", "strlen(dim)", TestExprMacroTable.INSTANCE))
        )
    );
    final InputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", false)
    );
    final InputRow actual = transformer.transform(row);
    Assertions.assertNotNull(actual);
    Assertions.assertEquals(ImmutableList.of("dim"), actual.getDimensions());
    // booleans are longs by default, so strlen of false (0L) is 1
    Assertions.assertEquals(1L, actual.getRaw("dim"));
    Assertions.assertEquals(row.getTimestamp(), actual.getTimestamp());
  }

  @Test
  public void testTransformWithStringTransformOnLongColumnTransformAfterCasting()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(new ExpressionTransform("dim", "strlen(dim)", TestExprMacroTable.INSTANCE))
        )
    );
    final InputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", 10L)
    );
    final InputRow actual = transformer.transform(row);
    Assertions.assertNotNull(actual);
    Assertions.assertEquals(ImmutableList.of("dim"), actual.getDimensions());
    Assertions.assertEquals(2L, actual.getRaw("dim"));
    Assertions.assertEquals(row.getTimestamp(), actual.getTimestamp());
  }

  @Test
  public void testTransformWithStringTransformOnDoubleColumnTransformAfterCasting()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(new ExpressionTransform("dim", "strlen(dim)", TestExprMacroTable.INSTANCE))
        )
    );
    final InputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", 200.5d)
    );
    final InputRow actual = transformer.transform(row);
    Assertions.assertNotNull(actual);
    Assertions.assertEquals(ImmutableList.of("dim"), actual.getDimensions());
    Assertions.assertEquals(5L, actual.getRaw("dim"));
    Assertions.assertEquals(row.getTimestamp(), actual.getTimestamp());
  }

  @Disabled("Disabled until https://github.com/apache/druid/issues/9824 is fixed")
  @Test
  public void testTransformWithStringTransformOnListColumnThrowingException()
  {
    assertThrows(AssertionError.class, () -> {
      final Transformer transformer = new Transformer(
          new TransformSpec(
              null,
              ImmutableList.of(new ExpressionTransform("dim", "strlen(dim)", TestExprMacroTable.INSTANCE))
          )
      );
      final InputRow row = new MapBasedInputRow(
          DateTimes.nowUtc(),
          ImmutableList.of("dim"),
          ImmutableMap.of("dim", ImmutableList.of(10, 20, 100))
      );
      final InputRow actual = transformer.transform(row);
      Assertions.assertNotNull(actual);
      Assertions.assertEquals(ImmutableList.of("dim"), actual.getDimensions());
      actual.getRaw("dim");
    });
  }

  @Test
  public void testTransformWithSelectorFilterWithStringBooleanValueOnBooleanColumnFilterAfterCasting()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(new SelectorDimFilter("dim", "false", null), null)
    );
    final InputRow row1 = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", false)
    );
    Assertions.assertEquals(row1, transformer.transform(row1));
    final InputRow row2 = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", true)
    );
    Assertions.assertNull(transformer.transform(row2));
  }

  @Test
  public void testTransformWithSelectorFilterWithStringBooleanValueOnStringColumn()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(new SelectorDimFilter("dim", "false", null), null)
    );
    final InputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", "false")
    );
    Assertions.assertEquals(row, transformer.transform(row));
    final InputRow row2 = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", "true")
    );
    Assertions.assertNull(transformer.transform(row2));
  }

  @Test
  public void testTransformWithTransformAndFilterTransformFirst()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            new SelectorDimFilter("dim", "0", null),
            // A boolean expression returns a long.
            ImmutableList.of(new ExpressionTransform("dim", "strlen(dim) == 10", TestExprMacroTable.INSTANCE))
        )
    );
    final InputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", "short")
    );
    final InputRow actual = transformer.transform(row);
    Assertions.assertNotNull(actual);
    Assertions.assertEquals(ImmutableList.of("dim"), actual.getDimensions());
    Assertions.assertEquals(0L, actual.getRaw("dim"));
    Assertions.assertEquals(row.getTimestamp(), actual.getTimestamp());
  }

  @Test
  public void testInputRowListPlusRawValuesTransformWithFilter()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            new SelectorDimFilter("dim", "val1", null),
            null
            )
    );
    List<InputRow> rows = Arrays.asList(
        new MapBasedInputRow(
            DateTimes.nowUtc(),
            ImmutableList.of("dim"),
            ImmutableMap.of("dim", "val1")
        ),

        //this row will be filtered
        new MapBasedInputRow(
            DateTimes.nowUtc(),
            ImmutableList.of("dim"),
            ImmutableMap.of("dim", "val2")
        )
    );
    List<Map<String, Object>> valList = Arrays.asList(
        ImmutableMap.of("dim", "val1"),
        ImmutableMap.of("dim", "val2")
    );

    final InputRowListPlusRawValues actual = transformer.transform(InputRowListPlusRawValues.ofList(valList, rows));
    Assertions.assertNotNull(actual);
    Assertions.assertEquals(1, actual.getInputRows().size());
    Assertions.assertEquals(1, actual.getRawValuesList().size());
    Assertions.assertEquals("val1", actual.getInputRows().get(0).getRaw("dim"));
    Assertions.assertEquals("val1", actual.getRawValuesList().get(0).get("dim"));
  }

  @Test
  public void testTransformWithArrayStringInputsExpr()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(new ExpressionTransform("dimlen", "array_length(dim)", TestExprMacroTable.INSTANCE))
        )
    );
    final InputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", ImmutableList.of("a", "b", "c"))
    );
    final InputRow actual = transformer.transform(row);
    Assertions.assertNotNull(actual);
    Assertions.assertEquals(ImmutableList.of("dim"), actual.getDimensions());
    Assertions.assertEquals(3L, actual.getRaw("dimlen"));
    Assertions.assertEquals(ImmutableList.of("3"), actual.getDimension("dimlen"));
    Assertions.assertEquals(row.getTimestamp(), actual.getTimestamp());
  }

  @Test
  public void testTransformWithArrayStringInputs()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(new ExpressionTransform("dim", "dim", TestExprMacroTable.INSTANCE))
        )
    );
    final InputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", ImmutableList.of("a", "b", "c"))
    );
    final InputRow actual = transformer.transform(row);
    Assertions.assertNotNull(actual);
    Assertions.assertEquals(ImmutableList.of("dim"), actual.getDimensions());
    Assertions.assertArrayEquals(new Object[]{"a", "b", "c"}, (Object[]) actual.getRaw("dim"));
    Assertions.assertEquals(ImmutableList.of("a", "b", "c"), actual.getDimension("dim"));
    Assertions.assertEquals(row.getTimestamp(), actual.getTimestamp());
  }

  @Test
  public void testTransformWithArrayLongInputs()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(new ExpressionTransform("dim", "dim", TestExprMacroTable.INSTANCE))
        )
    );
    final InputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", Arrays.asList(1, 2, null, 3))
    );
    final InputRow actual = transformer.transform(row);
    Assertions.assertNotNull(actual);
    Assertions.assertEquals(ImmutableList.of("dim"), actual.getDimensions());
    Assertions.assertArrayEquals(new Object[]{1L, 2L, null, 3L}, (Object[]) actual.getRaw("dim"));
    Assertions.assertEquals(ImmutableList.of("1", "2", "null", "3"), actual.getDimension("dim"));
    Assertions.assertEquals(row.getTimestamp(), actual.getTimestamp());
  }

  @Test
  public void testTransformWithArrayFloatInputs()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(new ExpressionTransform("dim", "dim", TestExprMacroTable.INSTANCE))
        )
    );
    final InputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", Arrays.asList(1.2f, 2.3f, null, 3.4f))
    );
    final InputRow actual = transformer.transform(row);
    Assertions.assertNotNull(actual);
    Assertions.assertEquals(ImmutableList.of("dim"), actual.getDimensions());
    Object[] raw = (Object[]) actual.getRaw("dim");
    // floats are converted to doubles since expressions have no doubles
    Assertions.assertEquals(1.2, (Double) raw[0], 0.00001);
    Assertions.assertEquals(2.3, (Double) raw[1], 0.00001);
    Assertions.assertNull(raw[2]);
    Assertions.assertEquals(3.4, (Double) raw[3], 0.00001);
    Assertions.assertEquals(
        ImmutableList.of("1.2000000476837158", "2.299999952316284", "null", "3.4000000953674316"),
        actual.getDimension("dim")
    );
    Assertions.assertEquals(row.getTimestamp(), actual.getTimestamp());
  }

  @Test
  public void testTransformWithArrayDoubleInputs()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(new ExpressionTransform("dim", "dim", TestExprMacroTable.INSTANCE))
        )
    );
    final InputRow row = new MapBasedInputRow(
        DateTimes.nowUtc(),
        ImmutableList.of("dim"),
        ImmutableMap.of("dim", Arrays.asList(1.2, 2.3, null, 3.4))
    );
    final InputRow actual = transformer.transform(row);
    Assertions.assertNotNull(actual);
    Assertions.assertEquals(ImmutableList.of("dim"), actual.getDimensions());
    Object[] raw = (Object[]) actual.getRaw("dim");
    Assertions.assertEquals(1.2, (Double) raw[0], 0.0);
    Assertions.assertEquals(2.3, (Double) raw[1], 0.0);
    Assertions.assertNull(raw[2]);
    Assertions.assertEquals(3.4, (Double) raw[3], 0.0);
    Assertions.assertEquals(ImmutableList.of("1.2", "2.3", "null", "3.4"), actual.getDimension("dim"));
    Assertions.assertEquals(row.getTimestamp(), actual.getTimestamp());
  }

  @Test
  public void testTransformWithArrayExpr()
  {
    final Transformer transformer = new Transformer(
        new TransformSpec(
            null,
            ImmutableList.of(
                new ExpressionTransform("dim", "array_slice(dim, 0, 5)", TestExprMacroTable.INSTANCE),
                new ExpressionTransform("dim1", "array_slice(dim, 0, 1)", TestExprMacroTable.INSTANCE)
            )
        )
    );
    final List<String> dimList = ImmutableList.of("a", "b", "c", "d", "e", "f", "g");
    final MapBasedRow row = new MapBasedRow(
        DateTimes.nowUtc(),
        ImmutableMap.of("dim", dimList)
    );
    Assertions.assertEquals(row.getDimension("dim"), dimList);
    Assertions.assertEquals(row.getRaw("dim"), dimList);

    final InputRow actualTranformedRow = transformer.transform(new InputRow()
    {
      @Override
      public List<String> getDimensions()
      {
        return new ArrayList<>(row.getEvent().keySet());
      }

      @Override
      public long getTimestampFromEpoch()
      {
        return 0;
      }

      @Override
      public DateTime getTimestamp()
      {
        return row.getTimestamp();
      }

      @Override
      public List<String> getDimension(String dimension)
      {
        return row.getDimension(dimension);
      }

      @Nullable
      @Override
      public Object getRaw(String dimension)
      {
        return row.getRaw(dimension);
      }

      @Nullable
      @Override
      public Number getMetric(String metric)
      {
        return row.getMetric(metric);
      }

      @Override
      public int compareTo(Row o)
      {
        return row.compareTo(o);
      }
    });
    Assertions.assertEquals(actualTranformedRow.getDimension("dim"), dimList.subList(0, 5));
    Assertions.assertArrayEquals(dimList.subList(0, 5).toArray(), (Object[]) actualTranformedRow.getRaw("dim"));
    Assertions.assertEquals(ImmutableList.of("a"), actualTranformedRow.getDimension("dim1"));
  }
}
