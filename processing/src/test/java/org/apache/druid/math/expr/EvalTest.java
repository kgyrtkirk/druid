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

package org.apache.druid.math.expr;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import junitparams.converters.Nullable;
import org.apache.druid.collections.SerializablePair;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.column.TypeStrategiesTest;
import org.apache.druid.segment.data.ComparableList;
import org.apache.druid.segment.data.ComparableStringArray;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 */
public class EvalTest extends InitializedNullHandlingTest
{

  @BeforeAll
  public static void setupClass()
  {
    TypeStrategies.registerComplex(
        TypeStrategiesTest.NULLABLE_TEST_PAIR_TYPE.getComplexTypeName(),
        new TypeStrategiesTest.NullableLongPairTypeStrategy()
    );
  }

  private long evalLong(String x, Expr.ObjectBinding bindings)
  {
    ExprEval ret = eval(x, bindings);
    assertEquals(ExpressionType.LONG, ret.type());
    return ret.asLong();
  }

  private double evalDouble(String x, Expr.ObjectBinding bindings)
  {
    ExprEval ret = eval(x, bindings);
    assertEquals(ExpressionType.DOUBLE, ret.type());
    return ret.asDouble();
  }

  private ExprEval eval(String x, Expr.ObjectBinding bindings)
  {
    return Parser.parse(x, ExprMacroTable.nil()).eval(bindings);
  }

  @Test
  public void testDoubleEval()
  {
    Expr.ObjectBinding bindings = InputBindings.forMap(ImmutableMap.of("x", 2.0d));
    assertEquals(2.0, evalDouble("x", bindings), 0.0001);
    assertEquals(2.0, evalDouble("\"x\"", bindings), 0.0001);
    assertEquals(304.0, evalDouble("300 + \"x\" * 2", bindings), 0.0001);

    try {
      ExpressionProcessing.initializeForStrictBooleansTests(false);
      Assertions.assertFalse(evalDouble("1.0 && 0.0", bindings) > 0.0);
      Assertions.assertTrue(evalDouble("1.0 && 2.0", bindings) > 0.0);

      Assertions.assertTrue(evalDouble("1.0 || 0.0", bindings) > 0.0);
      Assertions.assertFalse(evalDouble("0.0 || 0.0", bindings) > 0.0);

      Assertions.assertTrue(evalDouble("2.0 > 1.0", bindings) > 0.0);
      Assertions.assertTrue(evalDouble("2.0 >= 2.0", bindings) > 0.0);
      Assertions.assertTrue(evalDouble("1.0 < 2.0", bindings) > 0.0);
      Assertions.assertTrue(evalDouble("2.0 <= 2.0", bindings) > 0.0);
      Assertions.assertTrue(evalDouble("2.0 == 2.0", bindings) > 0.0);
      Assertions.assertTrue(evalDouble("2.0 != 1.0", bindings) > 0.0);

      Assertions.assertEquals(1L, evalLong("notdistinctfrom(2.0, 2.0)", bindings));
      Assertions.assertEquals(1L, evalLong("isdistinctfrom(2.0, 1.0)", bindings));
      Assertions.assertEquals(0L, evalLong("notdistinctfrom(2.0, 1.0)", bindings));
      Assertions.assertEquals(0L, evalLong("isdistinctfrom(2.0, 2.0)", bindings));

      Assertions.assertEquals(0L, evalLong("istrue(0.0)", bindings));
      Assertions.assertEquals(1L, evalLong("isfalse(0.0)", bindings));
      Assertions.assertEquals(1L, evalLong("nottrue(0.0)", bindings));
      Assertions.assertEquals(0L, evalLong("notfalse(0.0)", bindings));

      Assertions.assertEquals(1L, evalLong("istrue(1.0)", bindings));
      Assertions.assertEquals(0L, evalLong("isfalse(1.0)", bindings));
      Assertions.assertEquals(0L, evalLong("nottrue(1.0)", bindings));
      Assertions.assertEquals(1L, evalLong("notfalse(1.0)", bindings));

      Assertions.assertTrue(evalDouble("!-1.0", bindings) > 0.0);
      Assertions.assertTrue(evalDouble("!0.0", bindings) > 0.0);
      Assertions.assertFalse(evalDouble("!2.0", bindings) > 0.0);
    }
    finally {
      ExpressionProcessing.initializeForTests();
    }
    try {
      ExpressionProcessing.initializeForStrictBooleansTests(true);
      Assertions.assertEquals(0L, evalLong("1.0 && 0.0", bindings));
      Assertions.assertEquals(1L, evalLong("1.0 && 2.0", bindings));

      Assertions.assertEquals(1L, evalLong("1.0 || 0.0", bindings));
      Assertions.assertEquals(0L, evalLong("0.0 || 0.0", bindings));

      Assertions.assertEquals(1L, evalLong("2.0 > 1.0", bindings));
      Assertions.assertEquals(1L, evalLong("2.0 >= 2.0", bindings));
      Assertions.assertEquals(1L, evalLong("1.0 < 2.0", bindings));
      Assertions.assertEquals(1L, evalLong("2.0 <= 2.0", bindings));
      Assertions.assertEquals(1L, evalLong("2.0 == 2.0", bindings));
      Assertions.assertEquals(1L, evalLong("2.0 != 1.0", bindings));

      Assertions.assertEquals(1L, evalLong("notdistinctfrom(2.0, 2.0)", bindings));
      Assertions.assertEquals(1L, evalLong("isdistinctfrom(2.0, 1.0)", bindings));
      Assertions.assertEquals(0L, evalLong("notdistinctfrom(2.0, 1.0)", bindings));
      Assertions.assertEquals(0L, evalLong("isdistinctfrom(2.0, 2.0)", bindings));

      Assertions.assertEquals(0L, evalLong("istrue(0.0)", bindings));
      Assertions.assertEquals(1L, evalLong("isfalse(0.0)", bindings));
      Assertions.assertEquals(1L, evalLong("nottrue(0.0)", bindings));
      Assertions.assertEquals(0L, evalLong("notfalse(0.0)", bindings));

      Assertions.assertEquals(1L, evalLong("istrue(1.0)", bindings));
      Assertions.assertEquals(0L, evalLong("isfalse(1.0)", bindings));
      Assertions.assertEquals(0L, evalLong("nottrue(1.0)", bindings));
      Assertions.assertEquals(1L, evalLong("notfalse(1.0)", bindings));

      Assertions.assertEquals(1L, evalLong("!-1.0", bindings));
      Assertions.assertEquals(1L, evalLong("!0.0", bindings));
      Assertions.assertEquals(0L, evalLong("!2.0", bindings));

      assertEquals(3.5, evalDouble("2.0 + 1.5", bindings), 0.0001);
      assertEquals(0.5, evalDouble("2.0 - 1.5", bindings), 0.0001);
      assertEquals(3.0, evalDouble("2.0 * 1.5", bindings), 0.0001);
      assertEquals(4.0, evalDouble("2.0 / 0.5", bindings), 0.0001);
      assertEquals(0.2, evalDouble("2.0 % 0.3", bindings), 0.0001);
      assertEquals(8.0, evalDouble("2.0 ^ 3.0", bindings), 0.0001);
      assertEquals(-1.5, evalDouble("-1.5", bindings), 0.0001);


      assertEquals(2.0, evalDouble("sqrt(4.0)", bindings), 0.0001);
      assertEquals(2.0, evalDouble("if(1.0, 2.0, 3.0)", bindings), 0.0001);
      assertEquals(3.0, evalDouble("if(0.0, 2.0, 3.0)", bindings), 0.0001);
    }
    finally {
      ExpressionProcessing.initializeForTests();
    }
  }

  @Test
  public void testLongEval()
  {
    Expr.ObjectBinding bindings = InputBindings.forMap(ImmutableMap.of("x", 9223372036854775807L));

    assertEquals(9223372036854775807L, evalLong("x", bindings));
    assertEquals(9223372036854775807L, evalLong("\"x\"", bindings));
    assertEquals(92233720368547759L, evalLong("\"x\" / 100 + 1", bindings));

    Assertions.assertFalse(evalLong("9223372036854775807 && 0", bindings) > 0);
    Assertions.assertTrue(evalLong("9223372036854775807 && 9223372036854775806", bindings) > 0);

    Assertions.assertTrue(evalLong("9223372036854775807 || 0", bindings) > 0);
    Assertions.assertFalse(evalLong("-9223372036854775807 || -9223372036854775807", bindings) > 0);
    Assertions.assertTrue(evalLong("-9223372036854775807 || 9223372036854775807", bindings) > 0);
    Assertions.assertFalse(evalLong("0 || 0", bindings) > 0);

    Assertions.assertTrue(evalLong("9223372036854775807 > 9223372036854775806", bindings) > 0);
    Assertions.assertTrue(evalLong("9223372036854775807 >= 9223372036854775807", bindings) > 0);
    Assertions.assertTrue(evalLong("9223372036854775806 < 9223372036854775807", bindings) > 0);
    Assertions.assertTrue(evalLong("9223372036854775807 <= 9223372036854775807", bindings) > 0);
    Assertions.assertTrue(evalLong("9223372036854775807 == 9223372036854775807", bindings) > 0);
    Assertions.assertTrue(evalLong("9223372036854775807 != 9223372036854775806", bindings) > 0);
    Assertions.assertTrue(evalLong("notdistinctfrom(9223372036854775807, 9223372036854775807)", bindings) > 0);
    Assertions.assertTrue(evalLong("isdistinctfrom(9223372036854775807, 9223372036854775806)", bindings) > 0);

    assertEquals(9223372036854775807L, evalLong("9223372036854775806 + 1", bindings));
    assertEquals(9223372036854775806L, evalLong("9223372036854775807 - 1", bindings));
    assertEquals(9223372036854775806L, evalLong("4611686018427387903 * 2", bindings));
    assertEquals(4611686018427387903L, evalLong("9223372036854775806 / 2", bindings));
    assertEquals(7L, evalLong("9223372036854775807 % 9223372036854775800", bindings));
    assertEquals(9223372030926249001L, evalLong("3037000499 ^ 2", bindings));
    assertEquals(-9223372036854775807L, evalLong("-9223372036854775807", bindings));

    Assertions.assertTrue(evalLong("!-9223372036854775807", bindings) > 0);
    Assertions.assertTrue(evalLong("!0", bindings) > 0);
    Assertions.assertFalse(evalLong("!9223372036854775807", bindings) > 0);

    assertEquals(3037000499L, evalLong("cast(sqrt(9223372036854775807), 'long')", bindings));
    assertEquals(1L, evalLong("if(x == 9223372036854775807, 1, 0)", bindings));
    assertEquals(0L, evalLong("if(x - 1 == 9223372036854775807, 1, 0)", bindings));

    assertEquals(1271030400000L, evalLong("timestamp('2010-04-12')", bindings));
    assertEquals(1270998000000L, evalLong("timestamp('2010-04-12T+09:00')", bindings));
    assertEquals(1271055781000L, evalLong("timestamp('2010-04-12T07:03:01')", bindings));
    assertEquals(1271023381000L, evalLong("timestamp('2010-04-12T07:03:01+09:00')", bindings));
    assertEquals(1271023381419L, evalLong("timestamp('2010-04-12T07:03:01.419+09:00')", bindings));

    assertEquals(1271030400L, evalLong("unix_timestamp('2010-04-12')", bindings));
    assertEquals(1270998000L, evalLong("unix_timestamp('2010-04-12T+09:00')", bindings));
    assertEquals(1271055781L, evalLong("unix_timestamp('2010-04-12T07:03:01')", bindings));
    assertEquals(1271023381L, evalLong("unix_timestamp('2010-04-12T07:03:01+09:00')", bindings));
    assertEquals(1271023381L, evalLong("unix_timestamp('2010-04-12T07:03:01.419+09:00')", bindings));
    assertEquals(
        NullHandling.replaceWithDefault() ? "NULL" : "",
        eval("nvl(if(x == 9223372036854775807, '', 'x'), 'NULL')", bindings).asString()
    );
    assertEquals("x", eval("nvl(if(x == 9223372036854775806, '', 'x'), 'NULL')", bindings).asString());
  }

  @Test
  public void testIsNotDistinctFrom()
  {
    assertEquals(
        1L,
        new Function.IsNotDistinctFromFunc()
            .apply(
                ImmutableList.of(
                    new NullLongExpr(),
                    new NullLongExpr()
                ),
                InputBindings.nilBindings()
            )
            .value()
    );

    assertEquals(
        0L,
        new Function.IsNotDistinctFromFunc()
            .apply(
                ImmutableList.of(
                    new LongExpr(0L),
                    new NullLongExpr()
                ),
                InputBindings.nilBindings()
            )
            .value()
    );

    assertEquals(
        1L,
        new Function.IsNotDistinctFromFunc()
            .apply(
                ImmutableList.of(
                    new LongExpr(0L),
                    new LongExpr(0L)
                ),
                InputBindings.nilBindings()
            )
            .value()
    );
  }

  @Test
  public void testIsDistinctFrom()
  {
    assertEquals(
        0L,
        new Function.IsDistinctFromFunc()
            .apply(
                ImmutableList.of(
                    new NullLongExpr(),
                    new NullLongExpr()
                ),
                InputBindings.nilBindings()
            )
            .value()
    );

    assertEquals(
        1L,
        new Function.IsDistinctFromFunc()
            .apply(
                ImmutableList.of(
                    new LongExpr(0L),
                    new NullLongExpr()
                ),
                InputBindings.nilBindings()
            )
            .value()
    );

    assertEquals(
        0L,
        new Function.IsDistinctFromFunc()
            .apply(
                ImmutableList.of(
                    new LongExpr(0L),
                    new LongExpr(0L)
                ),
                InputBindings.nilBindings()
            )
            .value()
    );
  }

  @Test
  public void testIsFalse()
  {
    assertEquals(
        0L,
        new Function.IsFalseFunc()
            .apply(ImmutableList.of(new NullLongExpr()), InputBindings.nilBindings())
            .value()
    );

    assertEquals(
        1L,
        new Function.IsFalseFunc()
            .apply(ImmutableList.of(new LongExpr(0L)), InputBindings.nilBindings())
            .value()
    );

    assertEquals(
        0L,
        new Function.IsFalseFunc()
            .apply(ImmutableList.of(new LongExpr(1L)), InputBindings.nilBindings())
            .value()
    );
  }

  @Test
  public void testIsTrue()
  {
    assertEquals(
        0L,
        new Function.IsTrueFunc()
            .apply(ImmutableList.of(new NullLongExpr()), InputBindings.nilBindings())
            .value()
    );

    assertEquals(
        0L,
        new Function.IsTrueFunc()
            .apply(ImmutableList.of(new LongExpr(0L)), InputBindings.nilBindings())
            .value()
    );

    assertEquals(
        1L,
        new Function.IsTrueFunc()
            .apply(ImmutableList.of(new LongExpr(1L)), InputBindings.nilBindings())
            .value()
    );
  }

  @Test
  public void testIsNotFalse()
  {
    assertEquals(
        1L,
        new Function.IsNotFalseFunc()
            .apply(ImmutableList.of(new NullLongExpr()), InputBindings.nilBindings())
            .value()
    );

    assertEquals(
        0L,
        new Function.IsNotFalseFunc()
            .apply(ImmutableList.of(new LongExpr(0L)), InputBindings.nilBindings())
            .value()
    );

    assertEquals(
        1L,
        new Function.IsNotFalseFunc()
            .apply(ImmutableList.of(new LongExpr(1L)), InputBindings.nilBindings())
            .value()
    );
  }

  @Test
  public void testIsNotTrue()
  {
    assertEquals(
        1L,
        new Function.IsNotTrueFunc()
            .apply(ImmutableList.of(new NullLongExpr()), InputBindings.nilBindings())
            .value()
    );

    assertEquals(
        1L,
        new Function.IsNotTrueFunc()
            .apply(ImmutableList.of(new LongExpr(0L)), InputBindings.nilBindings())
            .value()
    );

    assertEquals(
        0L,
        new Function.IsNotTrueFunc()
            .apply(ImmutableList.of(new LongExpr(1L)), InputBindings.nilBindings())
            .value()
    );
  }

  @Test
  public void testArrayToScalar()
  {
    assertEquals(1L, ExprEval.ofLongArray(new Long[]{1L}).asLong());
    assertEquals(1.0, ExprEval.ofLongArray(new Long[]{1L}).asDouble(), 0.0);
    assertEquals(1, ExprEval.ofLongArray(new Long[]{1L}).asInt());
    assertEquals(true, ExprEval.ofLongArray(new Long[]{1L}).asBoolean());
    assertEquals("1", ExprEval.ofLongArray(new Long[]{1L}).asString());


    assertEquals(null, ExprEval.ofLongArray(new Long[]{null}).asString());

    assertEquals(0L, ExprEval.ofLongArray(new Long[]{1L, 2L}).asLong());
    assertEquals(0.0, ExprEval.ofLongArray(new Long[]{1L, 2L}).asDouble(), 0.0);
    assertEquals("[1, 2]", ExprEval.ofLongArray(new Long[]{1L, 2L}).asString());
    assertEquals(0, ExprEval.ofLongArray(new Long[]{1L, 2L}).asInt());
    assertEquals(false, ExprEval.ofLongArray(new Long[]{1L, 2L}).asBoolean());

    assertEquals(1.1, ExprEval.ofDoubleArray(new Double[]{1.1}).asDouble(), 0.0);
    assertEquals(1L, ExprEval.ofDoubleArray(new Double[]{1.1}).asLong());
    assertEquals("1.1", ExprEval.ofDoubleArray(new Double[]{1.1}).asString());
    assertEquals(1, ExprEval.ofDoubleArray(new Double[]{1.1}).asInt());
    assertEquals(true, ExprEval.ofDoubleArray(new Double[]{1.1}).asBoolean());

    assertEquals(null, ExprEval.ofDoubleArray(new Double[]{null}).asString());

    assertEquals(0.0, ExprEval.ofDoubleArray(new Double[]{1.1, 2.2}).asDouble(), 0.0);
    assertEquals(0L, ExprEval.ofDoubleArray(new Double[]{1.1, 2.2}).asLong());
    assertEquals("[1.1, 2.2]", ExprEval.ofDoubleArray(new Double[]{1.1, 2.2}).asString());
    assertEquals(0, ExprEval.ofDoubleArray(new Double[]{1.1, 2.2}).asInt());
    assertEquals(false, ExprEval.ofDoubleArray(new Double[]{1.1, 2.2}).asBoolean());

    assertEquals("foo", ExprEval.ofStringArray(new String[]{"foo"}).asString());

    assertEquals("1", ExprEval.ofStringArray(new String[]{"1"}).asString());
    assertEquals(1L, ExprEval.ofStringArray(new String[]{"1"}).asLong());
    assertEquals(1.0, ExprEval.ofStringArray(new String[]{"1"}).asDouble(), 0.0);
    assertEquals(1, ExprEval.ofStringArray(new String[]{"1"}).asInt());
    assertEquals(false, ExprEval.ofStringArray(new String[]{"1"}).asBoolean());
    assertEquals(true, ExprEval.ofStringArray(new String[]{"true"}).asBoolean());

    assertEquals("[1, 2.2]", ExprEval.ofStringArray(new String[]{"1", "2.2"}).asString());
    assertEquals(0L, ExprEval.ofStringArray(new String[]{"1", "2.2"}).asLong());
    assertEquals(0.0, ExprEval.ofStringArray(new String[]{"1", "2.2"}).asDouble(), 0.0);
    assertEquals(0, ExprEval.ofStringArray(new String[]{"1", "2.2"}).asInt());
    assertEquals(false, ExprEval.ofStringArray(new String[]{"1", "2.2"}).asBoolean());

    // test casting arrays to scalars
    assertEquals(1L, ExprEval.ofLongArray(new Long[]{1L}).castTo(ExpressionType.LONG).value());
    assertEquals(null, ExprEval.ofLongArray(new Long[]{null}).castTo(ExpressionType.LONG).value());
    assertEquals(1.0, ExprEval.ofLongArray(new Long[]{1L}).castTo(ExpressionType.DOUBLE).asDouble(), 0.0);
    assertEquals("1", ExprEval.ofLongArray(new Long[]{1L}).castTo(ExpressionType.STRING).value());

    assertEquals(1.1, ExprEval.ofDoubleArray(new Double[]{1.1}).castTo(ExpressionType.DOUBLE).asDouble(), 0.0);
    assertEquals(null, ExprEval.ofDoubleArray(new Double[]{null}).castTo(ExpressionType.DOUBLE).value());
    assertEquals(1L, ExprEval.ofDoubleArray(new Double[]{1.1}).castTo(ExpressionType.LONG).value());
    assertEquals("1.1", ExprEval.ofDoubleArray(new Double[]{1.1}).castTo(ExpressionType.STRING).value());

    assertEquals("foo", ExprEval.ofStringArray(new String[]{"foo"}).castTo(ExpressionType.STRING).value());
    assertEquals(null, ExprEval.ofStringArray(new String[]{"foo"}).castTo(ExpressionType.LONG).value());
    assertEquals(null, ExprEval.ofStringArray(new String[]{"foo"}).castTo(ExpressionType.DOUBLE).value());
    assertEquals("1", ExprEval.ofStringArray(new String[]{"1"}).castTo(ExpressionType.STRING).value());
    assertEquals(1L, ExprEval.ofStringArray(new String[]{"1"}).castTo(ExpressionType.LONG).value());
    assertEquals(1.0, ExprEval.ofStringArray(new String[]{"1"}).castTo(ExpressionType.DOUBLE).value());
  }

  @Test
  public void testStringArrayToScalarStringBadCast()
  {
    Throwable t = Assertions.assertThrows(
        IAE.class,
        () -> ExprEval.ofStringArray(new String[]{"foo", "bar"}).castTo(ExpressionType.STRING)
    );
    Assertions.assertEquals("Invalid type, cannot cast [ARRAY<STRING>] to [STRING]", t.getMessage());
  }

  @Test
  public void testStringArrayToScalarLongBadCast()
  {
    Throwable t = Assertions.assertThrows(
        IAE.class,
        () -> ExprEval.ofStringArray(new String[]{"foo", "bar"}).castTo(ExpressionType.LONG)
    );
    Assertions.assertEquals("Invalid type, cannot cast [ARRAY<STRING>] to [LONG]", t.getMessage());
  }

  @Test
  public void testStringArrayToScalarDoubleBadCast()
  {
    Throwable t = Assertions.assertThrows(
        IAE.class,
        () -> ExprEval.ofStringArray(new String[]{"foo", "bar"}).castTo(ExpressionType.DOUBLE)
    );
    Assertions.assertEquals("Invalid type, cannot cast [ARRAY<STRING>] to [DOUBLE]", t.getMessage());
  }

  @Test
  public void testLongArrayToScalarStringBadCast()
  {
    Throwable t = Assertions.assertThrows(
        IAE.class,
        () -> ExprEval.ofLongArray(new Long[]{1L, 2L}).castTo(ExpressionType.STRING)
    );
    Assertions.assertEquals("Invalid type, cannot cast [ARRAY<LONG>] to [STRING]", t.getMessage());
  }

  @Test
  public void testLongArrayToScalarLongBadCast()
  {
    Throwable t = Assertions.assertThrows(
        IAE.class,
        () -> ExprEval.ofLongArray(new Long[]{1L, 2L}).castTo(ExpressionType.LONG)
    );
    Assertions.assertEquals("Invalid type, cannot cast [ARRAY<LONG>] to [LONG]", t.getMessage());
  }

  @Test
  public void testLongArrayToScalarDoubleBadCast()
  {
    Throwable t = Assertions.assertThrows(
        IAE.class,
        () -> ExprEval.ofLongArray(new Long[]{1L, 2L}).castTo(ExpressionType.DOUBLE)
    );
    Assertions.assertEquals("Invalid type, cannot cast [ARRAY<LONG>] to [DOUBLE]", t.getMessage());
  }

  @Test
  public void testDoubleArrayToScalarStringBadCast()
  {
    Throwable t = Assertions.assertThrows(
        IAE.class,
        () -> ExprEval.ofDoubleArray(new Double[]{1.1, 2.2}).castTo(ExpressionType.STRING)
    );
    Assertions.assertEquals("Invalid type, cannot cast [ARRAY<DOUBLE>] to [STRING]", t.getMessage());
  }

  @Test
  public void testDoubleArrayToScalarLongBadCast()
  {
    Throwable t = Assertions.assertThrows(
        IAE.class,
        () -> ExprEval.ofDoubleArray(new Double[]{1.1, 2.2}).castTo(ExpressionType.LONG)
    );
    Assertions.assertEquals("Invalid type, cannot cast [ARRAY<DOUBLE>] to [LONG]", t.getMessage());
  }

  @Test
  public void testDoubleArrayToScalarDoubleBadCast()
  {
    Throwable t = Assertions.assertThrows(
        IAE.class,
        () -> ExprEval.ofDoubleArray(new Double[]{1.1, 2.2}).castTo(ExpressionType.DOUBLE)
    );
    Assertions.assertEquals("Invalid type, cannot cast [ARRAY<DOUBLE>] to [DOUBLE]", t.getMessage());
  }

  @Test
  public void testNestedDataCast()
  {
    ExprEval cast;
    cast = ExprEval.ofComplex(ExpressionType.NESTED_DATA, "hello").castTo(ExpressionType.STRING);
    Assertions.assertEquals("hello", cast.value());
    Assertions.assertEquals("hello", ExprEval.ofComplex(ExpressionType.NESTED_DATA, "hello").asString());
    Assertions.assertEquals(ExpressionType.STRING, cast.type());

    cast = ExprEval.of("hello").castTo(ExpressionType.NESTED_DATA);
    Assertions.assertEquals("hello", cast.value());
    Assertions.assertEquals(ExpressionType.NESTED_DATA, cast.type());

    cast = ExprEval.ofComplex(ExpressionType.NESTED_DATA, 123L).castTo(ExpressionType.STRING);
    Assertions.assertEquals("123", cast.value());
    Assertions.assertEquals(ExpressionType.STRING, cast.type());

    cast = ExprEval.ofComplex(ExpressionType.NESTED_DATA, 123L).castTo(ExpressionType.LONG);
    Assertions.assertEquals(123L, cast.value());
    Assertions.assertEquals(123L, ExprEval.ofComplex(ExpressionType.NESTED_DATA, 123L).asLong());
    Assertions.assertEquals(ExpressionType.LONG, cast.type());

    cast = ExprEval.of(123L).castTo(ExpressionType.NESTED_DATA);
    Assertions.assertEquals(123L, cast.value());
    Assertions.assertEquals(ExpressionType.NESTED_DATA, cast.type());

    cast = ExprEval.ofComplex(ExpressionType.NESTED_DATA, 123L).castTo(ExpressionType.DOUBLE);
    Assertions.assertEquals(123.0, cast.value());
    Assertions.assertEquals(123.0, ExprEval.ofComplex(ExpressionType.NESTED_DATA, 123L).asDouble(), 0.0);
    Assertions.assertEquals(ExpressionType.DOUBLE, cast.type());

    cast = ExprEval.of(12.3).castTo(ExpressionType.NESTED_DATA);
    Assertions.assertEquals(12.3, cast.value());
    Assertions.assertEquals(ExpressionType.NESTED_DATA, cast.type());

    cast = ExprEval.ofComplex(ExpressionType.NESTED_DATA, ImmutableList.of("a", "b", "c")).castTo(ExpressionType.STRING_ARRAY);
    Assertions.assertArrayEquals(new Object[]{"a", "b", "c"}, (Object[]) cast.value());
    Assertions.assertArrayEquals(
        new Object[]{"a", "b", "c"},
        ExprEval.ofComplex(ExpressionType.NESTED_DATA, ImmutableList.of("a", "b", "c")).asArray()
    );
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, cast.type());

    cast = ExprEval.ofArray(ExpressionType.STRING_ARRAY, new Object[]{"a", "b", "c"}).castTo(ExpressionType.NESTED_DATA);
    Assertions.assertArrayEquals(new Object[]{"a", "b", "c"}, (Object[]) cast.value());
    Assertions.assertEquals(ExpressionType.NESTED_DATA, cast.type());

    cast = ExprEval.ofComplex(ExpressionType.NESTED_DATA, ImmutableList.of(1L, 2L, 3L)).castTo(ExpressionType.LONG_ARRAY);
    Assertions.assertArrayEquals(new Object[]{1L, 2L, 3L}, (Object[]) cast.value());
    Assertions.assertArrayEquals(
        new Object[]{1L, 2L, 3L},
        ExprEval.ofComplex(ExpressionType.NESTED_DATA, ImmutableList.of(1L, 2L, 3L)).asArray()
    );
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, cast.type());

    cast = ExprEval.ofArray(ExpressionType.LONG_ARRAY, new Object[]{1L, 2L, 3L}).castTo(ExpressionType.NESTED_DATA);
    Assertions.assertArrayEquals(new Object[]{1L, 2L, 3L}, (Object[]) cast.value());
    Assertions.assertEquals(ExpressionType.NESTED_DATA, cast.type());

    cast = ExprEval.ofComplex(ExpressionType.NESTED_DATA, ImmutableList.of(1L, 2L, 3L)).castTo(ExpressionType.DOUBLE_ARRAY);
    Assertions.assertArrayEquals(new Object[]{1.0, 2.0, 3.0}, (Object[]) cast.value());
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, cast.type());

    cast = ExprEval.ofArray(ExpressionType.DOUBLE_ARRAY, new Object[]{1.1, 2.2, 3.3}).castTo(ExpressionType.NESTED_DATA);
    Assertions.assertArrayEquals(new Object[]{1.1, 2.2, 3.3}, (Object[]) cast.value());
    Assertions.assertEquals(ExpressionType.NESTED_DATA, cast.type());

    ExpressionType nestedArray = ExpressionTypeFactory.getInstance().ofArray(ExpressionType.NESTED_DATA);
    cast = ExprEval.ofComplex(
        ExpressionType.NESTED_DATA,
        ImmutableList.of(ImmutableMap.of("x", 1, "y", 2), ImmutableMap.of("x", 3, "y", 4))
    ).castTo(nestedArray);
    Assertions.assertArrayEquals(
        new Object[]{
            ImmutableMap.of("x", 1, "y", 2),
            ImmutableMap.of("x", 3, "y", 4)
        },
        (Object[]) cast.value()
    );
    Assertions.assertEquals(nestedArray, cast.type());
    Assertions.assertArrayEquals(
        new Object[]{
            ImmutableMap.of("x", 1, "y", 2),
            ImmutableMap.of("x", 3, "y", 4)
        },
        ExprEval.ofComplex(
            ExpressionType.NESTED_DATA,
            ImmutableList.of(
                ImmutableMap.of("x", 1, "y", 2),
                ImmutableMap.of("x", 3, "y", 4)
            )
        ).asArray()
    );

    cast = ExprEval.ofLong(1234L).castTo(nestedArray);
    Assertions.assertEquals(nestedArray, cast.type());
    Assertions.assertArrayEquals(
        new Object[]{1234L},
        cast.asArray()
    );
    cast = ExprEval.of("hello").castTo(nestedArray);
    Assertions.assertEquals(nestedArray, cast.type());
    Assertions.assertArrayEquals(
        new Object[]{"hello"},
        cast.asArray()
    );
    cast = ExprEval.ofDouble(1.234).castTo(nestedArray);
    Assertions.assertEquals(nestedArray, cast.type());
    Assertions.assertArrayEquals(
        new Object[]{1.234},
        cast.asArray()
    );
    cast = ExprEval.ofComplex(ExpressionType.NESTED_DATA, 1234L).castTo(nestedArray);
    Assertions.assertArrayEquals(
        new Object[]{1234L},
        cast.asArray()
    );
    Assertions.assertEquals(nestedArray, cast.type());
  }

  @Test
  public void testNestedAsOtherStuff()
  {
    ExprEval eval = ExprEval.ofComplex(ExpressionType.NESTED_DATA, StructuredData.wrap(true));
    Assertions.assertTrue(eval.asBoolean());
    Assertions.assertFalse(eval.isNumericNull());
    Assertions.assertEquals(1, eval.asInt());
    Assertions.assertEquals(1L, eval.asLong());
    Assertions.assertEquals(1.0, eval.asDouble(), 0.0);

    Assertions.assertTrue(ExprEval.ofComplex(ExpressionType.NESTED_DATA, true).asBoolean());
    eval = ExprEval.ofComplex(ExpressionType.NESTED_DATA, false);
    Assertions.assertFalse(eval.asBoolean());
    Assertions.assertFalse(eval.isNumericNull());
    Assertions.assertEquals(0, eval.asInt());
    Assertions.assertEquals(0L, eval.asLong());
    Assertions.assertEquals(0.0, eval.asDouble(), 0.0);

    eval = ExprEval.ofComplex(ExpressionType.NESTED_DATA, "true");
    Assertions.assertTrue(eval.asBoolean());
    Assertions.assertFalse(eval.isNumericNull());
    Assertions.assertEquals(1L, eval.asLong());
    Assertions.assertEquals(1, eval.asInt());
    Assertions.assertEquals(1.0, eval.asDouble(), 0.0);

    Assertions.assertTrue(ExprEval.ofComplex(ExpressionType.NESTED_DATA, StructuredData.wrap("true")).asBoolean());
    Assertions.assertTrue(ExprEval.ofComplex(ExpressionType.NESTED_DATA, "TRUE").asBoolean());
    Assertions.assertTrue(ExprEval.ofComplex(ExpressionType.NESTED_DATA, "True").asBoolean());

    eval = ExprEval.ofComplex(ExpressionType.NESTED_DATA, StructuredData.wrap(1L));
    Assertions.assertTrue(eval.asBoolean());
    Assertions.assertFalse(eval.isNumericNull());
    Assertions.assertEquals(1L, eval.asLong());
    Assertions.assertEquals(1, eval.asInt());
    Assertions.assertEquals(1.0, eval.asDouble(), 0.0);

    Assertions.assertTrue(ExprEval.ofComplex(ExpressionType.NESTED_DATA, 1L).asBoolean());

    eval = ExprEval.ofComplex(ExpressionType.NESTED_DATA, StructuredData.wrap(1.23));
    Assertions.assertTrue(eval.asBoolean());
    Assertions.assertFalse(eval.isNumericNull());
    Assertions.assertEquals(1L, eval.asLong());
    Assertions.assertEquals(1, eval.asInt());
    Assertions.assertEquals(1.23, eval.asDouble(), 0.0);

    eval = ExprEval.ofComplex(ExpressionType.NESTED_DATA, "hello");
    Assertions.assertFalse(eval.asBoolean());
    Assertions.assertTrue(eval.isNumericNull());
    Assertions.assertEquals(0, eval.asInt());
    Assertions.assertEquals(0L, eval.asLong());
    Assertions.assertEquals(0.0, eval.asDouble(), 0.0);

    eval = ExprEval.ofComplex(ExpressionType.NESTED_DATA, Arrays.asList("1", "2", "3"));
    Assertions.assertFalse(eval.asBoolean());
    Assertions.assertTrue(eval.isNumericNull());
    Assertions.assertEquals(0, eval.asInt());
    Assertions.assertEquals(0L, eval.asLong());
    Assertions.assertEquals(0.0, eval.asDouble(), 0.0);
    Assertions.assertArrayEquals(new Object[]{"1", "2", "3"}, eval.asArray());

    eval = ExprEval.ofComplex(ExpressionType.NESTED_DATA, Arrays.asList(1L, 2L, 3L));
    Assertions.assertFalse(eval.asBoolean());
    Assertions.assertTrue(eval.isNumericNull());
    Assertions.assertEquals(0, eval.asInt());
    Assertions.assertEquals(0L, eval.asLong());
    Assertions.assertEquals(0.0, eval.asDouble(), 0.0);
    Assertions.assertArrayEquals(new Object[]{1L, 2L, 3L}, eval.asArray());

    eval = ExprEval.ofComplex(ExpressionType.NESTED_DATA, Arrays.asList(1.1, 2.2, 3.3));
    Assertions.assertFalse(eval.asBoolean());
    Assertions.assertTrue(eval.isNumericNull());
    Assertions.assertEquals(0, eval.asInt());
    Assertions.assertEquals(0L, eval.asLong());
    Assertions.assertEquals(0.0, eval.asDouble(), 0.0);
    Assertions.assertArrayEquals(new Object[]{1.1, 2.2, 3.3}, eval.asArray());

    eval = ExprEval.ofComplex(
        ExpressionType.NESTED_DATA,
        ImmutableList.of(
            ImmutableMap.of("x", 1, "y", 2),
            ImmutableMap.of("x", 3, "y", 4)
        )
    );
    Assertions.assertFalse(eval.asBoolean());
    Assertions.assertEquals(0, eval.asLong());
    Assertions.assertEquals(0, eval.asInt());
    Assertions.assertEquals(0.0, eval.asDouble(), 0.0);
  }

  @Test
  public void testNonNestedComplexCastThrows()
  {
    ExpressionType someComplex = ExpressionTypeFactory.getInstance().ofComplex("tester");
    Throwable t = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ExprEval.ofType(someComplex, "hello").castTo(ExpressionType.STRING)
    );
    Assertions.assertEquals("Invalid type, cannot cast [COMPLEX<tester>] to [STRING]", t.getMessage());

    t = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ExprEval.ofType(someComplex, "hello").castTo(ExpressionType.LONG)
    );
    Assertions.assertEquals("Invalid type, cannot cast [COMPLEX<tester>] to [LONG]", t.getMessage());

    t = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ExprEval.ofType(someComplex, "hello").castTo(ExpressionType.DOUBLE)
    );
    Assertions.assertEquals("Invalid type, cannot cast [COMPLEX<tester>] to [DOUBLE]", t.getMessage());

    t = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ExprEval.ofType(someComplex, "hello").castTo(ExpressionType.STRING_ARRAY)
    );
    Assertions.assertEquals("Invalid type, cannot cast [COMPLEX<tester>] to [ARRAY<STRING>]", t.getMessage());

    t = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ExprEval.ofType(someComplex, "hello").castTo(ExpressionType.LONG_ARRAY)
    );
    Assertions.assertEquals("Invalid type, cannot cast [COMPLEX<tester>] to [ARRAY<LONG>]", t.getMessage());

    t = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ExprEval.ofType(someComplex, "hello").castTo(ExpressionType.DOUBLE_ARRAY)
    );
    Assertions.assertEquals("Invalid type, cannot cast [COMPLEX<tester>] to [ARRAY<DOUBLE>]", t.getMessage());

    t = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ExprEval.ofType(someComplex, "hello").castTo(ExpressionType.NESTED_DATA)
    );
    Assertions.assertEquals("Invalid type, cannot cast [COMPLEX<tester>] to [COMPLEX<json>]", t.getMessage());

    t = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ExprEval.of("hello").castTo(someComplex)
    );
    Assertions.assertEquals("Invalid type, cannot cast [STRING] to [COMPLEX<tester>]", t.getMessage());

    t = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ExprEval.of(123L).castTo(someComplex)
    );
    Assertions.assertEquals("Invalid type, cannot cast [LONG] to [COMPLEX<tester>]", t.getMessage());

    t = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ExprEval.of(1.23).castTo(someComplex)
    );
    Assertions.assertEquals("Invalid type, cannot cast [DOUBLE] to [COMPLEX<tester>]", t.getMessage());

    t = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ExprEval.ofStringArray(new Object[]{"a", "b", "c"}).castTo(someComplex)
    );
    Assertions.assertEquals("Invalid type, cannot cast [ARRAY<STRING>] to [COMPLEX<tester>]", t.getMessage());

    t = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ExprEval.ofLongArray(new Object[]{1L, 2L, 3L}).castTo(someComplex)
    );
    Assertions.assertEquals("Invalid type, cannot cast [ARRAY<LONG>] to [COMPLEX<tester>]", t.getMessage());

    t = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ExprEval.ofDoubleArray(new Object[]{1.1, 2.2, 3.3}).castTo(someComplex)
    );
    Assertions.assertEquals("Invalid type, cannot cast [ARRAY<DOUBLE>] to [COMPLEX<tester>]", t.getMessage());

    t = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> ExprEval.ofComplex(ExpressionType.NESTED_DATA, ImmutableMap.of("x", 1L)).castTo(someComplex)
    );
    Assertions.assertEquals("Invalid type, cannot cast [COMPLEX<json>] to [COMPLEX<tester>]", t.getMessage());
  }

  @Test
  public void testIsNumericNull()
  {
    if (NullHandling.sqlCompatible()) {
      Assertions.assertFalse(ExprEval.ofLong(1L).isNumericNull());
      Assertions.assertTrue(ExprEval.ofLong(null).isNumericNull());

      Assertions.assertFalse(ExprEval.ofDouble(1.0).isNumericNull());
      Assertions.assertTrue(ExprEval.ofDouble(null).isNumericNull());

      Assertions.assertTrue(ExprEval.of(null).isNumericNull());
      Assertions.assertTrue(ExprEval.of("one").isNumericNull());
      Assertions.assertFalse(ExprEval.of("1").isNumericNull());

      Assertions.assertFalse(ExprEval.ofLongArray(new Long[]{1L}).isNumericNull());
      Assertions.assertTrue(ExprEval.ofLongArray(new Long[]{null, 2L, 3L}).isNumericNull());
      Assertions.assertTrue(ExprEval.ofLongArray(new Long[]{null}).isNumericNull());

      Assertions.assertFalse(ExprEval.ofDoubleArray(new Double[]{1.1}).isNumericNull());
      Assertions.assertTrue(ExprEval.ofDoubleArray(new Double[]{null, 1.1, 2.2}).isNumericNull());
      Assertions.assertTrue(ExprEval.ofDoubleArray(new Double[]{null}).isNumericNull());

      Assertions.assertFalse(ExprEval.ofStringArray(new String[]{"1"}).isNumericNull());
      Assertions.assertTrue(ExprEval.ofStringArray(new String[]{null, "1", "2"}).isNumericNull());
      Assertions.assertTrue(ExprEval.ofStringArray(new String[]{"one"}).isNumericNull());
      Assertions.assertTrue(ExprEval.ofStringArray(new String[]{null}).isNumericNull());
    } else {
      Assertions.assertFalse(ExprEval.ofLong(1L).isNumericNull());
      Assertions.assertFalse(ExprEval.ofLong(null).isNumericNull());

      Assertions.assertFalse(ExprEval.ofDouble(1.0).isNumericNull());
      Assertions.assertFalse(ExprEval.ofDouble(null).isNumericNull());

      // strings are still null
      Assertions.assertTrue(ExprEval.of(null).isNumericNull());
      Assertions.assertTrue(ExprEval.of("one").isNumericNull());
      Assertions.assertFalse(ExprEval.of("1").isNumericNull());

      // arrays can still have nulls
      Assertions.assertFalse(ExprEval.ofLongArray(new Long[]{1L}).isNumericNull());
      Assertions.assertTrue(ExprEval.ofLongArray(new Long[]{null, 2L, 3L}).isNumericNull());
      Assertions.assertTrue(ExprEval.ofLongArray(new Long[]{null}).isNumericNull());

      Assertions.assertFalse(ExprEval.ofDoubleArray(new Double[]{1.1}).isNumericNull());
      Assertions.assertTrue(ExprEval.ofDoubleArray(new Double[]{null, 1.1, 2.2}).isNumericNull());
      Assertions.assertTrue(ExprEval.ofDoubleArray(new Double[]{null}).isNumericNull());

      Assertions.assertFalse(ExprEval.ofStringArray(new String[]{"1"}).isNumericNull());
      Assertions.assertTrue(ExprEval.ofStringArray(new String[]{null, "1", "2"}).isNumericNull());
      Assertions.assertTrue(ExprEval.ofStringArray(new String[]{"one"}).isNumericNull());
      Assertions.assertTrue(ExprEval.ofStringArray(new String[]{null}).isNumericNull());
    }
  }

  @Test
  public void testBooleanReturn()
  {
    Expr.ObjectBinding bindings = InputBindings.forMap(
        ImmutableMap.of("x", 100L, "y", 100L, "z", 100D, "w", 100D)
    );

    try {
      ExpressionProcessing.initializeForStrictBooleansTests(false);
      ExprEval eval = Parser.parse("x==z", ExprMacroTable.nil()).eval(bindings);
      Assertions.assertTrue(eval.asBoolean());
      assertEquals(ExpressionType.DOUBLE, eval.type());

      eval = Parser.parse("x!=z", ExprMacroTable.nil()).eval(bindings);
      Assertions.assertFalse(eval.asBoolean());
      assertEquals(ExpressionType.DOUBLE, eval.type());

      eval = Parser.parse("z==w", ExprMacroTable.nil()).eval(bindings);
      Assertions.assertTrue(eval.asBoolean());
      assertEquals(ExpressionType.DOUBLE, eval.type());

      eval = Parser.parse("z!=w", ExprMacroTable.nil()).eval(bindings);
      Assertions.assertFalse(eval.asBoolean());
      assertEquals(ExpressionType.DOUBLE, eval.type());
    }
    finally {
      ExpressionProcessing.initializeForTests();
    }
    try {
      ExpressionProcessing.initializeForStrictBooleansTests(true);
      ExprEval eval = Parser.parse("x==y", ExprMacroTable.nil()).eval(bindings);
      Assertions.assertTrue(eval.asBoolean());
      assertEquals(ExpressionType.LONG, eval.type());

      eval = Parser.parse("x!=y", ExprMacroTable.nil()).eval(bindings);
      Assertions.assertFalse(eval.asBoolean());
      assertEquals(ExpressionType.LONG, eval.type());

      eval = Parser.parse("x==z", ExprMacroTable.nil()).eval(bindings);
      Assertions.assertTrue(eval.asBoolean());
      assertEquals(ExpressionType.LONG, eval.type());

      eval = Parser.parse("x!=z", ExprMacroTable.nil()).eval(bindings);
      Assertions.assertFalse(eval.asBoolean());
      assertEquals(ExpressionType.LONG, eval.type());

      eval = Parser.parse("z==w", ExprMacroTable.nil()).eval(bindings);
      Assertions.assertTrue(eval.asBoolean());
      assertEquals(ExpressionType.LONG, eval.type());

      eval = Parser.parse("z!=w", ExprMacroTable.nil()).eval(bindings);
      Assertions.assertFalse(eval.asBoolean());
      assertEquals(ExpressionType.LONG, eval.type());
    }
    finally {
      ExpressionProcessing.initializeForTests();
    }
  }

  @Test
  public void testLogicalOperators()
  {
    Expr.ObjectBinding bindings = InputBindings.nilBindings();

    try {
      ExpressionProcessing.initializeForStrictBooleansTests(true);
      assertEquals(1L, eval("'true' && 'true'", bindings).value());
      assertEquals(0L, eval("'true' && 'false'", bindings).value());
      assertEquals(0L, eval("'false' && 'true'", bindings).value());
      assertEquals(0L, eval("'troo' && 'true'", bindings).value());
      assertEquals(0L, eval("'false' && 'false'", bindings).value());

      assertEquals(1L, eval("'true' || 'true'", bindings).value());
      assertEquals(1L, eval("'true' || 'false'", bindings).value());
      assertEquals(1L, eval("'false' || 'true'", bindings).value());
      assertEquals(1L, eval("'troo' || 'true'", bindings).value());
      assertEquals(0L, eval("'false' || 'false'", bindings).value());

      assertEquals(1L, eval("1 && 1", bindings).value());
      assertEquals(1L, eval("100 && 11", bindings).value());
      assertEquals(0L, eval("1 && 0", bindings).value());
      assertEquals(0L, eval("0 && 1", bindings).value());
      assertEquals(0L, eval("0 && 0", bindings).value());

      assertEquals(1L, eval("1 || 1", bindings).value());
      assertEquals(1L, eval("100 || 11", bindings).value());
      assertEquals(1L, eval("1 || 0", bindings).value());
      assertEquals(1L, eval("0 || 1", bindings).value());
      assertEquals(1L, eval("111 || 0", bindings).value());
      assertEquals(1L, eval("0 || 111", bindings).value());
      assertEquals(0L, eval("0 || 0", bindings).value());

      assertEquals(1L, eval("1.0 && 1.0", bindings).value());
      assertEquals(1L, eval("0.100 && 1.1", bindings).value());
      assertEquals(0L, eval("1.0 && 0.0", bindings).value());
      assertEquals(0L, eval("0.0 && 1.0", bindings).value());
      assertEquals(0L, eval("0.0 && 0.0", bindings).value());

      assertEquals(1L, eval("1.0 || 1.0", bindings).value());
      assertEquals(1L, eval("0.2 || 0.3", bindings).value());
      assertEquals(1L, eval("1.0 || 0.0", bindings).value());
      assertEquals(1L, eval("0.0 || 1.0", bindings).value());
      assertEquals(1L, eval("1.11 || 0.0", bindings).value());
      assertEquals(1L, eval("0.0 || 0.111", bindings).value());
      assertEquals(0L, eval("0.0 || 0.0", bindings).value());

      assertEquals(1L, eval("null || 1", bindings).value());
      assertEquals(1L, eval("1 || null", bindings).value());
      // in sql incompatible mode, null is false, so we return 0
      assertEquals(NullHandling.defaultLongValue(), eval("null || 0", bindings).valueOrDefault());
      assertEquals(NullHandling.defaultLongValue(), eval("0 || null", bindings).valueOrDefault());
      assertEquals(NullHandling.defaultLongValue(), eval("null || null", bindings).valueOrDefault());

      // in sql incompatible mode, null is false, so we return 0
      assertEquals(NullHandling.defaultLongValue(), eval("null && 1", bindings).valueOrDefault());
      assertEquals(NullHandling.defaultLongValue(), eval("1 && null", bindings).valueOrDefault());
      assertEquals(NullHandling.defaultLongValue(), eval("null && null", bindings).valueOrDefault());
      // if either side is false, output is false in both modes
      assertEquals(0L, eval("null && 0", bindings).value());
      assertEquals(0L, eval("0 && null", bindings).value());
    }
    finally {
      // reset
      ExpressionProcessing.initializeForTests();
    }

    try {
      // turn on legacy insanity mode
      ExpressionProcessing.initializeForStrictBooleansTests(false);

      assertEquals("true", eval("'true' && 'true'", bindings).value());
      assertEquals("false", eval("'true' && 'false'", bindings).value());
      assertEquals("false", eval("'false' && 'true'", bindings).value());
      assertEquals("troo", eval("'troo' && 'true'", bindings).value());
      assertEquals("false", eval("'false' && 'false'", bindings).value());

      assertEquals("true", eval("'true' || 'true'", bindings).value());
      assertEquals("true", eval("'true' || 'false'", bindings).value());
      assertEquals("true", eval("'false' || 'true'", bindings).value());
      assertEquals("true", eval("'troo' || 'true'", bindings).value());
      assertEquals("false", eval("'false' || 'false'", bindings).value());

      assertEquals(1.0, eval("1.0 && 1.0", bindings).value());
      assertEquals(1.1, eval("0.100 && 1.1", bindings).value());
      assertEquals(0.0, eval("1.0 && 0.0", bindings).value());
      assertEquals(0.0, eval("0.0 && 1.0", bindings).value());
      assertEquals(0.0, eval("0.0 && 0.0", bindings).value());

      assertEquals(1.0, eval("1.0 || 1.0", bindings).value());
      assertEquals(0.2, eval("0.2 || 0.3", bindings).value());
      assertEquals(1.0, eval("1.0 || 0.0", bindings).value());
      assertEquals(1.0, eval("0.0 || 1.0", bindings).value());
      assertEquals(1.11, eval("1.11 || 0.0", bindings).value());
      assertEquals(0.111, eval("0.0 || 0.111", bindings).value());
      assertEquals(0.0, eval("0.0 || 0.0", bindings).value());

      assertEquals(1L, eval("1 && 1", bindings).value());
      assertEquals(11L, eval("100 && 11", bindings).value());
      assertEquals(0L, eval("1 && 0", bindings).value());
      assertEquals(0L, eval("0 && 1", bindings).value());
      assertEquals(0L, eval("0 && 0", bindings).value());

      assertEquals(1L, eval("1 || 1", bindings).value());
      assertEquals(100L, eval("100 || 11", bindings).value());
      assertEquals(1L, eval("1 || 0", bindings).value());
      assertEquals(1L, eval("0 || 1", bindings).value());
      assertEquals(111L, eval("111 || 0", bindings).value());
      assertEquals(111L, eval("0 || 111", bindings).value());
      assertEquals(0L, eval("0 || 0", bindings).value());

      assertEquals(1.0, eval("1.0 && 1.0", bindings).value());
      assertEquals(1.1, eval("0.100 && 1.1", bindings).value());
      assertEquals(0.0, eval("1.0 && 0.0", bindings).value());
      assertEquals(0.0, eval("0.0 && 1.0", bindings).value());
      assertEquals(0.0, eval("0.0 && 0.0", bindings).value());

      assertEquals(1.0, eval("1.0 || 1.0", bindings).value());
      assertEquals(0.2, eval("0.2 || 0.3", bindings).value());
      assertEquals(1.0, eval("1.0 || 0.0", bindings).value());
      assertEquals(1.0, eval("0.0 || 1.0", bindings).value());
      assertEquals(1.11, eval("1.11 || 0.0", bindings).value());
      assertEquals(0.111, eval("0.0 || 0.111", bindings).value());
      assertEquals(0.0, eval("0.0 || 0.0", bindings).value());

      assertEquals(1L, eval("null || 1", bindings).value());
      assertEquals(1L, eval("1 || null", bindings).value());
      assertEquals(0L, eval("null || 0", bindings).value());
      Assertions.assertNull(eval("0 || null", bindings).value());
      Assertions.assertNull(eval("null || null", bindings).value());

      Assertions.assertNull(eval("null && 1", bindings).value());
      Assertions.assertNull(eval("1 && null", bindings).value());
      Assertions.assertNull(eval("null && 0", bindings).value());
      assertEquals(0L, eval("0 && null", bindings).value());
      assertNull(eval("null && null", bindings).value());
    }
    finally {
      // reset
      ExpressionProcessing.initializeForTests();
    }
  }

  @Test
  public void testBooleanInputs()
  {
    Map<String, Object> bindingsMap = new HashMap<>();
    bindingsMap.put("l1", 100L);
    bindingsMap.put("l2", 0L);
    bindingsMap.put("d1", 1.1);
    bindingsMap.put("d2", 0.0);
    bindingsMap.put("s1", "true");
    bindingsMap.put("s2", "false");
    bindingsMap.put("b1", true);
    bindingsMap.put("b2", false);
    Expr.ObjectBinding bindings = InputBindings.forMap(bindingsMap);

    try {
      ExpressionProcessing.initializeForStrictBooleansTests(true);
      assertEquals(1L, eval("s1 && s1", bindings).value());
      assertEquals(0L, eval("s1 && s2", bindings).value());
      assertEquals(0L, eval("s2 && s1", bindings).value());
      assertEquals(0L, eval("s2 && s2", bindings).value());

      assertEquals(1L, eval("s1 || s1", bindings).value());
      assertEquals(1L, eval("s1 || s2", bindings).value());
      assertEquals(1L, eval("s2 || s1", bindings).value());
      assertEquals(0L, eval("s2 || s2", bindings).value());

      assertEquals(1L, eval("l1 && l1", bindings).value());
      assertEquals(0L, eval("l1 && l2", bindings).value());
      assertEquals(0L, eval("l2 && l1", bindings).value());
      assertEquals(0L, eval("l2 && l2", bindings).value());

      assertEquals(1L, eval("b1 && b1", bindings).value());
      assertEquals(0L, eval("b1 && b2", bindings).value());
      assertEquals(0L, eval("b2 && b1", bindings).value());
      assertEquals(0L, eval("b2 && b2", bindings).value());

      assertEquals(1L, eval("d1 && d1", bindings).value());
      assertEquals(0L, eval("d1 && d2", bindings).value());
      assertEquals(0L, eval("d2 && d1", bindings).value());
      assertEquals(0L, eval("d2 && d2", bindings).value());

      assertEquals(1L, eval("b1", bindings).value());
      assertEquals(1L, eval("if(b1,1,0)", bindings).value());
      assertEquals(1L, eval("if(l1,1,0)", bindings).value());
      assertEquals(1L, eval("if(d1,1,0)", bindings).value());
      assertEquals(1L, eval("if(s1,1,0)", bindings).value());
      assertEquals(0L, eval("if(b2,1,0)", bindings).value());
      assertEquals(0L, eval("if(l2,1,0)", bindings).value());
      assertEquals(0L, eval("if(d2,1,0)", bindings).value());
      assertEquals(0L, eval("if(s2,1,0)", bindings).value());
    }
    finally {
      // reset
      ExpressionProcessing.initializeForTests();
    }

    try {
      // turn on legacy insanity mode
      ExpressionProcessing.initializeForStrictBooleansTests(false);

      assertEquals("true", eval("s1 && s1", bindings).value());
      assertEquals("false", eval("s1 && s2", bindings).value());
      assertEquals("false", eval("s2 && s1", bindings).value());
      assertEquals("false", eval("s2 && s2", bindings).value());

      assertEquals("true", eval("b1 && b1", bindings).value());
      assertEquals("false", eval("b1 && b2", bindings).value());
      assertEquals("false", eval("b2 && b1", bindings).value());
      assertEquals("false", eval("b2 && b2", bindings).value());

      assertEquals(100L, eval("l1 && l1", bindings).value());
      assertEquals(0L, eval("l1 && l2", bindings).value());
      assertEquals(0L, eval("l2 && l1", bindings).value());
      assertEquals(0L, eval("l2 && l2", bindings).value());

      assertEquals(1.1, eval("d1 && d1", bindings).value());
      assertEquals(0.0, eval("d1 && d2", bindings).value());
      assertEquals(0.0, eval("d2 && d1", bindings).value());
      assertEquals(0.0, eval("d2 && d2", bindings).value());

      assertEquals("true", eval("b1", bindings).value());
      assertEquals(1L, eval("if(b1,1,0)", bindings).value());
      assertEquals(1L, eval("if(l1,1,0)", bindings).value());
      assertEquals(1L, eval("if(d1,1,0)", bindings).value());
      assertEquals(1L, eval("if(s1,1,0)", bindings).value());
      assertEquals(0L, eval("if(b2,1,0)", bindings).value());
      assertEquals(0L, eval("if(l2,1,0)", bindings).value());
      assertEquals(0L, eval("if(d2,1,0)", bindings).value());
      assertEquals(0L, eval("if(s2,1,0)", bindings).value());
    }
    finally {
      // reset
      ExpressionProcessing.initializeForTests();
    }
  }

  @Test
  public void testArrayComparison()
  {
    Expr.ObjectBinding bindings = InputBindings.forInputSuppliers(
        ImmutableMap.<String, InputBindings.InputSupplier<?>>builder()
                    .put(
                        "stringArray",
                        InputBindings.inputSupplier(ExpressionType.STRING_ARRAY, () -> new Object[]{"a", "b", null, "c"})
                    )
                    .put(
                        "longArray",
                        InputBindings.inputSupplier(ExpressionType.LONG_ARRAY, () -> new Object[]{1L, null, 2L, 3L})
                    )
                    .put(
                        "doubleArray",
                        InputBindings.inputSupplier(ExpressionType.DOUBLE_ARRAY, () -> new Object[]{1.1, 2.2, 3.3, null})
                    )
                    .build()
    );

    Assertions.assertEquals(0L, eval("['a','b',null,'c'] > stringArray", bindings).value());
    Assertions.assertEquals(1L, eval("['a','b',null,'c'] >= stringArray", bindings).value());
    Assertions.assertEquals(1L, eval("['a','b',null,'c'] == stringArray", bindings).value());
    Assertions.assertEquals(0L, eval("['a','b',null,'c'] != stringArray", bindings).value());
    Assertions.assertEquals(1L, eval("notdistinctfrom(['a','b',null,'c'], stringArray)", bindings).value());
    Assertions.assertEquals(0L, eval("isdistinctfrom(['a','b',null,'c'], stringArray)", bindings).value());
    Assertions.assertEquals(1L, eval("['a','b',null,'c'] <= stringArray", bindings).value());
    Assertions.assertEquals(0L, eval("['a','b',null,'c'] < stringArray", bindings).value());

    Assertions.assertEquals(0L, eval("[1,null,2,3] > longArray", bindings).value());
    Assertions.assertEquals(1L, eval("[1,null,2,3] >= longArray", bindings).value());
    Assertions.assertEquals(1L, eval("[1,null,2,3] == longArray", bindings).value());
    Assertions.assertEquals(0L, eval("[1,null,2,3] != longArray", bindings).value());
    Assertions.assertEquals(1L, eval("notdistinctfrom([1,null,2,3], longArray)", bindings).value());
    Assertions.assertEquals(0L, eval("isdistinctfrom([1,null,2,3], longArray)", bindings).value());
    Assertions.assertEquals(1L, eval("[1,null,2,3] <= longArray", bindings).value());
    Assertions.assertEquals(0L, eval("[1,null,2,3] < longArray", bindings).value());

    Assertions.assertEquals(0L, eval("[1.1,2.2,3.3,null] > doubleArray", bindings).value());
    Assertions.assertEquals(1L, eval("[1.1,2.2,3.3,null] >= doubleArray", bindings).value());
    Assertions.assertEquals(1L, eval("[1.1,2.2,3.3,null] == doubleArray", bindings).value());
    Assertions.assertEquals(0L, eval("[1.1,2.2,3.3,null] != doubleArray", bindings).value());
    Assertions.assertEquals(1L, eval("notdistinctfrom([1.1,2.2,3.3,null], doubleArray)", bindings).value());
    Assertions.assertEquals(0L, eval("isdistinctfrom([1.1,2.2,3.3,null], doubleArray)", bindings).value());
    Assertions.assertEquals(1L, eval("[1.1,2.2,3.3,null] <= doubleArray", bindings).value());
    Assertions.assertEquals(0L, eval("[1.1,2.2,3.3,null] < doubleArray", bindings).value());
  }

  @Test
  public void testValueOrDefault()
  {
    ExprEval<?> longNull = ExprEval.ofLong(null);
    ExprEval<?> doubleNull = ExprEval.ofDouble(null);
    Assertions.assertEquals(NullHandling.sqlCompatible(), longNull.isNumericNull());
    Assertions.assertEquals(NullHandling.sqlCompatible(), doubleNull.isNumericNull());
    Assertions.assertEquals(NullHandling.defaultLongValue(), longNull.valueOrDefault());
    Assertions.assertEquals(NullHandling.defaultDoubleValue(), doubleNull.valueOrDefault());
    if (NullHandling.replaceWithDefault()) {
      Assertions.assertEquals(0L, longNull.asLong());
      Assertions.assertEquals(0, longNull.asInt());
      Assertions.assertEquals(0.0, longNull.asDouble(), 0.0);

      Assertions.assertEquals(0L, doubleNull.asLong());
      Assertions.assertEquals(0, doubleNull.asInt());
      Assertions.assertEquals(0.0, doubleNull.asDouble(), 0.0);
    }
  }

  @Test
  public void testEvalOfType()
  {
    // strings
    ExprEval eval = ExprEval.ofType(ExpressionType.STRING, "stringy");
    Assertions.assertEquals(ExpressionType.STRING, eval.type());
    Assertions.assertEquals("stringy", eval.value());

    eval = ExprEval.ofType(ExpressionType.STRING, 1L);
    Assertions.assertEquals(ExpressionType.STRING, eval.type());
    Assertions.assertEquals("1", eval.value());

    eval = ExprEval.ofType(ExpressionType.STRING, 1.0);
    Assertions.assertEquals(ExpressionType.STRING, eval.type());
    Assertions.assertEquals("1.0", eval.value());

    eval = ExprEval.ofType(ExpressionType.STRING, true);
    Assertions.assertEquals(ExpressionType.STRING, eval.type());
    Assertions.assertEquals("true", eval.value());

    // strings might also be liars and arrays or lists
    eval = ExprEval.ofType(ExpressionType.STRING, new Object[]{"a", "b", "c"});
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{"a", "b", "c"}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.STRING, new String[]{"a", "b", "c"});
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{"a", "b", "c"}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.STRING, Arrays.asList("a", "b", "c"));
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{"a", "b", "c"}, (Object[]) eval.value());

    // longs
    eval = ExprEval.ofType(ExpressionType.LONG, 1L);
    Assertions.assertEquals(ExpressionType.LONG, eval.type());
    Assertions.assertEquals(1L, eval.value());

    eval = ExprEval.ofType(ExpressionType.LONG, 1.0);
    Assertions.assertEquals(ExpressionType.LONG, eval.type());
    Assertions.assertEquals(1L, eval.value());

    eval = ExprEval.ofType(ExpressionType.LONG, "1");
    Assertions.assertEquals(ExpressionType.LONG, eval.type());
    Assertions.assertEquals(1L, eval.value());

    eval = ExprEval.ofType(ExpressionType.LONG, true);
    Assertions.assertEquals(ExpressionType.LONG, eval.type());
    Assertions.assertEquals(1L, eval.value());

    // doubles
    eval = ExprEval.ofType(ExpressionType.DOUBLE, 1L);
    Assertions.assertEquals(ExpressionType.DOUBLE, eval.type());
    Assertions.assertEquals(1.0, eval.value());

    eval = ExprEval.ofType(ExpressionType.DOUBLE, 1.0);
    Assertions.assertEquals(ExpressionType.DOUBLE, eval.type());
    Assertions.assertEquals(1.0, eval.value());

    eval = ExprEval.ofType(ExpressionType.DOUBLE, "1");
    Assertions.assertEquals(ExpressionType.DOUBLE, eval.type());
    Assertions.assertEquals(1.0, eval.value());

    eval = ExprEval.ofType(ExpressionType.DOUBLE, true);
    Assertions.assertEquals(ExpressionType.DOUBLE, eval.type());
    Assertions.assertEquals(1.0, eval.value());

    // complex
    TypeStrategiesTest.NullableLongPair pair = new TypeStrategiesTest.NullableLongPair(1L, 2L);
    ExpressionType type = ExpressionType.fromColumnType(TypeStrategiesTest.NULLABLE_TEST_PAIR_TYPE);

    eval = ExprEval.ofType(type, pair);
    Assertions.assertEquals(type, eval.type());
    Assertions.assertEquals(pair, eval.value());

    ByteBuffer buffer = ByteBuffer.allocate(TypeStrategiesTest.NULLABLE_TEST_PAIR_TYPE.getStrategy().estimateSizeBytes(pair));
    TypeStrategiesTest.NULLABLE_TEST_PAIR_TYPE.getStrategy().write(buffer, pair, buffer.limit());
    byte[] pairBytes = buffer.array();
    eval = ExprEval.ofType(type, pairBytes);
    Assertions.assertEquals(type, eval.type());
    Assertions.assertEquals(pair, eval.value());

    eval = ExprEval.ofType(type, StringUtils.encodeBase64String(pairBytes));
    Assertions.assertEquals(type, eval.type());
    Assertions.assertEquals(pair, eval.value());

    // json type best efforts its way to other types
    eval = ExprEval.ofType(ExpressionType.NESTED_DATA, ImmutableMap.of("x", 1L, "y", 2L));
    Assertions.assertEquals(ExpressionType.NESTED_DATA, eval.type());
    Assertions.assertEquals(ImmutableMap.of("x", 1L, "y", 2L), eval.value());

    ExpressionType stringyComplexThing = ExpressionType.fromString("COMPLEX<somestringything>");
    eval = ExprEval.ofType(stringyComplexThing, "notbase64");
    Assertions.assertEquals(stringyComplexThing, eval.type());
    Assertions.assertEquals("notbase64", eval.value());

    // arrays
    eval = ExprEval.ofType(ExpressionType.LONG_ARRAY, new Object[]{1L, 2L, 3L});
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1L, 2L, 3L}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.LONG_ARRAY, ImmutableList.of(1L, 2L, 3L));
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1L, 2L, 3L}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.LONG_ARRAY, new Long[]{1L, 2L, 3L});
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1L, 2L, 3L}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.LONG_ARRAY, new long[]{1L, 2L, 3L});
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1L, 2L, 3L}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.LONG_ARRAY, new int[]{1, 2, 3});
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1L, 2L, 3L}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.LONG_ARRAY, new Object[]{1L, 2L, null, 3L});
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1L, 2L, null, 3L}, (Object[]) eval.value());

    // arrays might have to fall back to using 'bestEffortOf', but will cast it to the expected output type
    eval = ExprEval.ofType(ExpressionType.LONG_ARRAY, new Object[]{"1", "2", "3"});
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1L, 2L, 3L}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.LONG_ARRAY, new String[]{"1", "2", "3"});
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1L, 2L, 3L}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.LONG_ARRAY, new Object[]{"1", "2", "wat", "3"});
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1L, 2L, null, 3L}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.LONG_ARRAY, new Object[]{1.0, 2.0, 3.0});
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1L, 2L, 3L}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.LONG_ARRAY, new double[]{1.0, 2.0, 3.0});
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1L, 2L, 3L}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.LONG_ARRAY, new Object[]{1.0, 2.0, null, 3.0});
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1L, 2L, null, 3L}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.LONG_ARRAY, new Object[]{1.0, 2L, "3", true, false});
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1L, 2L, 3L, 1L, 0L}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.LONG_ARRAY, new float[]{1.0f, 2.0f, 3.0f});
    Assertions.assertEquals(ExpressionType.LONG_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1L, 2L, 3L}, (Object[]) eval.value());

    // etc
    eval = ExprEval.ofType(ExpressionType.DOUBLE_ARRAY, new Object[]{1.0, 2.0, 3.0});
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1.0, 2.0, 3.0}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.DOUBLE_ARRAY, new Double[]{1.0, 2.0, 3.0});
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1.0, 2.0, 3.0}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.DOUBLE_ARRAY, new double[]{1.0, 2.0, 3.0});
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1.0, 2.0, 3.0}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.DOUBLE_ARRAY, new Object[]{"1", "2", "3"});
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1.0, 2.0, 3.0}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.DOUBLE_ARRAY, new Object[]{"1", "2", "wat", "3"});
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1.0, 2.0, null, 3.0}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.DOUBLE_ARRAY, new Object[]{1L, 2L, 3L});
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1.0, 2.0, 3.0}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.DOUBLE_ARRAY, new long[]{1L, 2L, 3L});
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1.0, 2.0, 3.0}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.DOUBLE_ARRAY, new Object[]{1L, 2L, null, 3L});
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1.0, 2.0, null, 3.0}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.DOUBLE_ARRAY, new Object[]{1.0, 2L, "3", true, false});
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1.0, 2.0, 3.0, 1.0, 0.0}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.DOUBLE_ARRAY, new Float[]{1.0f, 2.0f, 3.0f});
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1.0, 2.0, 3.0}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.DOUBLE_ARRAY, new float[]{1.0f, 2.0f, 3.0f});
    Assertions.assertEquals(ExpressionType.DOUBLE_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{1.0, 2.0, 3.0}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.STRING_ARRAY, new Object[]{"1", "2", "3"});
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{"1", "2", "3"}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.STRING_ARRAY, new Object[]{1L, 2L, 3L});
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{"1", "2", "3"}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.STRING_ARRAY, new Object[]{1.0, 2.0, 3.0});
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{"1.0", "2.0", "3.0"}, (Object[]) eval.value());

    eval = ExprEval.ofType(ExpressionType.STRING_ARRAY, new Object[]{1.0, 2L, "3", true, false});
    Assertions.assertEquals(ExpressionType.STRING_ARRAY, eval.type());
    Assertions.assertArrayEquals(new Object[]{"1.0", "2", "3", "true", "false"}, (Object[]) eval.value());

    // nested arrays
    ExpressionType nestedLongArray = ExpressionTypeFactory.getInstance().ofArray(ExpressionType.LONG_ARRAY);
    final Object[] expectedLongArray = new Object[]{
        new Object[]{1L, 2L, 3L},
        new Object[]{5L, null, 9L},
        null,
        new Object[]{2L, 4L, 6L}
    };

    List<?> longArrayInputs = Arrays.asList(
        new Object[]{
            new Object[]{1L, 2L, 3L},
            new Object[]{5L, null, 9L},
            null,
            new Object[]{2L, 4L, 6L}
        },
        Arrays.asList(
            new Object[]{1L, 2L, 3L},
            new Object[]{5L, null, 9L},
            null,
            new Object[]{2L, 4L, 6L}
        ),
        Arrays.asList(
            Arrays.asList(1L, 2L, 3L),
            Arrays.asList(5L, null, 9L),
            null,
            Arrays.asList(2L, 4L, 6L)
        ),
        Arrays.asList(
            Arrays.asList(1L, 2L, 3L),
            Arrays.asList("5", "hello", "9"),
            null,
            new Object[]{2.2, 4.4, 6.6}
        )
    );

    for (Object o : longArrayInputs) {
      eval = ExprEval.ofType(nestedLongArray, o);
      Assertions.assertEquals(nestedLongArray, eval.type());
      Object[] val = (Object[]) eval.value();
      Assertions.assertEquals(expectedLongArray.length, val.length);
      for (int i = 0; i < expectedLongArray.length; i++) {
        Assertions.assertArrayEquals((Object[]) expectedLongArray[i], (Object[]) val[i]);
      }
    }

    ExpressionType nestedDoubleArray = ExpressionTypeFactory.getInstance().ofArray(ExpressionType.DOUBLE_ARRAY);
    final Object[] expectedDoubleArray = new Object[]{
        new Object[]{1.1, 2.2, 3.3},
        new Object[]{5.5, null, 9.9},
        null,
        new Object[]{2.2, 4.4, 6.6}
    };

    List<?> doubleArrayInputs = Arrays.asList(
        new Object[]{
            new Object[]{1.1, 2.2, 3.3},
            new Object[]{5.5, null, 9.9},
            null,
            new Object[]{2.2, 4.4, 6.6}
        },
        new Object[]{
            Arrays.asList(1.1, 2.2, 3.3),
            Arrays.asList(5.5, null, 9.9),
            null,
            Arrays.asList(2.2, 4.4, 6.6)
        },
        Arrays.asList(
            Arrays.asList(1.1, 2.2, 3.3),
            Arrays.asList(5.5, null, 9.9),
            null,
            Arrays.asList(2.2, 4.4, 6.6)
        ),
        new Object[]{
            new Object[]{"1.1", "2.2", "3.3"},
            Arrays.asList("5.5", null, "9.9"),
            null,
            new String[]{"2.2", "4.4", "6.6"}
        }
    );

    for (Object o : doubleArrayInputs) {
      eval = ExprEval.ofType(nestedDoubleArray, o);
      Assertions.assertEquals(nestedDoubleArray, eval.type());
      Object[] val = (Object[]) eval.value();
      Assertions.assertEquals(expectedLongArray.length, val.length);
      for (int i = 0; i < expectedLongArray.length; i++) {
        Assertions.assertArrayEquals((Object[]) expectedDoubleArray[i], (Object[]) val[i]);
      }
    }
  }

  @Test
  public void testBestEffortOf()
  {
    // strings
    assertBestEffortOf("stringy", ExpressionType.STRING, "stringy");


    assertBestEffortOf(
        new byte[]{1, 2, 3, 4},
        ExpressionType.STRING,
        StringUtils.encodeBase64String(new byte[]{1, 2, 3, 4})
    );

    // longs
    assertBestEffortOf(1L, ExpressionType.LONG, 1L);
    assertBestEffortOf(1, ExpressionType.LONG, 1L);

    // by default, booleans are handled as longs
    assertBestEffortOf(true, ExpressionType.LONG, 1L);
    assertBestEffortOf(Arrays.asList(true, false), ExpressionType.LONG_ARRAY, new Object[]{1L, 0L});

    try {
      // in non-strict boolean mode, they are strings
      ExpressionProcessing.initializeForStrictBooleansTests(false);
      assertBestEffortOf(true, ExpressionType.STRING, "true");
      assertBestEffortOf(Arrays.asList(true, false), ExpressionType.STRING_ARRAY, new Object[]{"true", "false"});
    }
    finally {
      // reset
      ExpressionProcessing.initializeForTests();
    }

    // doubles
    assertBestEffortOf(1.0, ExpressionType.DOUBLE, 1.0);
    assertBestEffortOf(1.0f, ExpressionType.DOUBLE, 1.0);

    // arrays
    assertBestEffortOf(new Object[]{1L, 2L, 3L}, ExpressionType.LONG_ARRAY, new Object[]{1L, 2L, 3L});
    assertBestEffortOf(new Object[]{1L, 2L, null, 3L}, ExpressionType.LONG_ARRAY, new Object[]{1L, 2L, null, 3L});
    assertBestEffortOf(ImmutableList.of(1L, 2L, 3L), ExpressionType.LONG_ARRAY, new Object[]{1L, 2L, 3L});
    assertBestEffortOf(new long[]{1L, 2L, 3L}, ExpressionType.LONG_ARRAY, new Object[]{1L, 2L, 3L});
    assertBestEffortOf(new Object[]{1, 2, 3}, ExpressionType.LONG_ARRAY, new Object[]{1L, 2L, 3L});
    assertBestEffortOf(new Integer[]{1, 2, 3}, ExpressionType.LONG_ARRAY, new Object[]{1L, 2L, 3L});
    assertBestEffortOf(new int[]{1, 2, 3}, ExpressionType.LONG_ARRAY, new Object[]{1L, 2L, 3L});

    assertBestEffortOf(new Object[]{1.0, 2.0, 3.0}, ExpressionType.DOUBLE_ARRAY, new Object[]{1.0, 2.0, 3.0});
    assertBestEffortOf(
        new Object[]{null, 1.0, 2.0, 3.0},
        ExpressionType.DOUBLE_ARRAY,
        new Object[]{null, 1.0, 2.0, 3.0}
    );
    assertBestEffortOf(new Double[]{1.0, 2.0, 3.0}, ExpressionType.DOUBLE_ARRAY, new Object[]{1.0, 2.0, 3.0});
    assertBestEffortOf(new double[]{1.0, 2.0, 3.0}, ExpressionType.DOUBLE_ARRAY, new Object[]{1.0, 2.0, 3.0});
    assertBestEffortOf(new Object[]{1.0f, 2.0f, 3.0f}, ExpressionType.DOUBLE_ARRAY, new Object[]{1.0, 2.0, 3.0});
    assertBestEffortOf(new Float[]{1.0f, 2.0f, 3.0f}, ExpressionType.DOUBLE_ARRAY, new Object[]{1.0, 2.0, 3.0});
    assertBestEffortOf(new float[]{1.0f, 2.0f, 3.0f}, ExpressionType.DOUBLE_ARRAY, new Object[]{1.0, 2.0, 3.0});

    assertBestEffortOf(new Object[]{"1", "2", "3"}, ExpressionType.STRING_ARRAY, new Object[]{"1", "2", "3"});
    assertBestEffortOf(new String[]{"1", "2", "3"}, ExpressionType.STRING_ARRAY, new Object[]{"1", "2", "3"});
    assertBestEffortOf(ImmutableList.of("1", "2", "3"), ExpressionType.STRING_ARRAY, new Object[]{"1", "2", "3"});

    // arrays end up as the least restrictive type
    assertBestEffortOf(new Object[]{1.0, 2L}, ExpressionType.DOUBLE_ARRAY, new Object[]{1.0, 2.0});

    // arrays end up as the least restrictive type
    assertBestEffortOf(
        new Object[]{1.0, 2L, "3", true, false},
        ExpressionType.STRING_ARRAY,
        new Object[]{"1.0", "2", "3", "true", "false"}
    );

    assertBestEffortOf(
        ImmutableMap.of("x", 1L, "y", 2L),
        ExpressionType.NESTED_DATA,
        ImmutableMap.of("x", 1L, "y", 2L)
    );

    SerializablePair<String, Long> someOtherComplex = new SerializablePair<>("hello", 1234L);
    assertBestEffortOf(
        someOtherComplex,
        ExpressionType.UNKNOWN_COMPLEX,
        someOtherComplex
    );

    assertBestEffortOf(
        ComparableStringArray.of("a", "b", "c"),
        ExpressionType.STRING_ARRAY,
        new Object[]{"a", "b", "c"}
    );

    assertBestEffortOf(
        new ComparableList<>(Arrays.asList(1L, 2L)),
        ExpressionType.LONG_ARRAY,
        new Object[]{1L, 2L}
    );
  }

  private void assertBestEffortOf(@Nullable Object val, ExpressionType expectedType, @Nullable Object expectedValue)
  {
    ExprEval eval = ExprEval.bestEffortOf(val);
    Assertions.assertEquals(expectedType, eval.type());
    if (eval.type().isArray()) {
      Assertions.assertArrayEquals((Object[]) expectedValue, eval.asArray());
    } else {
      Assertions.assertEquals(expectedValue, eval.value());
    }
    // make sure that ofType matches bestEffortOf
    eval = ExprEval.ofType(eval.type(), val);
    Assertions.assertEquals(expectedType, eval.type());
    if (eval.type().isArray()) {
      Assertions.assertArrayEquals((Object[]) expectedValue, eval.asArray());
    } else {
      Assertions.assertEquals(expectedValue, eval.value());
    }
  }
}
