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

package org.apache.druid.query.expression;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.math.expr.InputBindings;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class HyperUniqueExpressionsTest extends InitializedNullHandlingTest
{
  private static final ExprMacroTable MACRO_TABLE = new ExprMacroTable(
      ImmutableList.of(
          new HyperUniqueExpressions.HllCreateExprMacro(),
          new HyperUniqueExpressions.HllAddExprMacro(),
          new HyperUniqueExpressions.HllEstimateExprMacro(),
          new HyperUniqueExpressions.HllRoundEstimateExprMacro()
      )
  );

  private static final String SOME_STRING = "foo";
  private static final long SOME_LONG = 1234L;
  private static final double SOME_DOUBLE = 1.234;

  Expr.ObjectBinding inputBindings = InputBindings.forInputSuppliers(
      new ImmutableMap.Builder<String, InputBindings.InputSupplier<?>>()
          .put("hll", InputBindings.inputSupplier(HyperUniqueExpressions.TYPE, HyperLogLogCollector::makeLatestCollector))
          .put("string", InputBindings.inputSupplier(ExpressionType.STRING, () -> SOME_STRING))
          .put("long", InputBindings.inputSupplier(ExpressionType.LONG, () -> SOME_LONG))
          .put("double", InputBindings.inputSupplier(ExpressionType.DOUBLE, () -> SOME_DOUBLE))
          .put("nullString", InputBindings.inputSupplier(ExpressionType.STRING, () -> null))
          .put("nullLong", InputBindings.inputSupplier(ExpressionType.LONG, () -> null))
          .put("nullDouble", InputBindings.inputSupplier(ExpressionType.DOUBLE, () -> null))
          .build()
  );

  @Test
  public void testCreate()
  {
    Expr expr = Parser.parse("hyper_unique()", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);

    Assertions.assertEquals(HyperUniqueExpressions.TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof HyperLogLogCollector);
    Assertions.assertEquals(0.0, ((HyperLogLogCollector) eval.value()).estimateCardinality(), 0);
  }

  @Test
  public void testString()
  {
    Expr expr = Parser.parse("hyper_unique_add('foo', hyper_unique())", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);

    Assertions.assertEquals(HyperUniqueExpressions.TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof HyperLogLogCollector);
    Assertions.assertEquals(1.0, ((HyperLogLogCollector) eval.value()).estimateCardinality(), 0.01);

    expr = Parser.parse("hyper_unique_add('bar', hyper_unique_add('foo', hyper_unique()))", MACRO_TABLE);
    eval = expr.eval(inputBindings);

    Assertions.assertEquals(HyperUniqueExpressions.TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof HyperLogLogCollector);
    Assertions.assertEquals(2.0, ((HyperLogLogCollector) eval.value()).estimateCardinality(), 0.01);

    expr = Parser.parse("hyper_unique_add(string, hyper_unique())", MACRO_TABLE);
    eval = expr.eval(inputBindings);

    Assertions.assertEquals(HyperUniqueExpressions.TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof HyperLogLogCollector);
    Assertions.assertEquals(1.0, ((HyperLogLogCollector) eval.value()).estimateCardinality(), 0.01);

    expr = Parser.parse("hyper_unique_add(nullString, hyper_unique())", MACRO_TABLE);
    eval = expr.eval(inputBindings);

    Assertions.assertEquals(HyperUniqueExpressions.TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof HyperLogLogCollector);
    Assertions.assertEquals(NullHandling.replaceWithDefault() ? 1.0 : 0.0, ((HyperLogLogCollector) eval.value()).estimateCardinality(), 0.01);
  }

  @Test
  public void testLong()
  {
    Expr expr = Parser.parse("hyper_unique_add(1234, hyper_unique())", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);

    Assertions.assertEquals(HyperUniqueExpressions.TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof HyperLogLogCollector);
    Assertions.assertEquals(1.0, ((HyperLogLogCollector) eval.value()).estimateCardinality(), 0.01);

    expr = Parser.parse("hyper_unique_add(1234, hyper_unique_add(5678, hyper_unique()))", MACRO_TABLE);
    eval = expr.eval(inputBindings);

    Assertions.assertEquals(HyperUniqueExpressions.TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof HyperLogLogCollector);
    Assertions.assertEquals(2.0, ((HyperLogLogCollector) eval.value()).estimateCardinality(), 0.01);

    expr = Parser.parse("hyper_unique_add(long, hyper_unique())", MACRO_TABLE);
    eval = expr.eval(inputBindings);

    Assertions.assertEquals(HyperUniqueExpressions.TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof HyperLogLogCollector);
    Assertions.assertEquals(1.0, ((HyperLogLogCollector) eval.value()).estimateCardinality(), 0.01);

    expr = Parser.parse("hyper_unique_add(nullLong, hyper_unique())", MACRO_TABLE);
    eval = expr.eval(inputBindings);

    Assertions.assertEquals(HyperUniqueExpressions.TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof HyperLogLogCollector);
    Assertions.assertEquals(NullHandling.replaceWithDefault() ? 1.0 : 0.0, ((HyperLogLogCollector) eval.value()).estimateCardinality(), 0.01);
  }

  @Test
  public void testDouble()
  {
    Expr expr = Parser.parse("hyper_unique_add(1.234, hyper_unique())", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);

    Assertions.assertEquals(HyperUniqueExpressions.TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof HyperLogLogCollector);
    Assertions.assertEquals(1.0, ((HyperLogLogCollector) eval.value()).estimateCardinality(), 0.01);

    expr = Parser.parse("hyper_unique_add(1.234, hyper_unique_add(5.678, hyper_unique()))", MACRO_TABLE);
    eval = expr.eval(inputBindings);

    Assertions.assertEquals(HyperUniqueExpressions.TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof HyperLogLogCollector);
    Assertions.assertEquals(2.0, ((HyperLogLogCollector) eval.value()).estimateCardinality(), 0.01);

    expr = Parser.parse("hyper_unique_add(double, hyper_unique())", MACRO_TABLE);
    eval = expr.eval(inputBindings);

    Assertions.assertEquals(HyperUniqueExpressions.TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof HyperLogLogCollector);
    Assertions.assertEquals(1.0, ((HyperLogLogCollector) eval.value()).estimateCardinality(), 0.01);

    expr = Parser.parse("hyper_unique_add(nullDouble, hyper_unique())", MACRO_TABLE);
    eval = expr.eval(inputBindings);

    Assertions.assertEquals(HyperUniqueExpressions.TYPE, eval.type());
    Assertions.assertTrue(eval.value() instanceof HyperLogLogCollector);
    Assertions.assertEquals(NullHandling.replaceWithDefault() ? 1.0 : 0.0, ((HyperLogLogCollector) eval.value()).estimateCardinality(), 0.01);
  }

  @Test
  public void testEstimate()
  {
    Expr expr = Parser.parse("hyper_unique_estimate(hyper_unique_add(1.234, hyper_unique()))", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);

    Assertions.assertEquals(ExpressionType.DOUBLE, eval.type());
    Assertions.assertEquals(1.0, eval.asDouble(), 0.01);
  }

  @Test
  public void testEstimateRound()
  {
    Expr expr = Parser.parse("hyper_unique_round_estimate(hyper_unique_add(1.234, hyper_unique()))", MACRO_TABLE);
    ExprEval eval = expr.eval(inputBindings);

    Assertions.assertEquals(ExpressionType.LONG, eval.type());
    Assertions.assertEquals(1L, eval.asLong(), 0.01);
  }

  @Test
  public void testCreateWrongArgsCount()
  {
    Throwable exception = assertThrows(IAE.class, () -> {
      Parser.parse("hyper_unique(100)", MACRO_TABLE);
    });
    assertTrue(exception.getMessage().contains("Function[hyper_unique] does not accept arguments"));
  }

  @Test
  public void testAddWrongArgsCount()
  {
    Throwable exception = assertThrows(IAE.class, () -> {
      Parser.parse("hyper_unique_add(100, hyper_unique(), 100)", MACRO_TABLE);
    });
    assertTrue(exception.getMessage().contains("Function[hyper_unique_add] requires 2 arguments"));
  }

  @Test
  public void testAddWrongArgType()
  {
    Throwable exception = assertThrows(IAE.class, () -> {
      Expr expr = Parser.parse("hyper_unique_add(long, string)", MACRO_TABLE);
      expr.eval(inputBindings);
    });
    assertTrue(exception.getMessage().contains("Function[hyper_unique_add] requires a hyper-log-log collector as the second argument"));
  }

  @Test
  public void testEstimateWrongArgsCount()
  {
    Throwable exception = assertThrows(IAE.class, () -> {
      Parser.parse("hyper_unique_estimate(hyper_unique(), 100)", MACRO_TABLE);
    });
    assertTrue(exception.getMessage().contains("Function[hyper_unique_estimate] requires 1 argument"));
  }

  @Test
  public void testEstimateWrongArgTypes()
  {
    Throwable exception = assertThrows(IAE.class, () -> {
      Expr expr = Parser.parse("hyper_unique_estimate(100)", MACRO_TABLE);
      expr.eval(inputBindings);
    });
    assertTrue(exception.getMessage().contains("Function[hyper_unique_estimate] requires a hyper-log-log collector as input"));
  }

  @Test
  public void testRoundEstimateWrongArgsCount()
  {
    Throwable exception = assertThrows(IAE.class, () -> {
      Parser.parse("hyper_unique_round_estimate(hyper_unique(), 100)", MACRO_TABLE);
    });
    assertTrue(exception.getMessage().contains("Function[hyper_unique_round_estimate] requires 1 argument"));
  }

  @Test
  public void testRoundEstimateWrongArgTypes()
  {
    Throwable exception = assertThrows(IAE.class, () -> {
      Expr expr = Parser.parse("hyper_unique_round_estimate(string)", MACRO_TABLE);
      expr.eval(inputBindings);
    });
    assertTrue(exception.getMessage().contains("Function[hyper_unique_round_estimate] requires a hyper-log-log collector as input"));
  }
}
