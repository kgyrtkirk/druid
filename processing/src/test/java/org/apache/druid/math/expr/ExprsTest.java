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
import org.apache.druid.segment.join.Equality;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;

public class ExprsTest
{
  @Test
  public void test_decomposeAnd_notAnAnd()
  {
    final List<Expr> decomposed = Exprs.decomposeAnd(new IdentifierExpr("foo"));

    // Expr instances don't, in general, implement value-based equals and hashCode. So we need to verify each field.
    Assertions.assertEquals(1, decomposed.size());
    assertThat(decomposed.get(0), CoreMatchers.instanceOf(IdentifierExpr.class));
    Assertions.assertEquals("foo", ((IdentifierExpr) decomposed.get(0)).getIdentifier());
  }

  @Test
  public void test_decomposeAnd_basic()
  {
    final List<Expr> decomposed = Exprs.decomposeAnd(
        new BinAndExpr(
            "&&",
            new BinAndExpr("&&", new IdentifierExpr("foo"), new IdentifierExpr("bar")),
            new BinAndExpr("&&", new IdentifierExpr("baz"), new IdentifierExpr("qux"))
        )
    );

    // Expr instances don't, in general, implement value-based equals and hashCode. So we need to verify each field.
    Assertions.assertEquals(4, decomposed.size());

    for (Expr expr : decomposed) {
      assertThat(expr, CoreMatchers.instanceOf(IdentifierExpr.class));
    }

    final List<String> identifiers = decomposed.stream()
                                               .map(expr -> ((IdentifierExpr) expr).getIdentifier())
                                               .collect(Collectors.toList());

    Assertions.assertEquals(
        ImmutableList.of("foo", "bar", "baz", "qux"),
        identifiers
    );
  }

  @Test
  public void test_decomposeEquals_notAnEquals()
  {
    final Optional<Equality> result = Exprs.decomposeEquals(new IdentifierExpr("foo"), "j.");
    Assertions.assertFalse(result.isPresent());
  }

  @Test
  public void test_decomposeEquals_basic()
  {
    final Optional<Equality> result = Exprs.decomposeEquals(
        new BinEqExpr(
            "==",
            new IdentifierExpr("foo"),
            new IdentifierExpr("j.bar")
        ),
        "j."
    );

    Assertions.assertTrue(result.isPresent());

    final Equality equality = result.get();
    assertThat(equality.getLeftExpr(), CoreMatchers.instanceOf(IdentifierExpr.class));
    Assertions.assertEquals("foo", ((IdentifierExpr) equality.getLeftExpr()).getIdentifier());
    Assertions.assertEquals("bar", equality.getRightColumn());
    Assertions.assertFalse(equality.isIncludeNull());
  }

  @Test
  public void test_decomposeEquals_notDistinctFrom()
  {
    final Optional<Equality> result = Exprs.decomposeEquals(
        new FunctionExpr(
            new Function.IsNotDistinctFromFunc(),
            "notdistinctfrom",
            ImmutableList.of(
                new IdentifierExpr("foo"),
                new IdentifierExpr("j.bar")
            )
        ),
        "j."
    );

    Assertions.assertTrue(result.isPresent());

    final Equality equality = result.get();
    assertThat(equality.getLeftExpr(), CoreMatchers.instanceOf(IdentifierExpr.class));
    Assertions.assertEquals("foo", ((IdentifierExpr) equality.getLeftExpr()).getIdentifier());
    Assertions.assertEquals("bar", equality.getRightColumn());
    Assertions.assertTrue(equality.isIncludeNull());
  }
}
