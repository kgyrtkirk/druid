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

package org.apache.druid.sql.calcite.expression;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.builtin.LeastOperatorConversion;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class LeastExpressionTest extends CalciteTestBase
{
  private static final String DOUBLE_KEY = "d";
  private static final double DOUBLE_VALUE = 3.1;
  private static final String LONG_KEY = "l";
  private static final long LONG_VALUE = 2L;
  private static final String STRING_KEY = "s";
  private static final String STRING_VALUE = "foo";
  private static final RowSignature ROW_SIGNATURE = RowSignature
      .builder()
      .add(DOUBLE_KEY, ColumnType.DOUBLE)
      .add(LONG_KEY, ColumnType.LONG)
      .add(STRING_KEY, ColumnType.STRING)
      .build();
  private static final Map<String, Object> BINDINGS = ImmutableMap.of(
      DOUBLE_KEY, DOUBLE_VALUE,
      LONG_KEY, LONG_VALUE,
      STRING_KEY, STRING_VALUE
  );

  private LeastOperatorConversion target;
  private ExpressionTestHelper testHelper;

  @BeforeEach
  void setUp()
  {
    target = new LeastOperatorConversion();
    testHelper = new ExpressionTestHelper(ROW_SIGNATURE, BINDINGS);
  }

  @Test
  void noArgs()
  {
    testExpression(
        Collections.emptyList(),
        buildExpectedExpression(),
        null
    );
  }

  @Test
  void allNull()
  {
    testExpression(
        Arrays.asList(
            testHelper.getConstantNull(),
            testHelper.getConstantNull()
        ),
        buildExpectedExpression(null, null),
        null
    );
  }

  @Test
  void someNull()
  {
    testExpression(
        Arrays.asList(
            testHelper.makeInputRef(DOUBLE_KEY),
            testHelper.getConstantNull(),
            testHelper.makeInputRef(STRING_KEY)
        ),
        buildExpectedExpression(
            testHelper.makeVariable(DOUBLE_KEY),
            null,
            testHelper.makeVariable(STRING_KEY)
        ),
        String.valueOf(DOUBLE_VALUE)
    );
  }

  @Test
  void allDouble()
  {
    testExpression(
        Arrays.asList(
            testHelper.makeLiteral(34.1),
            testHelper.makeInputRef(DOUBLE_KEY),
            testHelper.makeLiteral(5.2),
            testHelper.makeLiteral(767.3)
        ),
        buildExpectedExpression(
            34.1,
            testHelper.makeVariable(DOUBLE_KEY),
            5.2,
            767.3
        ),
        3.1
    );
  }

  @Test
  void allLong()
  {
    testExpression(
        Arrays.asList(
            testHelper.makeInputRef(LONG_KEY),
            testHelper.makeLiteral(0)
        ),
        buildExpectedExpression(
            testHelper.makeVariable(LONG_KEY),
            0
        ),
        0L
    );
  }

  @Test
  void allString()
  {
    testExpression(
        Arrays.asList(
            testHelper.makeLiteral("B"),
            testHelper.makeInputRef(STRING_KEY),
            testHelper.makeLiteral("A")
        ),
        buildExpectedExpression(
            "B",
            testHelper.makeVariable(STRING_KEY),
            "A"
        ),
        "A"
    );
  }

  @Test
  void coerceString()
  {
    testExpression(
        Arrays.asList(
            testHelper.makeLiteral(-1),
            testHelper.makeInputRef(DOUBLE_KEY),
            testHelper.makeLiteral("A")
        ),
        buildExpectedExpression(
            -1,
            testHelper.makeVariable(DOUBLE_KEY),
            "A"
        ),
        "-1"
    );
  }

  @Test
  void coerceDouble()
  {
    testExpression(
        Arrays.asList(
            testHelper.makeLiteral(-1),
            testHelper.makeInputRef(DOUBLE_KEY)
        ),
        buildExpectedExpression(
            -1,
            testHelper.makeVariable(DOUBLE_KEY)
        ),
        -1.0
    );
  }

  @Test
  void decimal()
  {
    testExpression(
        Arrays.asList(
            testHelper.makeLiteral(BigDecimal.valueOf(1.2)),
            testHelper.makeLiteral(BigDecimal.valueOf(3.4))
        ),
        buildExpectedExpression(
            1.2,
            3.4
        ),
        1.2
    );
  }

  @Test
  void decimalWithNullShouldReturnString()
  {
    testExpression(
        Arrays.asList(
            testHelper.makeLiteral(BigDecimal.valueOf(1.2)),
            testHelper.makeLiteral(BigDecimal.valueOf(3.4)),
            testHelper.getConstantNull()
        ),
        buildExpectedExpression(
            1.2,
            3.4,
            null
        ),
        "1.2"
    );
  }

  @Test
  void timestamp()
  {
    testExpression(
        Arrays.asList(
            testHelper.makeLiteral(DateTimes.utc(1000)),
            testHelper.makeLiteral(DateTimes.utc(2000))
        ),
        buildExpectedExpression(
            1000,
            2000
        ),
        1000L
    );
  }

  @Test
  void intervalYearMonth()
  {
    testExpression(
        Collections.singletonList(
            testHelper.makeLiteral(
                new BigDecimal(13), // YEAR-MONTH literals value is months
                new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, SqlParserPos.ZERO)
            )
        ),
        buildExpectedExpression(13),
        13L
    );
  }

  private void testExpression(
      List<? extends RexNode> exprs,
      final DruidExpression expectedExpression,
      final Object expectedResult
  )
  {
    testHelper.testExpressionString(target.calciteOperator(), exprs, expectedExpression, expectedResult);
  }

  private DruidExpression buildExpectedExpression(Object... args)
  {
    return testHelper.buildExpectedExpression(target.getDruidFunctionName(), args);
  }
}
