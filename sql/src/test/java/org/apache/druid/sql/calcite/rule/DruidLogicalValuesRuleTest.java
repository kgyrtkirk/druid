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

package org.apache.druid.sql.calcite.rule;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.math.BigDecimal;



public class DruidLogicalValuesRuleTest
{
  private static final PlannerContext DEFAULT_CONTEXT = Mockito.mock(PlannerContext.class);

  @Nested
  public class GetValueFromLiteralSimpleTypesTest extends InitializedNullHandlingTest
  {
    public static Iterable<Object[]> constructorFeeder()
    {
      return ImmutableList.of(
          new Object[]{"test", SqlTypeName.CHAR, String.class},
          new Object[]{"test", SqlTypeName.VARCHAR, String.class},
          new Object[]{0.1, SqlTypeName.DOUBLE, Double.class},
          new Object[]{0.1, SqlTypeName.REAL, Double.class},
          new Object[]{0.1, SqlTypeName.DECIMAL, Double.class},
          new Object[]{1L, SqlTypeName.TINYINT, Long.class},
          new Object[]{1L, SqlTypeName.SMALLINT, Long.class},
          new Object[]{1L, SqlTypeName.INTEGER, Long.class},
          new Object[]{1L, SqlTypeName.BIGINT, Long.class}
      );
    }

    private Comparable<?> val;
    private SqlTypeName sqlTypeName;
    private Class<?> javaType;

    public void initGetValueFromLiteralSimpleTypesTest(Comparable<?> val, SqlTypeName sqlTypeName, Class<?> javaType)
    {
      this.val = val;
      this.sqlTypeName = sqlTypeName;
      this.javaType = javaType;
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "{1}, {2}")
    public void testGetValueFromLiteral(Comparable<?> val, SqlTypeName sqlTypeName, Class<?> javaType)
    {
      initGetValueFromLiteralSimpleTypesTest(val, sqlTypeName, javaType);
      final RexLiteral literal = Mockito.spy(makeLiteral(val, sqlTypeName, javaType));
      final Object fromLiteral = DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
      Assertions.assertSame(javaType, fromLiteral.getClass());
      Assertions.assertEquals(val, fromLiteral);
      Mockito.verify(literal, Mockito.times(1)).getType();
    }

    private static RexLiteral makeLiteral(Comparable<?> val, SqlTypeName typeName, Class<?> javaType)
    {
      return (RexLiteral) new RexBuilder(DruidTypeSystem.TYPE_FACTORY).makeLiteral(
          typeName == SqlTypeName.DECIMAL && val != null ? new BigDecimal(String.valueOf(val)) : val,
          DruidTypeSystem.TYPE_FACTORY.createSqlType(typeName),
          false
      );
    }
  }

  @Nested
  public class GetValueFromLiteralOtherTypesTest
  {
    private static final PlannerContext DEFAULT_CONTEXT = Mockito.mock(PlannerContext.class);
    private static final DateTimeZone TIME_ZONE = DateTimes.inferTzFromString("Asia/Seoul");
    private static final RelDataTypeFactory TYPE_FACTORY = new SqlTypeFactoryImpl(DruidTypeSystem.INSTANCE);
    private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    @BeforeAll
    public static void setup()
    {
      Mockito.when(DEFAULT_CONTEXT.getTimeZone()).thenReturn(TIME_ZONE);
    }

    @Test
    public void testGetValueFromTrueLiteral()
    {
      RexLiteral literal = REX_BUILDER.makeLiteral(true);

      final Object fromLiteral = DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
      Assertions.assertSame(Long.class, fromLiteral.getClass());
      Assertions.assertEquals(1L, fromLiteral);
    }

    @Test
    public void testGetValueFromFalseLiteral()
    {
      RexLiteral literal = REX_BUILDER.makeLiteral(false);

      final Object fromLiteral = DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
      Assertions.assertSame(Long.class, fromLiteral.getClass());
      Assertions.assertEquals(0L, fromLiteral);
    }

    @Test
    public void testGetValueFromNullBooleanLiteral()
    {
      RexLiteral literal = REX_BUILDER.makeLiteral(null, REX_BUILDER.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN));

      if (NullHandling.sqlCompatible() && ExpressionProcessing.useStrictBooleans()) {
        final Object fromLiteral = DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
        Assertions.assertNull(fromLiteral);
      } else {
        final Object fromLiteralNonStrict = DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
        Assertions.assertSame(Long.class, fromLiteralNonStrict.getClass());
        Assertions.assertEquals(0L, fromLiteralNonStrict);
      }
    }

    @Test
    public void testGetValueFromTimestampLiteral()
    {
      RexLiteral literal = REX_BUILDER.makeTimestampLiteral(new TimestampString("2021-04-01 16:54:31"), 0);

      final Object fromLiteral = DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
      Assertions.assertSame(Long.class, fromLiteral.getClass());
      Assertions.assertEquals(new DateTime("2021-04-01T16:54:31", TIME_ZONE).getMillis(), fromLiteral);
    }

    @Test
    public void testGetValueFromDateLiteral()
    {
      RexLiteral literal = REX_BUILDER.makeDateLiteral(new DateString("2021-04-01"));

      final Object fromLiteral = DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
      Assertions.assertSame(Long.class, fromLiteral.getClass());
      Assertions.assertEquals(new DateTime("2021-04-01", TIME_ZONE).getMillis(), fromLiteral);
    }

    @Test
    public void testGetValueFromTimestampWithLocalTimeZoneLiteral()
    {
      RexLiteral literal = REX_BUILDER.makeTimestampWithLocalTimeZoneLiteral(
          new TimestampString("2021-04-01 16:54:31"),
          0
      );
      expectedException.expect(
          DruidExceptionMatcher
              .invalidSqlInput()
              .expectMessageIs(
                  "Cannot handle literal [2021-04-01 16:54:31:TIMESTAMP_WITH_LOCAL_TIME_ZONE(0)] "
                  + "of unsupported type [TIMESTAMP_WITH_LOCAL_TIME_ZONE]."
              )
      );
      DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
    }

    @Test
    public void testGetValueFromTimeLiteral()
    {
      RexLiteral literal = REX_BUILDER.makeTimeLiteral(new TimeString("16:54:31"), 0);
      expectedException.expect(
          DruidExceptionMatcher
              .invalidSqlInput()
              .expectMessageIs("Cannot handle literal [16:54:31] of unsupported type [TIME].")
      );
      DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
    }

    @Test
    public void testGetValueFromTimeWithLocalTimeZoneLiteral()
    {
      RexLiteral literal = REX_BUILDER.makeTimeWithLocalTimeZoneLiteral(new TimeString("16:54:31"), 0);
      expectedException.expect(
          DruidExceptionMatcher
              .invalidSqlInput()
              .expectMessageIs(
                  "Cannot handle literal [16:54:31:TIME_WITH_LOCAL_TIME_ZONE(0)] "
                  + "of unsupported type [TIME_WITH_LOCAL_TIME_ZONE]."
              )
      );
      DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
    }

    @Test
    public void testGetCastedValuesFromFloatToNumeric()
    {
      RexLiteral literal = REX_BUILDER.makeExactLiteral(
          new BigDecimal("123.0"),
          TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)
      );
      Object value = DruidLogicalValuesRule.getValueFromLiteral(literal, DEFAULT_CONTEXT);
      Assertions.assertEquals(value, 123L);
    }
  }
}
