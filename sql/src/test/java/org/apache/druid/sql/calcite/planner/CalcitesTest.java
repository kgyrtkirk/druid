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

package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableSortedSet;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CalcitesTest extends CalciteTestBase
{
  @Test
  public void testEscapeStringLiteral()
  {
    Assertions.assertEquals("''", Calcites.escapeStringLiteral(""));
    Assertions.assertEquals("'foo'", Calcites.escapeStringLiteral("foo"));
    Assertions.assertEquals("'foo bar'", Calcites.escapeStringLiteral("foo bar"));
    Assertions.assertEquals("U&'foö bar'", Calcites.escapeStringLiteral("foö bar"));
    Assertions.assertEquals("U&'foo \\0026\\0026 bar'", Calcites.escapeStringLiteral("foo && bar"));
    Assertions.assertEquals("U&'foo \\005C bar'", Calcites.escapeStringLiteral("foo \\ bar"));
    Assertions.assertEquals("U&'foo\\0027s bar'", Calcites.escapeStringLiteral("foo's bar"));
    Assertions.assertEquals("U&'друид'", Calcites.escapeStringLiteral("друид"));
  }

  @Test
  public void testFindUnusedPrefix()
  {
    Assertions.assertEquals("x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "bar")));
    Assertions.assertEquals("x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "bar", "x")));
    Assertions.assertEquals("_x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "bar", "x0")));
    Assertions.assertEquals("_x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "bar", "x4")));
    Assertions.assertEquals("__x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "xa", "_x2xx", "x0")));
    Assertions.assertEquals("x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "xa", "_x2xx", " x")));
    Assertions.assertEquals("x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "_xbxx")));
    Assertions.assertEquals("x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "xa", "_x")));
    Assertions.assertEquals("__x", Calcites.findUnusedPrefixForDigits("x", ImmutableSortedSet.of("foo", "x1a", "_x90")));
  }

  @Test
  public void testGetStringComparatorForColumnType()
  {
    Assertions.assertEquals(StringComparators.LEXICOGRAPHIC, Calcites.getStringComparatorForValueType(ColumnType.STRING));
    Assertions.assertEquals(StringComparators.NUMERIC, Calcites.getStringComparatorForValueType(ColumnType.LONG));
    Assertions.assertEquals(StringComparators.NUMERIC, Calcites.getStringComparatorForValueType(ColumnType.FLOAT));
    Assertions.assertEquals(StringComparators.NUMERIC, Calcites.getStringComparatorForValueType(ColumnType.DOUBLE));
    Assertions.assertEquals(StringComparators.NATURAL, Calcites.getStringComparatorForValueType(ColumnType.STRING_ARRAY));
    Assertions.assertEquals(StringComparators.NATURAL, Calcites.getStringComparatorForValueType(ColumnType.LONG_ARRAY));
    Assertions.assertEquals(StringComparators.NATURAL, Calcites.getStringComparatorForValueType(ColumnType.DOUBLE_ARRAY));
    Assertions.assertEquals(StringComparators.NATURAL, Calcites.getStringComparatorForValueType(ColumnType.NESTED_DATA));
    Assertions.assertEquals(StringComparators.NATURAL, Calcites.getStringComparatorForValueType(ColumnType.UNKNOWN_COMPLEX));
  }
}
