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

package org.apache.druid.query.planning;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PreJoinableClauseTest
{
  private final PreJoinableClause clause = new PreJoinableClause(
      "j.",
      new TableDataSource("foo"),
      JoinType.LEFT,
      JoinConditionAnalysis.forExpression("x == \"j.x\"", "j.", ExprMacroTable.nil())
  );

  @Test
  public void test_getPrefix()
  {
    Assertions.assertEquals("j.", clause.getPrefix());
  }

  @Test
  public void test_getJoinType()
  {
    Assertions.assertEquals(JoinType.LEFT, clause.getJoinType());
  }

  @Test
  public void test_getCondition()
  {
    Assertions.assertEquals("x == \"j.x\"", clause.getCondition().getOriginalExpression());
  }

  @Test
  public void test_getDataSource()
  {
    Assertions.assertEquals(new TableDataSource("foo"), clause.getDataSource());
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(PreJoinableClause.class)
                  .usingGetClass()
                  .withNonnullFields("prefix", "dataSource", "joinType", "condition")
                  .verify();
  }
}
