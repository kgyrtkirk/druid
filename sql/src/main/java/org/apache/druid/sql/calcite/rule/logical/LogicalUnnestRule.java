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

package org.apache.druid.sql.calcite.rule.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexNode;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalConvention;
import org.checkerframework.checker.nullness.qual.Nullable;

public class LogicalUnnestRule extends RelOptRule implements SubstitutionRule
{
  public LogicalUnnestRule()
  {
    super(operand(LogicalCorrelate.class, any()));
  }

  public @Nullable RelNode convert(RelNode rel)
  {
    LogicalCorrelate join = (LogicalCorrelate) rel;

    RelTraitSet newTrait = join.getTraitSet().replace(DruidLogicalConvention.instance());

    return new DruidLogicalCorrelate(
        join.getCluster(),
        newTrait,
        join.getHints(),
        convert(
            join.getLeft(),
            DruidLogicalConvention.instance()
        ),
        convert(
            join.getRight(),
            DruidLogicalConvention.instance()
        ),
        join.getCorrelationId(),
        join.getRequiredColumns(),
        join.getJoinType()
    );
  }

  @Override
  public boolean autoPruneOld()
  {
    return true;
  }

  @Override
  public boolean matches(RelOptRuleCall call)
  {
    RelOptRuleOperand a = operand(
        Uncollect.class,
        any()
    );

    LogicalCorrelate cor = call.rel(0);
    a.matches(cor);

    RexNode expr = unwrapUnnestExpression(cor.getRight());
    return false;

  }

  /**
   *
   * @param rel
   * @return
   */
  private RexNode unwrapUnnestExpression(RelNode rel)
  {
    // RelNode node = rel.stripped();
    // if (node instanceof Uncollect) {
    // Uncollect uncollect = (Uncollect) node;
    // if(!uncollect.withOrdinality) {
    // return
    // }
    // uncollect.
    // return
    // }
    return null;
  }

  public void onMatch(RelOptRuleCall call)
  {
    LogicalCorrelate cor = call.rel(0);

  }
}
