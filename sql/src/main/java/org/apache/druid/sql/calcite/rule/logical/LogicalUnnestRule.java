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

import com.google.common.collect.Iterables;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.druid.error.DruidException;

public class LogicalUnnestRule extends RelOptRule implements SubstitutionRule
{
  public LogicalUnnestRule()
  {
    super(
        operand(
            LogicalCorrelate.class,
            some(
                operand(RelNode.class, any()),
                operand(RelNode.class, any())
            )
        )
    );
  }

  @Override
  public boolean autoPruneOld()
  {
    return true;
  }

  public boolean matches1(RelOptRuleCall call)
  {
    LogicalCorrelate cor = call.rel(0);
    RexNode expr = unwrapUnnestExpression(cor.getRight());
    return expr != null;
  }

  public void onMatch(RelOptRuleCall call)
  {
    LogicalCorrelate cor = call.rel(0);
    RelNode left = call.rel(1);
    RexNode expr = unwrapUnnestExpression(cor.getRight().stripped());
    if (expr == null) {
      throw DruidException.defensive("Couldn't process possible unnest for reltree: \n%s", RelOptUtil.toString(cor));
    }

    RelBuilder builder = call.builder();
    builder.push(left);
    RelNode newNode = builder.push(
        new LogicalUnnest(
            cor.getCluster(),
            cor.getTraitSet(),
            builder.build(),
            expr,
            cor.getRowType()
        )
    ).build();
    call.transformTo(newNode);
  }

  private RexNode unwrapUnnestExpression(RelNode rel)
  {
    RelNode node = rel.stripped();
    if (node instanceof Uncollect) {
      Uncollect uncollect = (Uncollect) node;
      if (!uncollect.withOrdinality) {
        return unwrapProjectExpression(uncollect.getInput().stripped());
      }
    }
    return null;
  }

  private RexNode unwrapProjectExpression(RelNode rel)
  {
    if (rel instanceof Project) {
      Project project = (Project) rel;
      if (isValues(project.getInput().stripped())) {
        return Iterables.getOnlyElement(project.getProjects());
      }
    }
    return null;
  }

  private boolean isValues(RelNode input)
  {
    return (input instanceof LogicalValues);
  }

}
