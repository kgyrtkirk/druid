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
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.druid.error.DruidException;
import org.apache.druid.sql.calcite.expression.builtin.MultiValueStringToArrayOperatorConversion;
import java.util.ArrayList;
import java.util.List;

/**
 * Makes tweaks to LogicalUnnest input.
 *
 * Removes any MV_TO_ARRAY call if its present for the input of the
 * {@link LogicalUnnest}.
 *
 */
public class UnnestInputCleanupRule extends RelOptRule implements SubstitutionRule
{
  public UnnestInputCleanupRule()
  {
    super(
        operand(
            LogicalUnnest.class,
            operand(Project.class, any())
        )
    );
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    LogicalUnnest unnest = call.rel(0);
    Project project = call.rel(1);

    ImmutableBitSet input = InputFinder.analyze(unnest.unnestExpr).build();
    if (input.isEmpty()) {
      throw DruidException.defensive("Found an unbound unnest expression.");
    }
    int inputIndex = input.nextSetBit(0);

    List<RexNode> projects = new ArrayList<>(project.getProjects());
    RexNode unnestInput = projects.get(inputIndex);

    if(!(unnest.unnestExpr instanceof RexInputRef)) {
      return;
    }
    if((unnestInput instanceof RexInputRef)) {
      return;
    }

    if (inputIndex != projects.size() - 1) {
      return;
    }

    // Replace the unnest expression reference with an inputRef for #0
    // this will enable all other parts to see this Project as a mapping; which
    // will result in its removal (if empty).
    projects.set(inputIndex, call.builder().getRexBuilder().makeInputRef(project.getInput(), 0));

    RelNode newInputRel = call.builder()
        .push(project.getInput())
        .project(projects)
        .build();

    RexNode newUnnestExpr = unnestInput;
    RexNode newConditionExpr = null;

    if (unnest.condition != null) {
      // FIXME; think this thru
      return;

//      ImmutableBitSet input1 = InputFinder.analyze(unnest.condition).build();
//      if(input1.nextSetBit(inputIndex) == -1) {
//        // condition references at least the unwrapped
//        return;
//      }
//      newConditionExpr = RelOptUtil.pushPastProject(unnest.condition, tmpProject);
    }



    RelNode newUnnest = new LogicalUnnest(
        unnest.getCluster(), unnest.getTraitSet(), newInputRel, newUnnestExpr,
        unnest.getRowType(), newConditionExpr
    );
    call.transformTo(newUnnest);
    call.getPlanner().prune(unnest);

  }

  /**
   * Whether an expr is MV_TO_ARRAY of an input reference.
   */
  private static boolean isMvToArrayOfInputRef(final RexNode expr)
  {
    return expr.isA(SqlKind.OTHER_FUNCTION)
        && ((RexCall) expr).op.equals(MultiValueStringToArrayOperatorConversion.SQL_FUNCTION)
        && ((RexCall) expr).getOperands().get(0).isA(SqlKind.INPUT_REF);
  }

  /**
   * Unwrap MV_TO_ARRAY at the outer layer of an expr, if it refers to an input
   * ref.
   *
   * @param rexBuilder
   */
  public static RexNode unwrapMvToArray(RexBuilder rexBuilder, final RexNode expr)
  {
    if (isMvToArrayOfInputRef(expr)) {
      return ((RexCall) expr).getOperands().get(0);
    } else {
      return expr;
    }
  }

}
