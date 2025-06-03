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

package org.apache.calcite.rel.rules;

import com.google.common.collect.Multimaps;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.Util;
import org.apache.curator.shaded.com.google.common.collect.Maps;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidException.Category;
import org.apache.druid.error.DruidException.Persona;
import org.immutables.value.Value;

import java.util.Collection;
import java.util.Map;

/**
 * Almost fully copied from Calcite - only to add a restriction because multiple {@link LogicalWindow} nodes may label outputs
 * similarily which could lead to incorrect results in Druid.
 */
public class DruidProjectToLogicalProjectAndWindowRule extends ProjectToWindowRule.ProjectToLogicalProjectAndWindowRule
{
  protected DruidProjectToLogicalProjectAndWindowRule(DruidProjectToLogicalProjectAndWindowRuleConfig config)
  {
    super(config);
  }

  @Value.Immutable
  public interface DruidProjectToLogicalProjectAndWindowRuleConfig extends ProjectToLogicalProjectAndWindowRuleConfig {
    DruidProjectToLogicalProjectAndWindowRuleConfig DEFAULT =
        ImmutableDruidProjectToLogicalProjectAndWindowRuleConfig.of()
            .withOperandSupplier(b ->
                b.operand(Project.class)
                    .predicate(Project::containsOver)
                    .anyInputs())
            .withDescription("DruidProjectToWindowRule:project");

    @Override default DruidProjectToLogicalProjectAndWindowRule toRule() {
      return new DruidProjectToLogicalProjectAndWindowRule(this);
    }
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Project project = call.rel(0);
    assert project.containsOver();
    final RelNode input = project.getInput();
    final RexProgram program =
        RexProgram.create(
            input.getRowType(),
            project.getProjects(),
            null,
            project.getRowType(),
            project.getCluster().getRexBuilder());
    // temporary LogicalCalc, never registered
    final LogicalCalc calc = LogicalCalc.create(input, program);
    final CalcRelSplitter transform =
        new WindowedAggRelSplitter(calc, call.builder()) {
          @Override protected RelNode handle(RelNode rel) {
            if (!(rel instanceof LogicalCalc)) {
              return rel;
            }
            final LogicalCalc calc = (LogicalCalc) rel;
            final RexProgram program = calc.getProgram();
            relBuilder.push(calc.getInput());
            if (program.getCondition() != null) {
              relBuilder.filter(
                  program.expandLocalRef(program.getCondition()));
            }
            if (!program.projectsOnlyIdentity()) {
              relBuilder.project(
                  Util.transform(program.getProjectList(),
                      program::expandLocalRef),
                  calc.getRowType().getFieldNames());
            }
            return relBuilder.build();
          }
        };
    RelNode newRel = transform.execute();
    validateResultRel(newRel);
    call.transformTo(newRel);
  }

  private void validateResultRel(RelNode newRel)
  {
    RelNode node = newRel.stripped();
    if(node instanceof Project) {
      Project project = (Project) newRel.stripped();
      validateResultRel(project.getInput());
    }
    RelDataType rowType = node.getRowType();

    Map<String, Collection<RelDataTypeField>> nameToFieldMap = Multimaps.index(rowType.getFieldList(), RelDataTypeField::getName).asMap();
    Map<String, Collection<RelDataTypeField>> nonUniqueFields = Maps        .filterValues(nameToFieldMap, values -> values.size() > 1);

    if(!nonUniqueFields.isEmpty()) {
      throw DruidException.forPersona(Persona.USER)
      .ofCategory(Category.RUNTIME_FAILURE)
      .build("Window expression field name reuse!", nonUniqueFields);
    }
  }
}
