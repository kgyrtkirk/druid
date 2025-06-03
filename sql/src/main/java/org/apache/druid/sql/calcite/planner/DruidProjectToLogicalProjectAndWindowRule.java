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

import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.immutables.value.Value;

public class DruidProjectToLogicalProjectAndWindowRule extends ProjectToWindowRule.ProjectToLogicalProjectAndWindowRule
{
  protected DruidProjectToLogicalProjectAndWindowRule(DruidProjectToLogicalProjectAndWindowRuleConfig config)
  {
    super(config);
  }

  @Value.Immutable
  public interface DruidProjectToLogicalProjectAndWindowRuleConfig extends ProjectToLogicalProjectAndWindowRuleConfig {
    DruidProjectToLogicalProjectAndWindowRuleConfig DEFAULT =
        ImmutableDruidProjectToLogicalProjectAndWindowRuleConfig.builder()
            .operandSupplier(b ->
                b.operand(Project.class)
                    .predicate(Project::containsOver)
                    .anyInputs())
            .description("DruidProjectToWindowRule:project")
            .build();

    @Override default DruidProjectToLogicalProjectAndWindowRule toRule() {
      return new DruidProjectToLogicalProjectAndWindowRule(this);
    }

  }
}
