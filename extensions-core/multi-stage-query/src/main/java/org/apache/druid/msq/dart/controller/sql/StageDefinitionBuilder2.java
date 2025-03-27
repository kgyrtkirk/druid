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

package org.apache.druid.msq.dart.controller.sql;

import org.apache.druid.msq.kernel.StageDefinitionBuilder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;
import org.apache.druid.sql.calcite.rel.Projection;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.rel.logical.DruidProject;

import java.util.Optional;

public class StageDefinitionBuilder2
{
  private StageDefinitionBuilder qdb;
  private Projection projection;

  public StageDefinitionBuilder2(StageDefinitionBuilder qdb)
  {
    this.qdb = qdb;
  }

  public StageDefinitionBuilder finalizeStage()
  {
    return qdb;
  }

  public Optional<StageDefinitionBuilder2> extendWith(DruidNodeStack stack)
  {

    if (stack.peekNode() instanceof DruidProject && projection == null) {

      DruidProject project = (DruidProject) stack.peekNode();
      VirtualColumnRegistry virtualColumnRegistry = null;
      RowSignature inputRowSignature = null;
      PlannerContext plannerContext = null;
      projection = Projection.preAggregation(project,plannerContext,inputRowSignature, virtualColumnRegistry);
    }
    return Optional.empty();
  }

}
