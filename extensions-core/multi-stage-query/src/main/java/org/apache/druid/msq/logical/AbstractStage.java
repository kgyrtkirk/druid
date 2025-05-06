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

package org.apache.druid.msq.logical;

import org.apache.calcite.plan.Context;
import org.apache.druid.error.DruidException;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.query.QueryContext;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;

import java.util.ArrayList;
import java.util.List;

public class AbstractStage implements LogicalStage
{
  protected final List<InputSpec> inputSpecs;
  protected final RowSignature signature;
  protected final List<LogicalStage> inputStages;
  protected final Context ctx;

  public AbstractStage(Context ctx, RowSignature signature, List<InputSpec> inputs, List<LogicalStage> inputStages)
  {
    this.ctx = ctx;
    this.inputSpecs = inputs;
    this.signature = signature;
    this.inputStages = inputStages;
  }

  @Override
  public StageDefinition finalizeStage()
  {
    throw DruidException.defensive("This should have been implemented - or not reach this point!");
  }

  @Override
  public LogicalStage extendWith(DruidNodeStack stack)
  {
    return null;
  }

  @Override
  public final QueryDefinition build()
  {
    return QueryDefinition.create(buildStageDefinitions(), getQueryContext());
  }

  @Override
  public final List<StageDefinition> buildStageDefinitions()
  {
    List<StageDefinition> ret = new ArrayList<>();
    for (LogicalStage vertex : inputStages) {
      ret.addAll(vertex.buildStageDefinitions());
    }
    ret.add(finalizeStage());
    return ret;
  }

  protected final QueryContext getQueryContext()
  {
    return getPlannerContext().queryContext();
  }

  protected final PlannerContext getPlannerContext()
  {
    return ctx.unwrapOrThrow(PlannerContext.class);
  }
}