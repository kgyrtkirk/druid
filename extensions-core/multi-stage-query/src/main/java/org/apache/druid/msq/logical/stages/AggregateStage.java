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

package org.apache.druid.msq.logical.stages;

import org.apache.druid.msq.logical.StageMaker;
import org.apache.druid.msq.querykit.BaseFrameProcessorFactory;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;
import org.apache.druid.sql.calcite.rel.Grouping;

public class AggregateStage extends ProjectStage
{
  private Grouping grouping;

  public AggregateStage(ProjectStage projectStage, Grouping grouping)
  {
    super(projectStage);
    this.grouping = grouping;
  }

  @Override
  public LogicalStage extendWith(DruidNodeStack stack)
  {
    return null;
  }

  @Override
  public BaseFrameProcessorFactory buildFrameProcessor(StageMaker stageMaker)
  {
    return stageMaker.makeScanFrameProcessor(null, signature, dimFilter);
  }

  public static LogicalStage buildStages(ProjectStage projectStage, Grouping grouping)
  {
    AggregateStage aggStage = new AggregateStage(projectStage, grouping);
    SortStage sortStage = new SortStage(aggStage, grouping.getDimensions());
    AggregateStage finalAggStage = new AggregateStage(sortStage, grouping);

  }
}
