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

import org.apache.druid.msq.exec.StageProcessor;
import org.apache.druid.msq.logical.LogicalInputSpec;
import org.apache.druid.msq.logical.StageMaker;
import org.apache.druid.msq.querykit.groupby.GroupByPostShuffleStageProcessor;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;

class GroupByPostShuffleStage extends AbstractFrameProcessorStage
{
  // FIXME query
  private GroupByQuery gby;

  public GroupByPostShuffleStage(LogicalStage inputStage, GroupByQuery gby)
  {
    super(inputStage.getRowSignature(), LogicalInputSpec.of(inputStage));
    this.gby = gby;
  }

  @Override
  public LogicalStage extendWith(DruidNodeStack stack)
  {
    return null;
  }

  @Override
  public StageProcessor<?, ?> buildStageProcessor(StageMaker stageMaker)
  {
    return new GroupByPostShuffleStageProcessor(gby);
  }
}