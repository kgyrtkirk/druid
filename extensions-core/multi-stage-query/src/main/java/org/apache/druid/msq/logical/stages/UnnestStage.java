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
import org.apache.druid.query.DataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;
import org.apache.druid.sql.calcite.planner.querygen.SourceDescProducer.SourceDesc;
import org.apache.druid.sql.calcite.rule.logical.DruidUnnest;

import javax.annotation.Nullable;

import java.util.Collections;

public class UnnestStage extends AbstractFrameProcessorStage
{
  private static final DataSource DUYMMY = new TableDataSource("__dummy__");
  private final VirtualColumn virtualColumn;
  @Nullable
  private final DimFilter filter;

  public UnnestStage(RowSignature signature, LogicalInputSpec input, VirtualColumn virtualColumn,
      DimFilter filter)
  {
    super(signature, input);
    this.virtualColumn = virtualColumn;
    this.filter = filter;
  }

  @Override
  public LogicalStage extendWith(DruidNodeStack stack)
  {
    return null;
  }

  @Override
  public StageProcessor<?, ?> buildStageProcessor(StageMaker stageMaker)
  {
    UnnestDataSource unnestDataSource = UnnestDataSource.create(DUYMMY, virtualColumn, filter);
    return stageMaker.makeSegmentMapProcessor(signature, unnestDataSource);
  }


  public static class SegmentMapStage extends AbstractFrameProcessorStage
  {
    private SourceDesc sourceDesc;

    public SegmentMapStage(LogicalStage inputStage, SourceDesc sourceDesc)
    {
      super(sourceDesc.rowSignature, LogicalInputSpec.of(inputStage));
      this.sourceDesc = sourceDesc;
    }

    @Override
    public LogicalStage extendWith(DruidNodeStack stack)
    {
      return null;
    }

    @Override
    public StageProcessor<?, ?> buildStageProcessor(StageMaker stageMaker)
    {
      return stageMaker.makeSegmentMapProcessor(signature, sourceDesc.dataSource);
    }

  }

  public static LogicalStage buildUnnestStage(LogicalStage inputStage, DruidNodeStack stack)
  {


    DruidUnnest unnest = (DruidUnnest) stack.getNode();

    PlannerContext plannerContext=stack.getPlannerContext();
    SourceDesc sourceDesc = makeDummySourceDesc(inputStage);
    SourceDesc unnestSD = unnest.getSourceDesc(plannerContext, Collections.singletonList(sourceDesc));


    return new SegmentMapStage(inputStage, unnestSD);
//    return new UnnestStage(unnestSD.rowSignature, inputStage, virtualColumn, filter);

  }

  private static SourceDesc makeDummySourceDesc(LogicalStage inputStage)
  {
    return new SourceDesc(DUYMMY, inputStage.getRowSignature());
  }

}
