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

import org.apache.druid.error.DruidException;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.kernel.MixShuffleSpec;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageDefinitionBuilder;
import org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessorFactory;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;
import org.apache.druid.sql.calcite.rel.Projection;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.rel.logical.DruidProject;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

//@Value.Immutable
public class StageDefinitionBuilder2
{
  private static AtomicInteger stageIdSeq = new AtomicInteger(1);
  private PlannerContext plannerContext;

  public class AbstractStage implements IStageDef {

    protected final List<InputSpec> inputs;
    protected final RowSignature signature;

    public AbstractStage(RowSignature signature, List<InputSpec> inputs)
    {
      this.inputs = inputs;
      this.signature = signature;
    }

    @Override
    public StageDefinitionBuilder finalizeStage()
    {
      throw DruidException.defensive("This should have been implemented - or not reach this point!");
    }

    @Override
    public IStageDef extendWith(DruidNodeStack stack)
    {
      return null;
    }
  }

  public class RootStage extends AbstractStage   {

    public RootStage(RowSignature signature, List<InputSpec> inputs)
    {
      super(signature, inputs);
    }

    public RootStage(RootStage root, RowSignature signature1)
    {
      super(signature1 , root.inputs);
    }

    @Override
    public StageDefinitionBuilder finalizeStage()
    {
      StageDefinitionBuilder sdb = StageDefinition.builder(stageIdSeq.incrementAndGet())
          .inputs(inputs)
          .signature(signature)
          .shuffleSpec(MixShuffleSpec.instance())
          .processorFactory(makeScanProcessorFactory(null, signature));
      return sdb;
    }

    @Override
    public IStageDef extendWith(DruidNodeStack stack)
    {
//      if (stack.peekNode() instanceof DruidFilter) {
//
//
//        DruidFilter druidFilter = (DruidFilter) stack.peekNode();
//        FilteredStageDefinition filteredStage = FilteredStageDefinition.create(this, druidFilter);
//
//        return filteredStage;
//        DruidFilter filter = (DruidFilter) stack.peekNode();
//        VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
//            signature,
//            plannerContext.getExpressionParser(),
//            plannerContext.getPlannerConfig().isForceExpressionVirtualColumns()
//        );
//
//        DimFilter dimFilter = DruidQuery.getDimFilter(plannerContext, signature, virtualColumnRegistry, filter);
//
//        return new FilteredStageDefinition(
//            this,
//            virtualColumnRegistry.build(Collections.emptySet()),
//            preAggregation.getOutputRowSignature()
//        );
//      }

      if (stack.peekNode() instanceof DruidProject) {
        DruidProject project = (DruidProject) stack.peekNode();
        VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
            signature,
            plannerContext.getExpressionParser(),
            plannerContext.getPlannerConfig().isForceExpressionVirtualColumns()
        );
        Projection preAggregation = Projection.preAggregation(project, plannerContext, signature, virtualColumnRegistry);

        return new ProjectStageDefinition(
            this,
            virtualColumnRegistry.build(Collections.emptySet()),
            preAggregation.getOutputRowSignature()
        );
      }
      return null;
    }
  }

  public StageDefinitionBuilder2(PlannerContext plannerContext2)
  {
    this.plannerContext = plannerContext2;
  }

  private static final String IRRELEVANT = "irrelevant";


  private ScanQueryFrameProcessorFactory makeScanProcessorFactory(DataSource dataSource, RowSignature rowSignature)
  {
    ScanQuery s = Druids.newScanQueryBuilder()
        .dataSource(IRRELEVANT)
        .intervals(QuerySegmentSpec.DEFAULT)
        .columns(rowSignature.getColumnNames())
        .columnTypes(rowSignature.getColumnTypes())
        .build();

    return new ScanQueryFrameProcessorFactory(s);
  }



  class ProjectStageDefinition extends RootStage
  {
    private VirtualColumns virtualColumns;

    public ProjectStageDefinition(RootStage root, VirtualColumns virtualColumns2, RowSignature rowSignature)
    {
      super(root,rowSignature);
      this.virtualColumns = virtualColumns2;
    }

    @Override
    public StageDefinitionBuilder finalizeStage()
    {
      StageDefinitionBuilder sdb = StageDefinition.builder(stageIdSeq.incrementAndGet())
          .inputs(inputs)
          .signature(signature)
          .shuffleSpec(MixShuffleSpec.instance())
          .processorFactory(makeScanProcessorFactory(null, signature));
      return sdb;
    }

    private ScanQueryFrameProcessorFactory makeScanProcessorFactory(DataSource dataSource, RowSignature rowSignature)
    {
      ScanQuery s = Druids.newScanQueryBuilder()
          .dataSource(IRRELEVANT)
          .intervals(QuerySegmentSpec.DEFAULT)
          .virtualColumns(virtualColumns)
          .columns(rowSignature.getColumnNames())
          .columnTypes(rowSignature.getColumnTypes())
          .build();

      return new ScanQueryFrameProcessorFactory(s);
    }

    @Override
    public IStageDef extendWith(DruidNodeStack stack)
    {
      return null;
    }

  }

}
