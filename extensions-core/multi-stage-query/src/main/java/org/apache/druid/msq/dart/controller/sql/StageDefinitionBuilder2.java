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
public class StageDefinitionBuilder2 implements IStageDef
{

  private RowSignature signature;
  private Projection projection;
  private PlannerContext plannerContext;
  private List<InputSpec> inputs;

  public StageDefinitionBuilder2(List<InputSpec> inputs1, RowSignature signature1, PlannerContext plannerContext2)
  {
    this.inputs = inputs1;
    this.signature = signature1;
    this.plannerContext = plannerContext2;
  }


  private static final String IRRELEVANT = "irrelevant";

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
        .columns(rowSignature.getColumnNames())
        .columnTypes(rowSignature.getColumnTypes())
        .build();

    return new ScanQueryFrameProcessorFactory(s);
  }

  public IStageDef extendWith(DruidNodeStack stack)
  {
    if (stack.peekNode() instanceof DruidProject && projection == null) {
      DruidProject project = (DruidProject) stack.peekNode();
      VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
          signature,
          plannerContext.getExpressionParser(),
          plannerContext.getPlannerConfig().isForceExpressionVirtualColumns()
      );
      Projection preAggregation = Projection.preAggregation(project, plannerContext, signature, virtualColumnRegistry);

      return new ProjectStageDefinition(
          inputs,
          virtualColumnRegistry.build(Collections.emptySet()),
          preAggregation.getOutputRowSignature()
      );
      // preAggregation.getOutputRowSignature();
      // return withProjection(preAggregation);

    }

    return null;
  }

  private static AtomicInteger stageIdSeq = new AtomicInteger(1);

  static class ProjectStageDefinition implements IStageDef
  {
    private List<InputSpec> inputs;
    private VirtualColumns virtualColumns;
    private RowSignature outputRowSignature;

    public ProjectStageDefinition(List<InputSpec> inputs, VirtualColumns virtualColumns2,
        RowSignature outputRowSignature)
    {
      this.inputs = inputs;
      this.virtualColumns = virtualColumns2;
      this.outputRowSignature = outputRowSignature;
    }

    @Override
    public StageDefinitionBuilder finalizeStage()
    {
      StageDefinitionBuilder sdb = StageDefinition.builder(stageIdSeq.incrementAndGet())
          .inputs(inputs)
          .signature(outputRowSignature)
          .shuffleSpec(MixShuffleSpec.instance())
          .processorFactory(makeScanProcessorFactory(null, outputRowSignature));
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
