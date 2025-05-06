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

import org.apache.druid.error.DruidException;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.kernel.MixShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageDefinitionBuilder;
import org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessorFactory;
import org.apache.druid.query.Druids;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.Projection;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;
import org.apache.druid.sql.calcite.rel.logical.DruidFilter;
import org.apache.druid.sql.calcite.rel.logical.DruidProject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class LogicalVertexBuilder
{
  private AtomicInteger stageIdSeq = new AtomicInteger(1);
  private PlannerContext plannerContext;

  public LogicalVertexBuilder(PlannerContext plannerContext2)
  {
    this.plannerContext = plannerContext2;
  }

  public class AbstractVertex implements LogicalVertex
  {
    protected final List<InputSpec> inputSpecs;
    protected final RowSignature signature;
    protected final List<LogicalVertex> inputVertices;

    public AbstractVertex(RowSignature signature, List<InputSpec> inputs, List<LogicalVertex> inputVertices)
    {
      this.inputSpecs = inputs;
      this.signature = signature;
      this.inputVertices = inputVertices;
    }

    @Override
    public StageDefinition contructStage()
    {
      throw DruidException.defensive("This should have been implemented - or not reach this point!");
    }

    @Override
    public LogicalVertex extendWith(DruidNodeStack stack)
    {
      return null;
    }

    @Override
    public final QueryDefinition build()
    {
      return QueryDefinition.create(buildStageDefinitions(), plannerContext.queryContext());
    }

    @Override
    public final List<StageDefinition> buildStageDefinitions()
    {
      List<StageDefinition> ret = new ArrayList<>();
      for (LogicalVertex vertex : inputVertices) {
        ret.addAll(vertex.buildStageDefinitions());
      }
      ret.add(contructStage());
      return ret;
    }
  }

  public class RootVertex extends AbstractVertex
  {
    public RootVertex(RowSignature signature, List<InputSpec> inputSpecs)
    {
      super(signature, inputSpecs, Collections.emptyList());
    }

    public RootVertex(RootVertex root, RowSignature newSignature)
    {
      super(newSignature, root.inputSpecs, root.inputVertices);
    }

    @Override
    public StageDefinition contructStage()
    {
      return makeScanStage(VirtualColumns.EMPTY, signature, inputSpecs, null);
    }

    @Override
    public LogicalVertex extendWith(DruidNodeStack stack)
    {
      if (stack.peekNode() instanceof DruidFilter) {
        DruidFilter filter = (DruidFilter) stack.peekNode();
        return makeFilterVertex(filter);
      }

      if (stack.peekNode() instanceof DruidProject) {

        DruidProject project = (DruidProject) stack.peekNode();
        DruidFilter dummyFilter = new DruidFilter(
            project.getCluster(), project.getTraitSet(), project,
            project.getCluster().getRexBuilder().makeLiteral(true)
        );
        return makeFilterVertex(dummyFilter).extendWith(stack);
      }
      return null;
    }

    private LogicalVertex makeFilterVertex(DruidFilter filter)
    {
      VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
          signature,
          plannerContext.getExpressionParser(),
          plannerContext.getPlannerConfig().isForceExpressionVirtualColumns()
      );

      DimFilter dimFilter = DruidQuery.getDimFilter(
          plannerContext,
          signature, virtualColumnRegistry, filter
      );

      return new FilterVertex(
          this,
          virtualColumnRegistry,
          dimFilter
      );
    }
  }

  public FilterVertex create(RootVertex inputStage, DruidFilter filter)
  {
    VirtualColumnRegistry virtualColumnRegistry = VirtualColumnRegistry.create(
        inputStage.signature,
        plannerContext.getExpressionParser(),
        plannerContext.getPlannerConfig().isForceExpressionVirtualColumns()
    );
    DimFilter dimFilter = DruidQuery.getDimFilter(plannerContext, inputStage.signature, virtualColumnRegistry, filter);
    return new FilterVertex(inputStage, virtualColumnRegistry, dimFilter);
  }

  class FilterVertex extends RootVertex
  {
    protected final VirtualColumnRegistry virtualColumnRegistry;
    private DimFilter dimFilter;

    public FilterVertex(RootVertex inputStage, VirtualColumnRegistry virtualColumnRegistry, DimFilter dimFilter)
    {
      super(inputStage, inputStage.signature);
      this.virtualColumnRegistry = virtualColumnRegistry;
      this.dimFilter = dimFilter;
    }

    public FilterVertex(FilterVertex root, VirtualColumnRegistry newVirtualColumnRegistry, RowSignature rowSignature)
    {
      super(root, rowSignature);
      this.dimFilter = root.dimFilter;
      this.virtualColumnRegistry = newVirtualColumnRegistry;
    }

    @Override
    public StageDefinition contructStage()
    {
      VirtualColumns output = virtualColumnRegistry.build(Collections.emptySet());
      return makeScanStage(output, signature, inputSpecs, dimFilter);
    }

    @Override
    public LogicalVertex extendWith(DruidNodeStack stack)
    {
      if (stack.peekNode() instanceof DruidProject) {
        DruidProject project = (DruidProject) stack.peekNode();
        Projection preAggregation = Projection
            .preAggregation(project, plannerContext, signature, virtualColumnRegistry);

        return new ProjectVertex(
            this,
            virtualColumnRegistry,
            preAggregation.getOutputRowSignature()
        );
      }
      return null;
    }
  }

  class ProjectVertex extends FilterVertex
  {
    public ProjectVertex(FilterVertex root, VirtualColumnRegistry newVirtualColumnRegistry,
        RowSignature rowSignature)
    {
      super(root, newVirtualColumnRegistry, rowSignature);
    }

    @Override
    public LogicalVertex extendWith(DruidNodeStack stack)
    {
      return null;
    }
  }

  private static final String IRRELEVANT = "irrelevant";

  private StageDefinition makeScanStage(
      VirtualColumns virtualColumns,
      RowSignature signature,
      List<InputSpec> inputs,
      DimFilter dimFilter)
  {
    ScanQuery s = Druids.newScanQueryBuilder()
        .dataSource(IRRELEVANT)
        .intervals(QuerySegmentSpec.ETERNITY)
        .filters(dimFilter)
        .virtualColumns(virtualColumns)
        .columns(signature.getColumnNames())
        .columnTypes(signature.getColumnTypes())
        .build();
    ScanQueryFrameProcessorFactory scanProcessorFactory = new ScanQueryFrameProcessorFactory(s);
    StageDefinitionBuilder sdb = StageDefinition.builder(stageIdSeq.incrementAndGet())
        .inputs(inputs)
        .signature(signature)
        .shuffleSpec(MixShuffleSpec.instance())
        .processorFactory(scanProcessorFactory);
    return sdb.build(plannerContext.getSqlQueryId());
  }

  public RootVertex makeRootVertex(RowSignature rowSignature, List<InputSpec> isp)
  {
    return new RootVertex(null, rowSignature);
  }

}
