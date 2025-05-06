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

import org.apache.calcite.rel.RelNode;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.logical.LogicalVertexBuilder.RootVertex;
import org.apache.druid.msq.querykit.DataSourcePlan;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;
import org.apache.druid.sql.calcite.planner.querygen.SourceDescProducer.SourceDesc;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode;
import org.apache.druid.sql.calcite.rel.logical.DruidProject;
import org.apache.druid.sql.calcite.rel.logical.DruidTableScan;
import org.apache.druid.sql.calcite.rel.logical.DruidValues;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class QueryDefinitionTranslator
{
  private PlannerContext plannerContext;
  private LogicalVertexBuilder stageBuilder;

  public QueryDefinitionTranslator(PlannerContext plannerContext)
  {
    this.plannerContext = plannerContext;
    this.stageBuilder = new LogicalVertexBuilder(plannerContext);
  }

  public QueryDefinition translate(DruidLogicalNode relRoot)
  {
    DruidNodeStack stack = new DruidNodeStack();
    stack.push(relRoot);
    LogicalVertex vertex = buildVertexFor(stack);
    return vertex.build();
  }

  private LogicalVertex buildVertexFor(DruidNodeStack stack)
  {
    List<LogicalVertex> newInputs = new ArrayList<>();

    for (RelNode input : stack.peekNode().getInputs()) {
      stack.push((DruidLogicalNode) input, newInputs.size());
      newInputs.add(buildVertexFor(stack));
      stack.pop();
    }
    LogicalVertex vertex = processNodeWithInputs(stack, newInputs);
    return vertex;
  }

  private LogicalVertex processNodeWithInputs(DruidNodeStack stack, List<LogicalVertex> newInputs)
  {
    DruidLogicalNode node = stack.peekNode();
    Optional<RootVertex> vertex = buildRootVertex(node);
    if (vertex.isPresent()) {
      return vertex.get();
    }
    if (newInputs.size() == 1) {
      LogicalVertex inputVertex = newInputs.get(0);
      LogicalVertex newVertex = inputVertex.extendWith(stack);
      if (newVertex != null) {
        return newVertex;
      }
      Optional<LogicalVertex> seq = buildSequenceVertex(stack, newInputs.get(0));
      if (seq.isPresent()) {
        return seq.get();
      }
    }
    throw DruidException.defensive().build("Unable to process relNode[%s]", node);
  }

  private Optional<LogicalVertex> buildSequenceVertex(DruidNodeStack stack, LogicalVertex vertex)
  {
    DruidLogicalNode node = stack.peekNode();
    if (node instanceof DruidProject) {
      // return makeScanProcessorFactory(vertex.null, null);
    }
    return Optional.empty();
  }

  private Optional<RootVertex> buildRootVertex(DruidLogicalNode node)
  {
    if (node instanceof DruidValues) {
      return translateValues((DruidValues) node);
    }
    if (node instanceof DruidTableScan) {
      return translateTableScan((DruidTableScan) node);
    }
    return Optional.empty();
  }

  private Optional<RootVertex> translateTableScan(DruidTableScan node)
  {
    SourceDesc sd = node.getSourceDesc(plannerContext, Collections.emptyList());
    DataSource ds = sd.dataSource;
    TableDataSource ids = (TableDataSource) ds;
    DataSourcePlan dsp = DataSourcePlan.forTable(
        ids,
        Intervals.ONLY_ETERNITY,
        null, null,
        false
    );
    List<InputSpec> isp = dsp.getInputSpecs();

    RootVertex vertex = stageBuilder.new RootVertex(sd.rowSignature, isp);
    return Optional.of(vertex);
  }

  // this is a hack for now
  private Optional<RootVertex> translateValues(DruidValues node)
  {
    SourceDesc sd = node.getSourceDesc(plannerContext, Collections.emptyList());
    DataSource ds = sd.dataSource;
    InlineDataSource ids = (InlineDataSource) ds;
    DataSourcePlan dsp = DataSourcePlan.forInline(ids, false);
    List<InputSpec> isp = dsp.getInputSpecs();

    RootVertex vertex = stageBuilder.new RootVertex(sd.rowSignature, isp);
    return Optional.of(vertex);
  }
}
