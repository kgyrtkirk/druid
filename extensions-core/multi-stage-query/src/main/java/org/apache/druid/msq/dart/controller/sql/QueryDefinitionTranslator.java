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

import org.apache.calcite.rel.RelNode;
import org.apache.druid.error.DruidException;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.kernel.MixShuffleSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.kernel.StageDefinitionBuilder;
import org.apache.druid.msq.querykit.DataSourcePlan;
import org.apache.druid.msq.querykit.scan.ScanQueryFrameProcessorFactory;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.DruidQueryGenerator.DruidNodeStack;
import org.apache.druid.sql.calcite.planner.querygen.SourceDescProducer.SourceDesc;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode;
import org.apache.druid.sql.calcite.rel.logical.DruidValues;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryDefinitionTranslator
{

  private static final String IRRELEVANT = "irrelevant";
  private DruidLogicalNode logicalRoot;
  private PlannerContext plannerContext;
  private AtomicInteger stageIdSeq = new AtomicInteger(1);
  private QDVertexFactory vertexFactory;

  public QueryDefinitionTranslator(PlannerContext plannerContext, DruidLogicalNode logicalRoot)
  {
    this.plannerContext = plannerContext;
    this.logicalRoot = logicalRoot;
    this.vertexFactory = new QDVertexFactory(plannerContext);
  }

  public QueryDefinition translate(DruidLogicalNode relRoot)
  {
    DruidNodeStack stack = new DruidNodeStack();
    stack.push(relRoot);
    Vertex vertex = buildVertexFor(stack);
    QueryDefinitionBuilder qdb = QueryDefinition.builder(plannerContext.getSqlQueryId());
    return vertex.build(qdb).build();
  }

  private Vertex buildVertexFor(DruidNodeStack stack)
  {
    List<Vertex> newInputs = new ArrayList<>();

    for (RelNode input : stack.peekNode().getInputs()) {
      stack.push((DruidLogicalNode) input, newInputs.size());
      newInputs.add(buildVertexFor(stack));
      stack.pop();
    }
    Vertex vertex = processNodeWithInputs(stack, newInputs);
    return vertex;
  }

  private Vertex processNodeWithInputs(DruidNodeStack stack, List<Vertex> newInputs)
  {
    DruidLogicalNode node = stack.peekNode();
    Optional<Vertex> vertex = buildRootVertex(node);
    if (vertex.isPresent()) {
      return vertex.get();
    }
    if (newInputs.size() == 1) {
      Vertex inputVertex = newInputs.get(0);
      Optional<Vertex> newVertex = inputVertex.extendWith(stack);
      if (newVertex.isPresent()) {
        return newVertex.get();
      }
    }
    throw DruidException.defensive().build("Unable to process relNode[%s]", node);
  }

  private Optional<Vertex> buildRootVertex(DruidLogicalNode node)
  {
    if (node instanceof DruidValues) {
      return translateValues((DruidValues) node);
    }
    return Optional.empty();
  }

  // this is a hack for now
  private Optional<Vertex> translateValues(DruidValues node)
  {
    SourceDesc sd = node.getSourceDesc(plannerContext, Collections.emptyList());
    DataSource ds = sd.dataSource;
    InlineDataSource ids = (InlineDataSource) ds;
    DataSourcePlan dsp = DataSourcePlan.forInline(ids, false);
    List<InputSpec> isp = dsp.getInputSpecs();



    QueryDefinitionBuilder qdb = QueryDefinition.builder(IRRELEVANT);
    StageDefinitionBuilder sdb = StageDefinition.builder(stageIdSeq.incrementAndGet())
        .inputs(isp)
        .signature(sd.rowSignature)
        .shuffleSpec(MixShuffleSpec.instance())
        .processorFactory(makeScanProcessorFactory(dsp.getNewDataSource(), sd.rowSignature));

    Vertex vertex = vertexFactory.createVertex(sdb, Collections.emptyList());
    return Optional.of(vertex);
  }

  private ScanQueryFrameProcessorFactory makeScanProcessorFactory(DataSource dataSource, RowSignature rowSignature)
  {

    ScanQuery s = Druids.newScanQueryBuilder()
    .dataSource(dataSource)
    .intervals(QuerySegmentSpec.DEFAULT)
    .columns(rowSignature.getColumnNames())
    .columnTypes(rowSignature.getColumnTypes())
//    .columns("cnt", "m1", "v0", "v1")
//    .columnTypes(ColumnType.LONG, ColumnType.FLOAT, ColumnType.LONG, ColumnType.LONG)
    .build();


    return new ScanQueryFrameProcessorFactory(s);
//    Druids.newScanQueryBuilder()
//          .dataSource("irrelevant")
//          .intervals(QuerySegmentSpec.DEFAULT)
//
//    if(true)
//    {
//      throw new RuntimeException("FIXME: Unimplemented!");
//    }
//    return null;

  }

  protected static class QDVertexFactory
  {
    private final PlannerContext plannerContext;

    public QDVertexFactory(PlannerContext plannerContext)
    {
      this.plannerContext = plannerContext;
    }

    Vertex createVertex( StageDefinitionBuilder qdb, List<Vertex> inputs)
    {
      return new PDQVertex(qdb, inputs);
    }

    public class PDQVertex implements Vertex
    {
      final StageDefinitionBuilder sdb;
      final List<Vertex> inputs;

      public PDQVertex(StageDefinitionBuilder qdb, List<Vertex> inputs)
      {
        this.sdb = qdb.copy();
        this.inputs = inputs;
      }

      @Override
      public QueryDefinitionBuilder build(QueryDefinitionBuilder qdbx)
      {
        for (Vertex vertex : inputs) {
          qdbx = vertex.build(qdbx);
        }
        StageDefinitionBuilder finalizedStage = finalizeStage();

        return qdbx.add(finalizedStage);
      }

      private StageDefinitionBuilder finalizeStage()
      {
        return sdb;
      }

      /**
       * Extends the the current partial query with the new parent if possible.
       */
      @Override
      public Optional<Vertex> extendWith(DruidNodeStack stack)
      {
        Optional<StageDefinitionBuilder> newPartialQuery = extendStage(stack);
        if (!newPartialQuery.isPresent()) {
          return Optional.empty();
        }
        if (true) {
          throw DruidException.defensive("afs");
        }
        return Optional.of(createVertex(newPartialQuery.get(), inputs));
      }

      private Optional<StageDefinitionBuilder> extendStage(DruidNodeStack stack)
      {
        if (true) {
          throw new RuntimeException("FIXME: Unimplemented!");
        }
        return null;
      }
    }
  }

  /**
   * Execution dag vertex - encapsulates a list of operators.
   */
  private interface Vertex
  {
    /**
     * Builds the query.
     */
    QueryDefinitionBuilder build(QueryDefinitionBuilder qdbx);

    /**
     * Extends the current vertex to include the specified parent.
     */
    Optional<Vertex> extendWith(DruidNodeStack stack);

  }

}
