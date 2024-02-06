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

package org.apache.druid.sql.calcite.planner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery.Stage;
import org.apache.druid.sql.calcite.rel.logical.DruidTableScan;
import org.apache.druid.sql.calcite.rel.logical.DruidValues;
import org.apache.druid.sql.calcite.rule.DruidLogicalValuesRule;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.InlineTable;
import org.apache.druid.sql.calcite.table.RowSignatures;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts a DAG of {@link org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode} convention
 * to a native Druid query for execution.
 */
public class DruidQueryGenerator
{
  private final PlannerContext plannerContext;
  private final RelNode relRoot;
  private final RexBuilder rexBuilder;

  public DruidQueryGenerator(PlannerContext plannerContext, RelNode relRoot, RexBuilder rexBuilder)
  {
    this.plannerContext = plannerContext;
    this.relRoot = relRoot;
    this.rexBuilder = rexBuilder;
  }

  public DruidQuery buildQuery()
  {
    return buildVertexFor(relRoot, true).buildQuery(true);
  }

  private Vertex createVertex(RelNode scan)
  {
    Vertex vertex = new Vertex();
    vertex.partialDruidQuery = PartialDruidQuery.create(scan);
    return vertex;
  }

  private Vertex createVertex(RelNode node, Vertex inputVertex)
  {
    Vertex vertex = new Vertex();
    vertex.inputs = ImmutableList.of(inputVertex);
    vertex.partialDruidQuery = PartialDruidQuery.createOuterQuery(inputVertex.partialDruidQuery);
    return vertex;
  }

  /**
   * Execution dag vertex - encapsulates a list of operators.
   *
   * Right now it relies on {@link PartialDruidQuery} to hold on to the operators it encapsulates.
   */
  private class Vertex
  {
    PartialDruidQuery partialDruidQuery;
    DruidTable queryTable;
    List<Vertex> inputs;
    public DruidTable currentTable;

    public DruidQuery buildQuery(boolean topLevel)
    {
      InputDesc input = getInput();
      return partialDruidQuery.build(
          input.dataSource,
          input.rowSignature,
          plannerContext,
          rexBuilder,
          currentTable != null && !topLevel
      );
    }

    private InputDesc getInput()
    {
      if (currentTable != null) {
        return new InputDesc(currentTable.getDataSource(), currentTable.getRowSignature());
      }
      if (inputs.size() == 1) {
        DruidQuery inputQuery = inputs.get(0).buildQuery(false);
        return new InputDesc(new QueryDataSource(inputQuery.getQuery()), inputQuery.getOutputRowSignature());
      }
      throw new IllegalStateException();
    }

    private Vertex newVertex(PartialDruidQuery partialDruidQuery)
    {
      Vertex vertex = new Vertex();
      vertex.inputs = inputs;
      vertex.currentTable = currentTable;
      vertex.queryTable = queryTable;
      vertex.partialDruidQuery = partialDruidQuery;
      return vertex;
    }

    /**
     * Merges the given {@link RelNode} into the current partial query.
     *
     * @return the new merged vertex - or null if its not possible to merge
     */
    private Vertex mergeIntoDruidQuery(RelNode node, boolean isRoot)
    {
      if (accepts(node, Stage.WHERE_FILTER, Filter.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withWhereFilter((Filter) node);
        return newVertex(newPartialQuery);
      }
      if (accepts(node, Stage.SELECT_PROJECT, Project.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withSelectProject((Project) node);
        return newVertex(newPartialQuery);
      }
      if (accepts(node, Stage.AGGREGATE, Aggregate.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withAggregate((Aggregate) node);
        return newVertex(newPartialQuery);
      }
      if (accepts(node, Stage.AGGREGATE_PROJECT, Project.class) && isRoot) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withAggregateProject((Project) node);
        return newVertex(newPartialQuery);
      }
      if (accepts(node, Stage.HAVING_FILTER, Filter.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withHavingFilter((Filter) node);
        return newVertex(newPartialQuery);
      }
      if (accepts(node, Stage.SORT, Sort.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withSort((Sort) node);
        return newVertex(newPartialQuery);
      }
      if (accepts(node, Stage.SORT_PROJECT, Project.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withSortProject((Project) node);
        return newVertex(newPartialQuery);
      }
      if (accepts(node, Stage.WINDOW, Window.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withWindow((Window) node);
        return newVertex(newPartialQuery);
      }
      if (accepts(node, Stage.WINDOW_PROJECT, Project.class)) {
        PartialDruidQuery newPartialQuery = partialDruidQuery.withWindowProject((Project) node);
        return newVertex(newPartialQuery);
      }
      return null;
    }

    private boolean accepts(RelNode node, Stage whereFilter, Class<? extends RelNode> class1)
    {
      return partialDruidQuery.canAccept(whereFilter) && class1.isInstance(node);
    }
  }

  private Vertex buildVertexFor(RelNode node, boolean isRoot)
  {
    List<Vertex> newInputs = new ArrayList<>();
    for (RelNode input : node.getInputs()) {
      newInputs.add(buildVertexFor(input, false));
    }
    Vertex vertex = processNodeWithInputs(node, newInputs, isRoot);
    return vertex;
  }

  private Vertex processNodeWithInputs(RelNode node, List<Vertex> newInputs, boolean isRoot)
  {
    if (node instanceof DruidTableScan) {
      return visitTableScan((DruidTableScan) node);
    }
    if (node instanceof DruidValues) {
      return visitValues((DruidValues) node);
    }
    if (newInputs.size() == 1) {
      Vertex inputVertex = newInputs.get(0);
      Vertex newVertex = inputVertex.mergeIntoDruidQuery(node, isRoot);
      if (newVertex != null) {
        return newVertex;
      }
      inputVertex = createVertex(node, inputVertex);
      newVertex = inputVertex.mergeIntoDruidQuery(node, false);
      if (newVertex != null) {
        return newVertex;
      } else {
        throw new IllegalStateException("should not happend");
      }
    }
//    if(node instanceof Union) {
//      return processUnion(node, newInputs, isRoot);
//    }
    throw new UnsupportedOperationException();
  }

//  private Vertex processUnion(RelNode node, List<Vertex> newInputs, boolean isRoot)
//  {
//    Preconditions.checkArgument(isRoot, "non-root not supported");
//
////    PartialDruidQuery.to
//
//  }

  private Vertex visitValues(DruidValues values)
  {
    final List<ImmutableList<RexLiteral>> tuples = values.getTuples();
    final List<Object[]> objectTuples = tuples
        .stream()
        .map(
            tuple -> tuple
                .stream()
                .map(v -> DruidLogicalValuesRule.getValueFromLiteral(v, plannerContext))
                .collect(Collectors.toList())
                .toArray(new Object[0])
        )
        .collect(Collectors.toList());
    RowSignature rowSignature = RowSignatures.fromRelDataType(
        values.getRowType().getFieldNames(),
        values.getRowType()
    );
    InlineTable inlineTable = new InlineTable(InlineDataSource.fromIterable(objectTuples, rowSignature));

    Vertex vertex = createVertex(values);
    vertex.currentTable = inlineTable;

    return vertex;

  }

  private Vertex visitTableScan(DruidTableScan scan)
  {
    if (!(scan instanceof DruidTableScan)) {
      throw new ISE("Planning hasn't converted logical table scan to druid convention");
    }
    DruidTableScan druidTableScan = scan;
    Preconditions.checkArgument(scan.getInputs().size() == 0);

    Vertex vertex = createVertex(scan);

    final RelOptTable table = scan.getTable();
    final DruidTable druidTable = table.unwrap(DruidTable.class);

    Preconditions.checkArgument(druidTable != null);

    vertex.currentTable = druidTable;
    if (druidTableScan.getProject() != null) {
      vertex.partialDruidQuery = vertex.partialDruidQuery.withSelectProject(druidTableScan.getProject());
    }
    return vertex;
  }

  /**
   * Utility class to return represent input related things.
   */
  private static class InputDesc
  {
    private DataSource dataSource;
    private RowSignature rowSignature;

    public InputDesc(DataSource dataSource, RowSignature rowSignature)
    {
      this.dataSource = dataSource;
      this.rowSignature = rowSignature;
    }
  }
}
