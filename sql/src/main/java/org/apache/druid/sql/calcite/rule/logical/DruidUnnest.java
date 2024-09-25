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

package org.apache.druid.sql.calcite.rule.logical;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.SourceDescProducer;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode;

import java.util.List;

public class DruidUnnest extends Unnest implements DruidLogicalNode, SourceDescProducer
{
  protected DruidUnnest(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexNode unnestExpr,
      RelDataType rowType)
  {
    super(cluster, traits, input, unnestExpr, rowType);
  }

  @Override
  protected RelNode copy(RelTraitSet traitSet, RelNode input)
  {
    return new DruidUnnest(getCluster(), traitSet, input, unnestExpr, rowType);
  }

  @Override
  public SourceDesc getSourceDesc(PlannerContext plannerContext, List<SourceDesc> sources)
  {
    SourceDesc inputDesc = sources.get(0);
    UnnestDataSource ds = UnnestDataSource.create(inputDesc.dataSource, null, null);


    RowSignature.builder()
    .addAll(inputDesc.rowSignature)
    .add(unnest1, unnestExpr.getType()));
    RowSignature aa = inputDesc.rowSignature;
    return new SourceDesc(        inputDesc.dataSource, aa    );
    // return null;//DruidJoinQueryRel.buildJoinSourceDesc(leftDesc, null,
    // plannerContext, this, null);
  }
}
