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
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.querygen.SourceDescProducer;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

public class DruidLogicalCorrelate extends Correlate implements DruidLogicalNode, SourceDescProducer
{

  public DruidLogicalCorrelate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode left,
      RelNode right,
      CorrelationId correlationId,
      ImmutableBitSet requiredColumns,
      JoinRelType joinType)
  {
    super(
        cluster,
        traitSet,
        hints,
        left,
        right,
        correlationId,
        requiredColumns,
        joinType
    );
  }

  @Override
  public SourceDesc getSourceDesc(PlannerContext plannerContext, List<SourceDesc> sources)
  {
    if(true)
    {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Correlate copy(RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId,
      ImmutableBitSet requiredColumns, JoinRelType joinType)
  {
    return new DruidLogicalCorrelate(
        getCluster(), traitSet, hints, left, right, correlationId, requiredColumns, joinType
    );

  }

  @Override
  public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
  {
    return super.computeSelfCost(planner, mq);

  }
}
