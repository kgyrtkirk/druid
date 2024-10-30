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

package org.apache.druid.query.union;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.allocation.MemoryAllocatorFactory;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.FrameSignaturePair;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryLogic;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.RowSignature.Finalization;

import java.util.Optional;
import java.util.stream.Collectors;

public class UnionQueryQueryToolChest extends QueryToolChest<Object, UnionQuery>
    implements QueryLogic
{
  protected QueryRunnerFactoryConglomerate conglomerate;

  @Inject
  public void initialize(QueryRunnerFactoryConglomerate conglomerate)
  {
    this.conglomerate = conglomerate;
  }

  @Override
  public <T> QueryRunner<Object> entryPoint(Query<T> query, QuerySegmentWalker walker)
  {
    return new UnionQueryRunner((UnionQuery) query, conglomerate, walker);
  }

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner<Object> mergeResults(QueryRunner<Object> runner)
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public QueryMetrics<? super UnionQuery> makeMetrics(UnionQuery query)
  {
    return new DefaultQueryMetrics<>();
  }

  @Override
  public Function<Object, Object> makePreComputeManipulatorFn(
      UnionQuery query,
      MetricManipulationFn fn)
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Object> getResultTypeReference()
  {
    return null;
  }

  @Override
  public RowSignature resultArraySignature(UnionQuery query)
  {
    for (Query<?> q : query.queries) {
      RowSignature sig = q.getResultRowSignature(Finalization.UNKNOWN);
      if (sig != null) {
        return sig;
      }
    }
    throw DruidException.defensive(
        "None of the subqueries [%s] could provide row signature.",
        query.queries.stream().map(q -> q.getClass().getSimpleName()).collect(Collectors.toSet())
    );
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Sequence<Object[]> resultsAsArrays(
      UnionQuery query,
      Sequence<Object> resultSequence)
  {
    Sequence<?> res = resultSequence;
    return (Sequence<Object[]>) res;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Optional<Sequence<FrameSignaturePair>> resultsAsFrames(
      UnionQuery query,
      Sequence<Object> resultSequence,
      MemoryAllocatorFactory memoryAllocatorFactory,
      boolean useNestedForUnknownTypes)
  {
    Sequence<?> res = resultSequence;
    return Optional.of((Sequence<FrameSignaturePair>) res);
  }

}
