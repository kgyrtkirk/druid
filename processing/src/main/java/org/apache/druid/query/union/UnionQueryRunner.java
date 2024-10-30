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

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryLogic;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.context.ResponseContext;

import java.util.ArrayList;
import java.util.List;

class UnionQueryRunner implements QueryRunner<Object>
{
  private final QuerySegmentWalker walker;
  private final List<QueryRunner> runners;
  private QueryRunnerFactoryConglomerate conglomerate;

  public UnionQueryRunner(
      UnionQuery query,
      QuerySegmentWalker walker,
      QueryRunnerFactoryConglomerate conglomerate)
  {
    this.walker = walker;
    this.conglomerate = conglomerate;
    this.runners = makeSubQueryRunners(query);
  }

  private List<QueryRunner> makeSubQueryRunners(UnionQuery unionQuery)
  {
    List<QueryRunner> runners = new ArrayList<>();
    for (Query<?> query : unionQuery.queries) {
      runners.add(buildRunnerFor(query));
    }
    return runners;
  }

  private QueryRunner<?> buildRunnerFor(Query<?> query)
  {
    QueryLogic queryLogic = getQueryLogicFor(query);
    return queryLogic.entryPoint(query, walker);
  }

  private QueryLogic getQueryLogicFor(Query<?> query)
  {
    QueryLogic queryLogic = conglomerate.getQueryLogic(query);
    if (queryLogic != null) {
      return queryLogic;
    }
    return new ToolChestBasedQueryLogic(conglomerate.getToolChest(query));
  }

  @Override
  public Sequence<Object> run(QueryPlus<Object> queryPlus, ResponseContext responseContext)
  {
    UnionQuery unionQuery = queryPlus.unwrapQuery(UnionQuery.class);

    List<Sequence<Object>> seqs = new ArrayList<>();
    for (int i = 0; i < runners.size(); i++) {
      Query<?> q = unionQuery.queries.get(i);
      QueryRunner runner = runners.get(i);
      Sequence run = runner.run(queryPlus.withQuery(q), responseContext);
      seqs.add(run);
    }
    return Sequences.concat(seqs);
  }
}
