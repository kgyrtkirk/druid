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

import org.apache.druid.error.DruidException;
import org.apache.druid.frame.allocation.ArenaMemoryAllocatorFactory;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryLogic;
import org.apache.druid.query.QueryLogicExecutionContext;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.ResultSerializationMode;
import org.apache.druid.query.context.ResponseContext;

/**
 * Provides {@link QueryLogic} based on {@link QueryToolChest} functions.
 */
public class ToolChestBasedQueryLogic<T> implements QueryLogic
{
  private QueryToolChest<T, Query<T>> toolChest;

  public ToolChestBasedQueryLogic(QueryToolChest<T, Query<T>> toolChest)
  {
    this.toolChest = toolChest;
  }

  @Override
  public <T> QueryRunner<Object> entryPoint(Query<T> query, QueryLogicExecutionContext context)
  {
    return new ToolChestBasedQueryRunner(query, context, toolChest);
  }

  private static class ToolChestBasedQueryRunner<T> implements QueryRunner<T>
  {
    private final QueryRunner<T> runner;
    private final QueryToolChest<T, Query<T>> toolChest;
    private final QueryLogicExecutionContext context;

    public ToolChestBasedQueryRunner(Query<T> query, QueryLogicExecutionContext context,
        QueryToolChest<T, Query<T>> toolChest)
    {
      this.context = context;
      this.runner = query.getRunner(context.walker);
      this.toolChest = toolChest;
    }

    // note: returns a Sequence<Object> and not Sequenct<T>
    @Override
    public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
    {
      Query<T> query = queryPlus.getQuery();
      Sequence<T> seq = runner.run(queryPlus, responseContext);

      boolean useNestedForUnknownTypeInSubquery = query.context()
          .isUseNestedForUnknownTypeInSubquery(context.isUseNestedForUnknownTypeInSubquery);

      ResultSerializationMode serializationMode = getResultSerializationMode(query);
      Sequence<?> resultSeq;
      switch (serializationMode)
      {
        case ROWS:
          resultSeq = toolChest.resultsAsArrays(query, seq);
          break;
        case FRAMES:
          resultSeq = toolChest.resultsAsFrames(
              query,
              seq,
              ArenaMemoryAllocatorFactory.makeDefault(),
              useNestedForUnknownTypeInSubquery
          ).orElseThrow(() -> DruidException.defensive("Unable to materialize the results as frames."));
          break;
        default:
          throw DruidException.defensive("Not supported serializationMode [%s].", serializationMode);
      }
      // this case is not valid; however QueryRunner<T> makes most of the
      // template usage okay.
      return (Sequence<T>) resultSeq;
    }

    private ResultSerializationMode getResultSerializationMode(Query<T> query)
    {
      ResultSerializationMode serializationMode = query.context().getEnum(
          ResultSerializationMode.CTX_SERIALIZATION_PARAMETER,
          ResultSerializationMode.class,
          null
      );
      if (serializationMode == null) {
        throw DruidException.defensive(
            "Serialization mode [%s] is not setup correctly!", ResultSerializationMode.CTX_SERIALIZATION_PARAMETER
        );
      }
      return serializationMode;
    }
  }
}
