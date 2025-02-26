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

package org.apache.druid.query.planning;

import org.apache.druid.error.DruidException;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.spec.QuerySegmentSpec;

/**
 * Represents the native engine's execution vertex.
 *
 * I believe due to evolutional purposes there are some concepts which went
 * beyond their inital design:
 *
 * Multiple queries might be executed in one stage: <br/>
 * The GroupBy query could be collapsed at exection time).
 *
 * Dag of datasources: <br/>
 * an execution may process an entire dag of datasource in some cases
 * (joindatasource) ; or collapse some into the execution (filter)
 */
public class ExecutionVertex
{
  protected final Query<?> topQuery;
  private DataSource baseDataSource;
  private QuerySegmentSpec querySegmentSpec;

  private ExecutionVertex(Query<?> topQuery)
  {
    this.topQuery = topQuery;
  }

  public static ExecutionVertex of(Query<?> query)
  {
    ExecutionVertexExplorer executionVertexExplorer = new ExecutionVertexExplorer();
    executionVertexExplorer.visit(query);
    query.getDataSource();
    return new ExecutionVertex(query);
  }

  public DataSource getBaseDataSource()
  {
    return baseDataSource;
  }

  // FIXME: correct apidcos?
  public boolean isConcreteBased()
  {
    return getBaseDataSource().isConcrete();
  }

  public boolean isTableBased()
  {
    return baseDataSource instanceof TableDataSource;
//        || baseDataSource instanceof RestrictedDataSource
//        || (baseDataSource instanceof UnionDataSource &&
//            baseDataSource.getChildren()
//                .stream()
//                .allMatch(ds -> ds instanceof TableDataSource))
//        || (baseDataSource instanceof UnnestDataSource &&
//            baseDataSource.getChildren()
//                .stream()
//                .allMatch(ds -> ds instanceof TableDataSource)));
  }


  static abstract class ExecutionVertexShuttle
  {
    public Query<?> visit(Query<?> query)
    {
      if (query instanceof BaseQuery<?>) {
        BaseQuery<?> baseQuery = (BaseQuery<?>) query;
        DataSource oldDataSource = baseQuery.getDataSource();
        DataSource newDataSource = visit(oldDataSource);
        if (oldDataSource != newDataSource) {
          return baseQuery.withDataSource(newDataSource);
        }
      }
      return query;
    }

    public abstract DataSource visit(DataSource dataSource);

  }
  static class ExecutionVertexExplorer extends ExecutionVertexShuttle
  {
  boolean discoveringBase = true;

    boolean discoveringBase = true;

    public Query<?> visit(Query<?> query)
    {
      if (query instanceof BaseQuery<?>) {
        BaseQuery<?> baseQuery = (BaseQuery<?>) query;
        DataSource oldDataSource = baseQuery.getDataSource();
        DataSource newDataSource = visit(oldDataSource);
        if (oldDataSource != newDataSource) {
          return baseQuery.withDataSource(newDataSource);
        }
      }
      return query;
    }

    private DataSource visit(DataSource dataSource)
    {
      if (true) {
        throw new RuntimeException("FIXME: Unimplemented!");
      }
      return null;

    }

  }

  public static DruidException ofIllegal(Object dataSource)
  {
    return DruidException.defensive("Asd");
  }

  /**
   * Unwraps the {@link #getBaseDataSource()} if its a {@link TableDataSource}.
   *
   * @throws An error of type {@link DruidException.Category#DEFENSIVE} if the {@link BaseDataSource} is not a table.
   *
   * note that this may not be true even {@link #isConcreteAndTableBased()} is true - in cases when the base
   * datasource is a {@link UnionDataSource} of {@link TableDataSource}.
   */
  public final TableDataSource getBaseTableDataSource()
  {
    if (baseDataSource instanceof TableDataSource) {
      return (TableDataSource) baseDataSource;
    } else {
      throw DruidException.defensive("Base dataSource[%s] is not a table!", baseDataSource);
    }
  }

  /**
   * The applicable {@link QuerySegmentSpec} for this vertex.
   *
   * There might be more queries inside a single vertex; so the outer one is not necessary correct.
   */
  public QuerySegmentSpec getEffectiveQuerySegmentSpec()
  {
    if (querySegmentSpec == null) {
      throw DruidException
          .defensive("Can't answer this question. Please obtain a datasource analysis from the Query object!");
    }
    return querySegmentSpec;
  }

  public boolean canRunQueryUsingClusterWalker()
  {
    throw new RuntimeException("FIXME: Unimplemented!");
  }

  public boolean canRunQueryUsingLocalWalker()
  {
      throw new RuntimeException("FIXME: Unimplemented!");
  }

  @Deprecated
  public boolean isGlobal()
  {
    return baseDataSource.isGlobal();

  }

  @Deprecated
  public boolean isJoin()
  {
    return false;

  }

  @Deprecated
  public boolean isBaseColumn(String string)
  {
    return true;//dsa.isBaseColumn(string);
  }
}
