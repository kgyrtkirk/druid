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

import com.google.common.base.Preconditions;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.join.JoinPrefixUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * Represents the native engine's execution vertex.
 *
 * Most likely due to evolutional purposes there are some concepts which went
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
  protected final DataSource baseDataSource;
  protected final QuerySegmentSpec querySegmentSpec;
  protected final List<String> joinPrefixes;
  protected boolean allRightsAreGlobal;

  private ExecutionVertex(ExecutionVertexExplorer explorer)
  {
    this.topQuery = explorer.topQuery;
    this.baseDataSource = explorer.baseDataSource;
    this.querySegmentSpec = explorer.querySegmentSpec;
    this.joinPrefixes = explorer.joinPrefixes;
    this.allRightsAreGlobal = explorer.allRightsAreGlobal;
  }

  public static ExecutionVertex of(Query<?> query)
  {
    ExecutionVertexExplorer explorer = new ExecutionVertexExplorer(query);
    return new ExecutionVertex(explorer);
  }

  public DataSource getBaseDataSource()
  {
    return baseDataSource;
  }

  // FIXME: correct apidcos?
  // FIXME rename
  public boolean isExecutable()
  {
    return getBaseDataSource().isConcrete() && allRightsAreGlobal;
//topQuery.getDataSource().isConcrete();
  }

  public boolean isTableBased()
  {
    return baseDataSource instanceof TableDataSource
        // || baseDataSource instanceof RestrictedDataSource
        || (baseDataSource instanceof UnionDataSource &&
            baseDataSource.getChildren()
                .stream()
                .allMatch(ds -> ds instanceof TableDataSource));
    // || (baseDataSource instanceof UnnestDataSource &&
    // baseDataSource.getChildren()
    // .stream()
    // .allMatch(ds -> ds instanceof TableDataSource)));
  }

  /**
   * Execution vertex node.
   *
   * Can be a {@link Query} or a {@link DataSource}.
   */
  static class EVNode
  {
    final DataSource dataSource;
    final Query<?> query;
    final Integer index;

    EVNode(DataSource dataSource, Integer index)
    {
      this.dataSource = dataSource;
      this.query = null;
      this.index = index;
    }

    EVNode(Query<?> query, Integer index)
    {
      this.dataSource = null;
      this.query = query;
      this.index = index;
    }

    public boolean isQuery()
    {
      return query != null;
    }

    public Query<?> getQuery()
    {
      Preconditions.checkNotNull(query, "query is null!");
      return query;
    }
  }

  static abstract class ExecutionVertexShuttle
  {
    protected Stack<EVNode> parents = new Stack<>();

    protected final Query<?> traverse(Query<?> query)
    {
      try {
        parents.push(new EVNode(query, null));
        if (!mayTraverseQuery(query)) {
          return query;
        }
        if (query instanceof BaseQuery<?>) {
          BaseQuery<?> baseQuery = (BaseQuery<?>) query;
          DataSource oldDataSource = baseQuery.getDataSource();
          DataSource newDataSource = traverse(oldDataSource, null);
          if (oldDataSource != newDataSource) {
            query = baseQuery.withDataSource(newDataSource);
          }
        } else {
          throw DruidException.defensive("Can't traverse a query[%s]!", query);
        }
        return visitQuery(query);
      } finally {
        parents.pop();
      }
    }

    protected final DataSource traverse(DataSource dataSource, Integer index)
    {
      try {
        parents.push(new EVNode(dataSource, index));
        boolean traverse = mayTraverseDataSource(parents.peek());
        if (dataSource instanceof QueryDataSource) {
          QueryDataSource queryDataSource = (QueryDataSource) dataSource;
          if (traverse) {
            Query<?> oldQuery = queryDataSource.getQuery();
            Query<?> newQuery = traverse(oldQuery);
            if (oldQuery != newQuery) {
              dataSource = new QueryDataSource(newQuery);
            }
          }
          return visit(dataSource, !traverse);
        } else {
          List<DataSource> children = dataSource.getChildren();
          List<DataSource> newChildren = new ArrayList<>();
          boolean changed = false;
          if (traverse) {
            for (int i = 0; i < children.size(); i++) {
              DataSource oldDS = children.get(i);
              DataSource newDS = traverse(oldDS, i);
              newChildren.add(newDS);
              changed |= (oldDS != newDS);
            }
          }
          DataSource newDataSource = changed ? dataSource.withChildren(newChildren) : dataSource;
          return visit(newDataSource, !traverse);

        }
      } finally {
        parents.pop();
      }
    }

    protected abstract boolean mayTraverseQuery(Query<?> query);

    protected abstract boolean mayTraverseDataSource(EVNode evNode);

    protected abstract DataSource visit(DataSource dataSource, boolean leaf);

    protected abstract Query<?> visitQuery(Query<?> query);
  }

  static class ExecutionVertexExplorer extends ExecutionVertexShuttle
  {
    boolean discoveringBase = true;
    DataSource baseDataSource;
    QuerySegmentSpec querySegmentSpec;
    Query<?> topQuery;
    List<String> joinPrefixes = new ArrayList<String>();
    boolean allRightsAreGlobal = true;

    public ExecutionVertexExplorer(Query<?> query)
    {
      topQuery = query;
      traverse(query);
    }

    @Override
    protected boolean mayTraverseQuery(Query<?> query)
    {
      return true;
    }

    @Override
    protected boolean mayTraverseDataSource(EVNode node)
    {
      if (parents.size() < 2) {
        return true;
      }
      if (node.index != null && node.index > 0) {
        return false;
      }
      if (node.dataSource instanceof QueryDataSource) {
        EVNode parentNode = parents.get(parents.size() - 2);
        if (parentNode.isQuery()) {
          return parentNode.getQuery().mayCollapseQueryDataSource();
        }
      }
      if (node.dataSource instanceof UnionDataSource) {
        return false;
      }
      return true;
    }

    @Override
    protected DataSource visit(DataSource dataSource, boolean leaf)
    {
      if (discoveringBase) {
        baseDataSource = dataSource;
        discoveringBase = false;
      }

      if (!leaf &&  dataSource instanceof JoinDataSource) {
        JoinDataSource joinDataSource = (JoinDataSource) dataSource;
        joinPrefixes.add(joinDataSource.getRightPrefix());
      }
      if(leaf && !isLeftLeaning()) {
        allRightsAreGlobal &= dataSource.isGlobal();
      }
      return dataSource;
    }

    private boolean isLeftLeaning()
    {
      for (EVNode evNode : parents) {
        if (evNode.index != null && evNode.index != 0) {
          return false;
        }
      }
      return true;
    }

    @Override
    protected Query<?> visitQuery(Query<?> query)
    {
      if (querySegmentSpec == null && isLeftLeaning()) {
        querySegmentSpec = getQuerySegmentSpec(query);
      }
      return query;
    }

    private QuerySegmentSpec getQuerySegmentSpec(Query<?> query)
    {
      if (query instanceof BaseQuery) {
        BaseQuery<?> baseQuery = (BaseQuery<?>) query;
        return baseQuery.getQuerySegmentSpec();
      } else {
        return new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY);
      }
    }
  }

  public static ExecutionVertex ofIllegal(DataSource dataSource)
  {
    ScanQuery query = Druids
        .newScanQueryBuilder()
        .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
        .dataSource(dataSource)
        .build();
    return ExecutionVertex.of(query);
  }

  /**
   * Unwraps the {@link #getBaseDataSource()} if its a {@link TableDataSource}.
   *
   * @throws An
   *           error of type {@link DruidException.Category#DEFENSIVE} if the
   *           {@link #getBaseDataSource()} is not a table.
   *
   *           note that this may not be true even
   *           {@link #isConcreteAndTableBased()} is true - in cases when the
   *           base datasource is a {@link UnionDataSource} of
   *           {@link TableDataSource}.
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
   * There might be more queries inside a single vertex; so the outer one is not
   * necessary correct.
   */
  public QuerySegmentSpec getEffectiveQuerySegmentSpec()
  {
    Preconditions.checkNotNull(querySegmentSpec, "querySegmentSpec is null!");
    return querySegmentSpec;
  }

  public boolean canRunQueryUsingClusterWalker()
  {
    return isExecutable() && isTableBased();
  }

  public boolean canRunQueryUsingLocalWalker()
  {
    return isExecutable() && !isTableBased();
  }

  public boolean isJoin()
  {
    return !joinPrefixes.isEmpty();

  }

  public boolean isBaseColumn(String columnName)
  {
    for (String prefix : joinPrefixes) {
      if (JoinPrefixUtils.isPrefixedBy(columnName, prefix)) {
        return false;
      }
    }
    return true;
  }

  public DataSourceAnalysis getX()
  {
    if(true)
    {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }
}
