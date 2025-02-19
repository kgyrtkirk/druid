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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.join.JoinPrefixUtils;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Identifies and answers questions regarding the native engine's vertex boundary.
 *
 * I believe due to evolutional purposes there are some concepts which went beyond their inital design:
 * <ul>
 *   <li>multiple queries might be executed in one stage: there is one query type which is collapsed at exection time (GroupBy)</li>
 *   <li>dag of datasources: an execution may process an entire dag of datasource in some cases (joindatasource) ; or collapse some into the execution (filter)</li>
 * </ul>
 *
 */
public class VertexBoundary
{
  private final DataSource baseDataSource;
  @Nullable
  private final Query<?> baseQuery;
  @Nullable
  private final DimFilter joinBaseTableFilter;
  private final List<PreJoinableClause> preJoinableClauses;
  @Nullable
  private final QuerySegmentSpec querySegmentSpec;

  public VertexBoundary(
      DataSource baseDataSource,
      @Nullable Query<?> baseQuery,
      @Nullable DimFilter joinBaseTableFilter,
      List<PreJoinableClause> preJoinableClauses,
      @Nullable
      QuerySegmentSpec querySegmentSpec
  )
  {
    if (baseDataSource instanceof JoinDataSource) {
      // The base cannot be a join (this is a class invariant).
      // If it happens, it's a bug in the datasource analyzer.
      throw new IAE("Base dataSource cannot be a join! Original base datasource was: %s", baseDataSource);
    }

    this.baseDataSource = baseDataSource;
    this.baseQuery = baseQuery;
    this.joinBaseTableFilter = joinBaseTableFilter;
    this.preJoinableClauses = preJoinableClauses;
    this.querySegmentSpec = querySegmentSpec;
  }

  /**
   * Returns the base (bottom-leftmost) datasource.
   */
  public DataSource getBaseDataSource()
  {
    return baseDataSource;
  }

  /**
   * Unwraps the {@link #getBaseDataSource()} if its a {@link TableDataSource}.
   *
   * @throws An error of type {@link DruidException.Category#DEFENSIVE} if the {@link BaseDataSource} is not a table.
   *
   * note that this may not be true even {@link #isConcreteAndTableBased()} is true - in cases when the base
   * datasource is a {@link UnionDataSource} of {@link TableDataSource}.
   */
  public TableDataSource getBaseTableDataSource()
  {
    if (baseDataSource instanceof TableDataSource) {
      return (TableDataSource) baseDataSource;
    } else {
      throw DruidException.defensive("Base dataSource[%s] is not a table!", baseDataSource);
    }
  }

  /**
   * If {@link #getBaseDataSource()} is a {@link UnionDataSource}, returns it. Otherwise, returns an empty Optional.
   */
  public Optional<UnionDataSource> getBaseUnionDataSource()
  {
    if (baseDataSource instanceof UnionDataSource) {
      return Optional.of((UnionDataSource) baseDataSource);
    } else {
      return Optional.empty();
    }
  }

  /**
   * Returns the bottom-most (i.e. innermost) {@link Query} from a possible stack of outer queries at the root of
   * the datasource tree. This is the query that will be applied to the base datasource plus any joinables that might
   * be present.
   *
   * @return the query associated with the base datasource if  is true, else empty
   */
  public Optional<Query<?>> getBaseQuery()
  {
    return Optional.ofNullable(baseQuery);
  }

  /**
   * If the original data source is a join data source and there is a DimFilter on the base table data source,
   * that DimFilter is returned here
   */
  public Optional<DimFilter> getJoinBaseTableFilter()
  {
    return Optional.ofNullable(joinBaseTableFilter);
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

  /**
   * Returns join clauses corresponding to joinable leaf datasources (every leaf except the bottom-leftmost).
   */
  public List<PreJoinableClause> getPreJoinableClauses()
  {
    return preJoinableClauses;
  }

  /**
   * Returns true if this datasource can be computed by the core Druid query stack via a scan of a concrete base
   * datasource. All other datasources involved, if any, must be global.
   */
  public boolean isConcreteBased()
  {
    return baseDataSource.isConcrete() && preJoinableClauses.stream()
                                                            .allMatch(clause -> clause.getDataSource().isGlobal());
  }

  /**
   * Returns whether this datasource is one of:
   *
   * <ul>
   *   <li>{@link TableDataSource}</li>
   *   <li>{@link UnionDataSource} composed entirely of {@link TableDataSource}</li>
   *   <li>{@link UnnestDataSource} composed entirely of {@link TableDataSource}</li>
   * </ul>
   */
  public boolean isTableBased()
  {
    return (baseDataSource instanceof TableDataSource
            || baseDataSource instanceof RestrictedDataSource
            || (baseDataSource instanceof UnionDataSource &&
                baseDataSource.getChildren()
                              .stream()
                              .allMatch(ds -> ds instanceof TableDataSource))
            || (baseDataSource instanceof UnnestDataSource &&
                baseDataSource.getChildren()
                              .stream()
                              .allMatch(ds -> ds instanceof TableDataSource)));
  }

  /**
   * Returns true if this datasource is both (see {@link #isConcreteBased()} and {@link #isTableBased()}.
   * This is an important property, because it corresponds to datasources that can be handled by Druid's distributed
   * query stack.
   */
  public boolean isConcreteAndTableBased()
  {
    // At the time of writing this comment, UnionDataSource children are required to be tables, so the instanceof
    // check is redundant. But in the future, we will likely want to support unions of things other than tables,
    // so check anyway for future-proofing.
    return isConcreteBased() && isTableBased();
  }

  /**
   * Returns true if this datasource is made out of a join operation
   */
  public boolean isJoin()
  {
    return !preJoinableClauses.isEmpty();
  }

  /**
   * Returns whether "column" on the analyzed datasource refers to a column from the base datasource.
   */
  public boolean isBaseColumn(final String column)
  {
    if (baseQuery != null) {
      return false;
    }

    for (final PreJoinableClause clause : preJoinableClauses) {
      if (JoinPrefixUtils.isPrefixedBy(column, clause.getPrefix())) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VertexBoundary that = (VertexBoundary) o;
    return Objects.equals(baseDataSource, that.baseDataSource) && Objects.equals(querySegmentSpec, that.querySegmentSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(baseDataSource, querySegmentSpec);
  }

  @Override
  public String toString()
  {
    return "DataSourceAnalysis{" +
           ", baseDataSource=" + baseDataSource +
           ", baseQuery=" + baseQuery +
           ", preJoinableClauses=" + preJoinableClauses +
           '}';
  }

  /**
   * {@link DataSource#isGlobal()}.
   */
  public boolean isGlobal()
  {
    for (PreJoinableClause preJoinableClause : preJoinableClauses) {
      if (!preJoinableClause.getDataSource().isGlobal()) {
        return false;
      }
    }
    return baseDataSource.isGlobal();
  }

  public VertexBoundary maybeWithQuerySegmentSpec(QuerySegmentSpec newQuerySegmentSpec)
  {
    if (newQuerySegmentSpec == null) {
      newQuerySegmentSpec = new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY);
    }
    if (querySegmentSpec == null) {
      return new VertexBoundary(
          baseDataSource,
          baseQuery,
          joinBaseTableFilter,
          preJoinableClauses,
          newQuerySegmentSpec
      );
    }
    return this;
  }
}
