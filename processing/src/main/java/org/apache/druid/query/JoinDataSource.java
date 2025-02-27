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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.DimFilters;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.join.HashJoinSegment;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinPrefixUtils;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.segment.join.JoinableClause;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.join.filter.JoinFilterAnalyzer;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysisKey;
import org.apache.druid.segment.join.filter.JoinableClauses;
import org.apache.druid.segment.join.filter.rewrite.JoinFilterRewriteConfig;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Represents a join of two datasources.
 * <p>
 * Logically, this datasource contains the result of:
 * <p>
 * (1) prefixing all right-side columns with "rightPrefix"
 * (2) then, joining the left and (prefixed) right sides using the provided type and condition
 * <p>
 * Any columns from the left-hand side that start with "rightPrefix", and are at least one character longer than
 * the prefix, will be shadowed. It is up to the caller to ensure that no important columns are shadowed by the
 * chosen prefix.
 * <p>
 * When analyzed by {@link DataSourceAnalysis}, the right-hand side of this datasource
 * will become a {@link PreJoinableClause} object.
 */
public class JoinDataSource implements DataSource
{
  private final DataSource left;
  private final DataSource right;
  private final String rightPrefix;
  private final JoinConditionAnalysis conditionAnalysis;
  private final JoinType joinType;
  // An optional filter on the left side if left is direct table access
  @Nullable
  private final DimFilter leftFilter;
  @Nullable
  private final JoinableFactoryWrapper joinableFactoryWrapper;
  private final JoinAlgorithm joinAlgorithm;
  private static final Logger log = new Logger(JoinDataSource.class);

  private JoinDataSource(
      DataSource left,
      DataSource right,
      String rightPrefix,
      JoinConditionAnalysis conditionAnalysis,
      JoinType joinType,
      @Nullable DimFilter leftFilter,
      @Nullable JoinableFactoryWrapper joinableFactoryWrapper,
      JoinAlgorithm joinAlgorithm
  )
  {
    this.left = Preconditions.checkNotNull(left, "left");
    this.right = Preconditions.checkNotNull(right, "right");
    this.rightPrefix = JoinPrefixUtils.validatePrefix(rightPrefix);
    this.conditionAnalysis = Preconditions.checkNotNull(conditionAnalysis, "conditionAnalysis");
    this.joinType = Preconditions.checkNotNull(joinType, "joinType");
    this.leftFilter = validateLeftFilter(left, leftFilter);
    this.joinableFactoryWrapper = joinableFactoryWrapper;
    this.joinAlgorithm = JoinAlgorithm.BROADCAST.equals(joinAlgorithm) ? null : joinAlgorithm;
  }

  /**
   * Create a join dataSource from a string condition.
   */
  @JsonCreator
  public static JoinDataSource create(
      @JsonProperty("left") DataSource left,
      @JsonProperty("right") DataSource right,
      @JsonProperty("rightPrefix") String rightPrefix,
      @JsonProperty("condition") String condition,
      @JsonProperty("joinType") JoinType joinType,
      @Nullable @JsonProperty("leftFilter") DimFilter leftFilter,
      @JacksonInject ExprMacroTable macroTable,
      @Nullable @JacksonInject JoinableFactoryWrapper joinableFactoryWrapper,
      @Nullable @JsonProperty("joinAlgorithm") JoinAlgorithm joinAlgorithm
  )
  {
    return new JoinDataSource(
        left,
        right,
        StringUtils.nullToEmptyNonDruidDataString(rightPrefix),
        JoinConditionAnalysis.forExpression(
            Preconditions.checkNotNull(condition, "condition"),
            StringUtils.nullToEmptyNonDruidDataString(rightPrefix),
            macroTable
        ),
        joinType,
        leftFilter,
        joinableFactoryWrapper,
        joinAlgorithm
    );
  }

  /**
   * Create a join dataSource from an existing {@link JoinConditionAnalysis}.
   */
  public static JoinDataSource create(
      final DataSource left,
      final DataSource right,
      final String rightPrefix,
      final JoinConditionAnalysis conditionAnalysis,
      final JoinType joinType,
      final DimFilter leftFilter,
      @Nullable final JoinableFactoryWrapper joinableFactoryWrapper,
      @Nullable final JoinAlgorithm joinAlgorithm
  )
  {
    return new JoinDataSource(
        left,
        right,
        rightPrefix,
        conditionAnalysis,
        joinType,
        leftFilter,
        joinableFactoryWrapper,
        joinAlgorithm
    );
  }

  @Override
  public Set<String> getTableNames()
  {
    final Set<String> names = new HashSet<>();
    names.addAll(left.getTableNames());
    names.addAll(right.getTableNames());
    return names;
  }

  @JsonProperty
  public DataSource getLeft()
  {
    return left;
  }

  @JsonProperty
  public DataSource getRight()
  {
    return right;
  }

  @JsonProperty
  public String getRightPrefix()
  {
    return rightPrefix;
  }

  @JsonProperty
  public String getCondition()
  {
    return conditionAnalysis.getOriginalExpression();
  }

  public JoinConditionAnalysis getConditionAnalysis()
  {
    return conditionAnalysis;
  }

  @JsonProperty
  public JoinType getJoinType()
  {
    return joinType;
  }

  @JsonProperty
  @Nullable
  @JsonInclude(Include.NON_NULL)
  public DimFilter getLeftFilter()
  {
    return leftFilter;
  }

  @Nullable
  public JoinableFactoryWrapper getJoinableFactoryWrapper()
  {
    return joinableFactoryWrapper;
  }

  @Override
  public List<DataSource> getChildren()
  {
    return ImmutableList.of(left, right);
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    if (children.size() != 2) {
      throw new IAE("Expected [2] children, got [%d]", children.size());
    }

    return new JoinDataSource(
        children.get(0),
        children.get(1),
        rightPrefix,
        conditionAnalysis,
        joinType,
        leftFilter,
        joinableFactoryWrapper,
        joinAlgorithm
    );
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    return left.isCacheable(isBroker) && right.isCacheable(isBroker);
  }

  @Override
  public boolean isGlobal()
  {
    return left.isGlobal() && right.isGlobal();
  }

  @Override
  public boolean isProcessable()
  {
    return left.isProcessable() && right.isGlobal();
  }

  /**
   * Computes a set of column names for left table expressions in join condition which may already have been defined as
   * a virtual column in the virtual column registry. It helps to remove any extraenous virtual columns created and only
   * use the relevant ones.
   *
   * @return a set of column names which might be virtual columns on left table in join condition
   */
  public Set<String> getVirtualColumnCandidates()
  {
    return getConditionAnalysis().getEquiConditions()
                                 .stream()
                                 .filter(equality -> equality.getLeftExpr() != null)
                                 .map(equality -> equality.getLeftExpr().analyzeInputs().getRequiredBindings())
                                 .flatMap(Set::stream)
                                 .collect(Collectors.toSet());
  }


  @Override
  public byte[] getCacheKey()
  {
    if (true) {
      final CacheKeyBuilder keyBuilder;
      keyBuilder = new CacheKeyBuilder(JoinableFactoryWrapper.JOIN_OPERATION);
      keyBuilder.appendCacheable(leftFilter);
      keyBuilder.appendCacheable(conditionAnalysis);
      keyBuilder.appendCacheable(joinType);
      keyBuilder.appendCacheable(left);
      keyBuilder.appendCacheable(right);
      return keyBuilder.build();
    }

    JoinDataSourceAnalysis analysis = null;//getAnalysis();
    final List<PreJoinableClause> clauses = analysis.getPreJoinableClauses();
    if (clauses.isEmpty()) {
      throw new IAE("No join clauses to build the cache key for data source [%s]", this);
    }

    final CacheKeyBuilder keyBuilder;
    keyBuilder = new CacheKeyBuilder(JoinableFactoryWrapper.JOIN_OPERATION);
    if (analysis.getJoinBaseTableFilter().isPresent()) {
      keyBuilder.appendCacheable(analysis.getJoinBaseTableFilter().get());
    }
    for (PreJoinableClause clause : clauses) {
      final Optional<byte[]> bytes =
          joinableFactoryWrapper.getJoinableFactory()
                                .computeJoinCacheKey(clause.getDataSource(), clause.getCondition());
      if (!bytes.isPresent()) {
        // Encountered a data source which didn't support cache yet
        log.debug("skipping caching for join since [%s] does not support caching", clause.getDataSource());
        return new byte[]{};
      }
      keyBuilder.appendByteArray(bytes.get());
      keyBuilder.appendString(clause.getCondition().getOriginalExpression());
      keyBuilder.appendString(clause.getPrefix());
      keyBuilder.appendString(clause.getJoinType().name());
    }
    return keyBuilder.build();
  }

  @JsonProperty("joinAlgorithm")
  @JsonInclude(Include.NON_NULL)
  private JoinAlgorithm getJoinAlgorithmForSerialization()
  {
    return joinAlgorithm;
  }

  public JoinAlgorithm getJoinAlgorithm()
  {
    return joinAlgorithm == null ? JoinAlgorithm.BROADCAST : joinAlgorithm;
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
    JoinDataSource that = (JoinDataSource) o;
    return Objects.equals(left, that.left) &&
           Objects.equals(right, that.right) &&
           Objects.equals(rightPrefix, that.rightPrefix) &&
           Objects.equals(conditionAnalysis, that.conditionAnalysis) &&
           Objects.equals(leftFilter, that.leftFilter) &&
           joinAlgorithm == that.joinAlgorithm &&
           joinType == that.joinType;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(left, right, rightPrefix, conditionAnalysis, joinType, leftFilter, joinAlgorithm);
  }

  @Override
  public String toString()
  {
    return "JoinDataSource{" +
           "left=" + left +
           ", right=" + right +
           ", rightPrefix='" + rightPrefix + '\'' +
           ", condition=" + conditionAnalysis +
           ", joinType=" + joinType +
           ", leftFilter=" + leftFilter +
           ", joinAlgorithm=" + joinAlgorithm +
           '}';
  }

  /**
   * Creates a Function that maps base segments to {@link HashJoinSegment} if needed (i.e. if the number of join
   * clauses is > 0). If mapping is not needed, this method will return {@link Function#identity()}.
   * @param query
   */
  @Override
  public Function<SegmentReference, SegmentReference> createSegmentMapFunction(Query query)
  {
    JoinDataSourceAnalysis joinAnalysis = getJoinAnalysisForDataSource();
    List<PreJoinableClause> clauses = joinAnalysis.getPreJoinableClauses();
    Filter baseFilter = joinAnalysis.getJoinBaseTableFilter().map(Filters::toFilter).orElse(null);

    if (clauses.isEmpty()) {
      throw DruidException.defensive("A JoinDataSource with no join clauses should not be mapped.");
    }
    final JoinableClauses joinableClauses = JoinableClauses.createClauses(
        clauses,
        joinableFactoryWrapper.getJoinableFactory()
    );
    final JoinFilterRewriteConfig filterRewriteConfig = JoinFilterRewriteConfig.forQuery(query);

    // Pick off any join clauses that can be converted into filters.
    final Set<String> requiredColumns = query.getRequiredColumns();
    final Filter baseFilterToUse;
    final List<JoinableClause> clausesToUse;

    if (requiredColumns != null && filterRewriteConfig.isEnableRewriteJoinToFilter()) {
      final Pair<List<Filter>, List<JoinableClause>> conversionResult = JoinableFactoryWrapper.convertJoinsToFilters(
          joinableClauses.getJoinableClauses(),
          requiredColumns,
          Ints.checkedCast(Math.min(filterRewriteConfig.getFilterRewriteMaxSize(), Integer.MAX_VALUE))
      );

      baseFilterToUse = Filters.maybeAnd(
          Lists.newArrayList(
              Iterables.concat(
                  Collections.singleton(baseFilter),
                  conversionResult.lhs
              )
          )
      ).orElse(null);
      clausesToUse = conversionResult.rhs;

    } else {
      baseFilterToUse = baseFilter;
      clausesToUse = joinableClauses.getJoinableClauses();
    }

    // Analyze remaining join clauses to see if filters on them can be pushed down.
    final JoinFilterPreAnalysis joinFilterPreAnalysis = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
        new JoinFilterPreAnalysisKey(
            filterRewriteConfig,
            clausesToUse,
            query.getVirtualColumns(),
            Filters.maybeAnd(Arrays.asList(baseFilterToUse, Filters.toFilter(query.getFilter())))
                .orElse(null)
        )
    );
    final Function<SegmentReference, SegmentReference> baseMapFn = joinAnalysis.getBaseDataSource().createSegmentMapFunction(query);
    return baseSegment -> createHashJoinSegment(
        baseMapFn.apply(baseSegment),
        baseFilterToUse,
        clausesToUse,
        joinFilterPreAnalysis
    );
  }

  private SegmentReference createHashJoinSegment(
      SegmentReference sourceSegment,
      Filter baseFilterToUse,
      List<JoinableClause> clausesToUse,
      JoinFilterPreAnalysis joinFilterPreAnalysis)
  {
    if (clausesToUse.isEmpty() && baseFilterToUse == null) {
      return sourceSegment;
    }
    return new HashJoinSegment(sourceSegment, baseFilterToUse, clausesToUse, joinFilterPreAnalysis);
  }

  /**
   * Computes the DataSourceAnalysis with join boundaries.
   *
   * It will only process what the join datasource could handle in one go - and not more.
   */
  @VisibleForTesting
  public JoinDataSourceAnalysis getJoinAnalysisForDataSource()
  {
    return JoinDataSourceAnalysis.constructAnalysis(this);
  }

  /**
   * Builds the DataSourceAnalysis for this join.
   *
   * @param vertexBoundary if the returned analysis should go up to the vertex boundary.
   *
   * @throws IllegalArgumentException if dataSource cannot be fully flattened.
   */
  private static JoinDataSourceAnalysis constructAnalysis(final JoinDataSource dataSource, boolean vertexBoundary)
  {
    DataSource current = dataSource;
    DimFilter currentDimFilter = TrueDimFilter.instance();
    final List<PreJoinableClause> preJoinableClauses = new ArrayList<>();

    do {
      if (current instanceof JoinDataSource) {
        final JoinDataSource joinDataSource = (JoinDataSource) current;
        currentDimFilter = DimFilters.conjunction(currentDimFilter, joinDataSource.getLeftFilter());
        PreJoinableClause e = new PreJoinableClause(joinDataSource);
        preJoinableClauses.add(e);
        current = joinDataSource.getLeft();
        continue;
      }
      if (vertexBoundary) {
        if (current instanceof UnnestDataSource) {
          final UnnestDataSource unnestDataSource = (UnnestDataSource) current;
          current = unnestDataSource.getBase();
          continue;
        } else if (current instanceof FilteredDataSource) {
          final FilteredDataSource filteredDataSource = (FilteredDataSource) current;
          current = filteredDataSource.getBase();
          continue;
        }
      }
      break;
    } while (true);

    if (currentDimFilter == TrueDimFilter.instance()) {
      currentDimFilter = null;
    }

    // Join clauses were added in the order we saw them while traversing down, but we need to apply them in the
    // going-up order. So reverse them.
    Collections.reverse(preJoinableClauses);

    return new JoinDataSourceAnalysis(current, null, currentDimFilter, preJoinableClauses, null);
  }


  /**
   * Validates whether the provided leftFilter is permitted to apply to the provided left-hand datasource. Throws an
   * exception if the combination is invalid. Returns the filter if the combination is valid.
   */
  @Nullable
  private static DimFilter validateLeftFilter(final DataSource leftDataSource, @Nullable final DimFilter leftFilter)
  {
    if (leftFilter == null || TrueDimFilter.instance().equals(leftFilter)) {
      return null;
    }
    // Currently we only support leftFilter when applied to concrete leaf datasources (ones with no children).
    // Note that this mean we don't support unions of table, even though this would be reasonable to add in the future.
    Preconditions.checkArgument(
        leftDataSource.isProcessable() && leftDataSource.getChildren().isEmpty(),
        "left filter is only supported if left data source is direct table access"
    );
    return leftFilter;
  }


  /**
   * Analysis of a datasource for purposes of deciding how to execute a particular query.
   *
   * The analysis breaks a datasource down in the following way:
   *
   * <pre>
   *
   *                             Q  <-- Possible query datasource(s) [may be none, or multiple stacked]
   *                             |
   *                             Q  <-- Base query datasource, returned by {@link #getBaseQuery()} if it exists
   *                             |
   *                             J  <-- Possible join tree, expected to be left-leaning
   *                            / \
   *                           J  Dj <--  Other leaf datasources
   *   Base datasource        / \         which will be joined
   *  (bottom-leftmost) -->  Db Dj  <---- into the base datasource
   *
   * </pre>
   *
   * The base datasource (Db) is returned by {@link #getBaseDataSource()}. The other leaf datasources are returned by
   * {@link #getPreJoinableClauses()}.
   *
   * The base datasource (Db) will never be a join, but it can be any other type of datasource (table, query, etc).
   * Note that join trees are only flattened if they occur at the top of the overall tree (or underneath an outer query),
   * and that join trees are only flattened to the degree that they are left-leaning. Due to these facts, it is possible
   * for the base or leaf datasources to include additional joins.
   *
   * The base datasource is the one that will be considered by the core Druid query stack for scanning via
   * {@link org.apache.druid.segment.Segment} and {@link org.apache.druid.segment.CursorFactory}. The other leaf
   * datasources must be joinable onto the base data.
   *
   * The idea here is to keep things simple and dumb. So we focus only on identifying left-leaning join trees, which map
   * neatly onto a series of hash table lookups at query time. The user/system generating the queries, e.g. the druid-sql
   * layer (or the end user in the case of native queries), is responsible for containing the smarts to structure the
   * tree in a way that will lead to optimal execution.
   */
  public static class JoinDataSourceAnalysis
  {
    private final DataSource baseDataSource;
    @Nullable
    private final DimFilter joinBaseTableFilter;
    private final List<PreJoinableClause> preJoinableClauses;

    public JoinDataSourceAnalysis(
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
      this.joinBaseTableFilter = joinBaseTableFilter;
      this.preJoinableClauses = preJoinableClauses;
    }

    /**
     * Returns the base (bottom-leftmost) datasource.
     */
    public DataSource getBaseDataSource()
    {
      return baseDataSource;
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
     * Returns join clauses corresponding to joinable leaf datasources (every leaf except the bottom-leftmost).
     */
    public List<PreJoinableClause> getPreJoinableClauses()
    {
      return preJoinableClauses;
    }
    /**
     * Returns whether "column" on the analyzed datasource refers to a column from the base datasource.
     */
    public boolean isBaseColumn(final String column)
    {
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
      JoinDataSourceAnalysis that = (JoinDataSourceAnalysis) o;
      return Objects.equals(baseDataSource, that.baseDataSource);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(baseDataSource);
    }

    @Override
    public String toString()
    {
      return "DataSourceAnalysis{" +
             ", baseDataSource=" + baseDataSource +
             ", preJoinableClauses=" + preJoinableClauses +
             '}';
    }


    public static JoinDataSourceAnalysis constructAnalysis(final JoinDataSource dataSource)
    {
      DataSource current = dataSource;
      DimFilter currentDimFilter = TrueDimFilter.instance();
      final List<PreJoinableClause> preJoinableClauses = new ArrayList<>();

      do {
        if (current instanceof JoinDataSource) {
          final JoinDataSource joinDataSource = (JoinDataSource) current;
          currentDimFilter = DimFilters.conjunction(currentDimFilter, joinDataSource.getLeftFilter());
          PreJoinableClause e = new PreJoinableClause(joinDataSource);
          preJoinableClauses.add(e);
          current = joinDataSource.getLeft();
          continue;
        }
        break;
      } while (true);

      if (currentDimFilter == TrueDimFilter.instance()) {
        currentDimFilter = null;
      }

      // Join clauses were added in the order we saw them while traversing down, but we need to apply them in the
      // going-up order. So reverse them.
      Collections.reverse(preJoinableClauses);

      return new JoinDataSourceAnalysis(current, null, currentDimFilter, preJoinableClauses, null);
    }
  }
}
