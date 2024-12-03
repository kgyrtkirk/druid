/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.druid.sql.calcite.rule;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.druid.sql.calcite.rule.DruidAggregateCaseToFilterRule.AggregateCallTransform.LocalAggBuilder;
import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.rex.RexLiteral.isNullLiteral;

/**
 * Rule that converts CASE-style filtered aggregates into true filtered
 * aggregates.
 *
 * <p>For example,
 *
 * <blockquote>
 *   <code>SELECT SUM(CASE WHEN gender = 'F' THEN salary END)<br>
 *   FROM Emp</code>
 * </blockquote>
 *
 * <p>becomes
 *
 * <blockquote>
 *   <code>SELECT SUM(salary) FILTER (WHERE gender = 'F')<br>
 *   FROM Emp</code>
 * </blockquote>
 *
 * @see CoreRules#AGGREGATE_CASE_TO_FILTER
 */
@Value.Enclosing
public class DruidAggregateCaseToFilterRule extends RelRule<DruidAggregateCaseToFilterRule.Config>
    implements TransformationRule {
  public static final AggregateCallTransform FILTERED_DISTINCT = new FilteredDistinctTransform();
  public static final AggregateCallTransform FILTERED_COUNT = new FilteredCountTransform();
  public static final AggregateCallTransform FILTERED_AGGREGATION =
      new FilteredAggregationTransform();
  public static final AggregateCallTransform FILTERED_SUM = new FilteredSumTransform();

  public static final List<AggregateCallTransform> DEFAULT_TRANSFORMS =
      ImmutableList.of(FILTERED_DISTINCT, FILTERED_COUNT, FILTERED_AGGREGATION, FILTERED_SUM);

  /** Creates an AggregateCaseToFilterRule. */
  protected DruidAggregateCaseToFilterRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  protected DruidAggregateCaseToFilterRule(RelBuilderFactory relBuilderFactory,
      String description) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .withDescription(description).as(Config.class));
  }

  @Override public boolean matches(final RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);
    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      for (AggregateCallTransform transform : config.transforms()) {
        if (transform.matches(aggregateCall, project)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);

    LocalAggBuilder lab = new LocalAggBuilder(call.builder(), aggregate, project);

    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      RexNode expr = transform(lab, aggregateCall);
      lab.projectAboveAgg(expr);
    }

    if (lab.aggs.equals(aggregate.getAggCallList())) {
      // no progress
      return;
    }

    RelNode newRel = lab.build();

    call.transformTo(newRel);
    call.getPlanner().prune(aggregate);
  }

  public RexNode transform(LocalAggBuilder lab, AggregateCall aggregateCall) {
    for (AggregateCallTransform t : config.transforms()) {
      @Nullable
      RexNode expr = t.transform(lab, aggregateCall);
      if (expr != null) {
        return expr;
      }
    }
    return lab.addAggregation(aggregateCall);
  }

  /**
   * Provides facilities to implement Aggregate rewrites with a {@link Project} below and above.
   *
   * {@link LocalAggBuilder} should be used to create the new rewritten {@link Aggregate}.
   */
  public interface AggregateCallTransform {
    /**
     * Helper class to aid building the output {@link Aggregate}.
     *
     * Keeps track of things to help build the new aggregates vertically.
     *
     * Constructed layout is:
     * <pre>
     * Project( $projectsAbove )
     *   Aggregate( $aggs )
     *     Project( $projectsBelow )
     * </pre>
     */
    class LocalAggBuilder {
      private RelBuilder builder;
      private List<RexNode> projectsAbove = new ArrayList<>();
      private List<AggregateCall> aggs = new ArrayList<>();
      private List<RexNode> projectsBelow;
      private Project oldProject;
      private Aggregate oldAggregate;

      public LocalAggBuilder(RelBuilder builder, Aggregate oldAggregate, Project oldProject) {
        this.builder = builder;
        this.oldAggregate = oldAggregate;
        this.oldProject = oldProject;
        this.projectsBelow = new ArrayList<>(oldProject.getProjects());
        this.projectsAbove = createProjectsForGroupKeys(oldAggregate);
      }

      private static List<RexNode> createProjectsForGroupKeys(Aggregate agg) {
        List<RexNode> ret = new ArrayList<>();
        for (int i = 0; i < agg.getGroupCount(); i++) {
          ret.add(RexInputRef.of(i, agg.getRowType()));
        }
        return ret;
      }

      public RexBuilder getRexBuilder() {
        return builder.getRexBuilder();
      }

      /**
       * Makes the expression available at the returned input index.
       *
       * Returns the index of the projected expression.
       */
      public int projectBelowAgg(RexNode expr) {
        projectsBelow.add(expr);
        return projectsBelow.size() - 1;
      }

      public void projectAboveAgg(RexNode expr) {
        projectsAbove.add(expr);
      }

      public int projectCombinedFilter(AggregateCall call, RexNode condition) {
        return projectBelowAgg(buildCombinedFilter(call, condition));
      }

      protected RexNode buildCombinedFilter(AggregateCall call, RexNode condition) {
        if (call.filterArg < 0) {
          return condition;
        }
        RexNode oldFilterExpr = oldProject.getProjects().get(call.filterArg);
        return RexUtil.composeConjunction(getRexBuilder(),
            ImmutableList.of(condition, oldFilterExpr));
      }

      public RelNode build() {
        final RelBuilder relBuilder =
            builder.push(oldProject.getInput()).project(projectsBelow);

        final RelBuilder.GroupKey groupKey = relBuilder
            .groupKey(oldAggregate.getGroupSet(), oldAggregate.getGroupSets());

        relBuilder.aggregate(groupKey, aggs).project(projectsAbove)
            .convert(oldAggregate.getRowType(), false);

        return relBuilder.build();
      }

      public RelDataTypeFactory getTypeFactory() {
        return builder.getTypeFactory();
      }

      public RexNode addAggregation(@Nullable AggregateCall agg) {
        aggs.add(agg);
        return new RexInputRef(oldAggregate.getGroupCount() + aggs.size() - 1, agg.getType());
      }

      public RexNode getAggFilterExpr(AggregateCall call) {
        if (call.filterArg < 0) {
          return null;
        }
        return oldProject.getProjects().get(call.filterArg);
      }
    }

    @Nullable RexNode transform(LocalAggBuilder lab, AggregateCall call);

    boolean matches(AggregateCall aggregateCall, Project project);
  }

  /**
   * Aggregate rewrites specialized to target AGG( CASE {COND} THEN {LEFT} ELSE {RIGHT} END ).
   *
   * The inner CASE statement is presented as a {@link RexIf} class to the internal implementations.
   */
  public abstract static class ThreeArgCaseBasedAggregateCallTransform
      implements AggregateCallTransform {

    /**
     * Describes a 2 branched CASE statement (IF).
     */
    protected static class RexIf {
      public final RexNode condition;
      public final RexNode left;
      public final RexNode right;

      public RexIf(RexNode condition, RexNode left, RexNode right) {
        this.condition = condition;
        this.left = left;
        this.right = right;
      }

      public static RexIf of(RexNode rexNode) {
        if (!isThreeArgCase(rexNode)) {
          return null;
        }
        List<RexNode> operands = ((RexCall) rexNode).operands;
        return new RexIf(operands.get(0), operands.get(1), operands.get(2));
      }

      private static boolean isThreeArgCase(final RexNode rexNode) {
        return rexNode.getKind() == SqlKind.CASE && ((RexCall) rexNode).operands.size() == 3;
      }

      /**
       * Makes a null literal if the node is 0.
       */
      public RexNode nullIfZero(RexBuilder rexBuilder, RexNode node) {
        if (isIntLiteral(node, BigDecimal.ZERO)) {
          return rexBuilder.makeNullLiteral(node.getType());
        }
        return node;
      }

      /**
       * Normalizes the conditional.
       *
       * Swaps branches to put the null to the end.
       */
      public RexIf normalize(RexBuilder rexBuilder, boolean treatZeroAsNull) {
        RexNode newLeft = treatZeroAsNull ? nullIfZero(rexBuilder, left) : left;
        RexNode newRight = treatZeroAsNull ? nullIfZero(rexBuilder, right) : right;

        if (isNullLiteral(left) && !isNullLiteral(right)) {
          // Flip the conditional to put the `null` on the else side.
          return new RexIf(
              rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_TRUE, condition),
              newRight, newLeft);
        }
        if (condition.getType().isNullable()) {
          return new RexIf(
              rexBuilder.makeCall(SqlStdOperatorTable.IS_TRUE, condition),
              newLeft, newRight);
        }
        return new RexIf(condition, newLeft, newRight);
      }
    }

    @Override public final @Nullable RexNode transform(LocalAggBuilder lab, AggregateCall aggregateCall) {
      RexIf rexIf = extractRexIf(aggregateCall, lab.oldProject);
      if (rexIf == null || !matches(aggregateCall, rexIf)) {
        return null;
      }
      return transform(lab, aggregateCall, rexIf);
    }

    @Override public final boolean matches(AggregateCall aggregateCall, Project project) {
      RexIf rexIf = extractRexIf(aggregateCall, project);
      if (rexIf == null) {
        return false;
      }
      return matches(aggregateCall, rexIf);
    }

    /**
     * Rewrites the passed {@link AggregateCall} to an alternate.
     *
     * It must register new expressions with the {@link LocalAggBuilder}. Only
     * called if {@link #matches(AggregateCall, RexIf)} is true.
     * May return null to back-out from the transformation.
     */
    protected abstract @Nullable RexNode transform(LocalAggBuilder lab,
        AggregateCall call, RexIf rexIf);

    protected abstract boolean matches(AggregateCall aggregateCall, RexIf rexIf);

    protected static RexIf extractRexIf(AggregateCall aggregateCall,
        Project project) {
      if (aggregateCall.getArgList().size() != 1) {
        return null;
      }
      Integer argIndex = aggregateCall.getArgList().get(0);
      final RexNode rexNode = project.getProjects().get(argIndex);
      RexIf rexIf = RexIf.of(rexNode);
      if (rexIf == null) {
        return null;
      }
      RexBuilder rexBuilder = project.getCluster().getRexBuilder();
      return rexIf.normalize(rexBuilder,
          aggregateCall.getAggregation().getKind() == SqlKind.SUM0);
    }
  }

  /**
   * Recognizes conditionally filtered distinct.
   *
   * <pre>
   * COUNT(DISTINCT CASE WHEN x = 'foo' THEN y END)
   *  =>
   * COUNT(DISTINCT y) FILTER(WHERE x = 'foo')
   * </pre>
   */
  protected static class FilteredDistinctTransform
      extends ThreeArgCaseBasedAggregateCallTransform {
    @Override protected boolean matches(AggregateCall aggregateCall, RexIf rexIf) {
      SqlKind kind = aggregateCall.getAggregation().getKind();
      return aggregateCall.isDistinct() && kind == SqlKind.COUNT
          && isNullLiteral(rexIf.right);
    }

    @Override protected @Nullable RexNode transform(LocalAggBuilder lab,
        AggregateCall call, RexIf rexIf) {
      int leftIndex = lab.projectBelowAgg(rexIf.left);
      int filterIndex = lab.projectCombinedFilter(call, rexIf.condition);
      final RelDataType dataType = makeNotNullableBigIntType(lab.getRexBuilder());
      AggregateCall agg =
          AggregateCall.create(SqlStdOperatorTable.COUNT, true, false, false,
              call.rexList, ImmutableList.of(leftIndex), filterIndex, null,
              RelCollations.EMPTY, dataType, call.getName());
      return lab.addAggregation(agg);
    }
  }

  /**
   * Recognizes conditionally filtered distinct.
   *
   * <pre>
  * SUM0(CASE WHEN x = 'foo' THEN 1 END)
  *   => COUNT() FILTER (x = 'foo')
  * COUNT(CASE WHEN x = 'foo' THEN 'dummy' END)
  *    => COUNT() FILTER (x = 'foo')
   * </pre>
   *
   * note:
   *
   * <pre>
   * SUM0(CASE WHEN x = 'foo' THEN 1 ELSE 0 END)
   * </pre>
   *
   * is also handled as the `0` branch was normalized.
   */
  protected static class FilteredCountTransform
      extends ThreeArgCaseBasedAggregateCallTransform {

    protected boolean matches(AggregateCall call, RexIf rexIf) {
      SqlKind kind = call.getAggregation().getKind();
      return !call.isDistinct()
          && ((kind == SqlKind.SUM0 && isIntLiteral(rexIf.left, BigDecimal.ONE))
              || kind == SqlKind.COUNT)
          && isNullLiteral(rexIf.right) && call.getAggregation().allowsFilter();
    }

    @Override protected @Nullable RexNode transform(LocalAggBuilder lab,
        AggregateCall call, RexIf rexIf) {
      int filterIdx = lab.projectCombinedFilter(call, rexIf.condition);
      final RelDataType dataType = makeNotNullableBigIntType(lab.getRexBuilder());
      AggregateCall agg = AggregateCall.create(SqlStdOperatorTable.COUNT,
          false, false, false, call.rexList, ImmutableList.of(), filterIdx,
          null, RelCollations.EMPTY, dataType, call.getName());
      return lab.addAggregation(agg);
    }
  }

  private static RelDataType makeNotNullableBigIntType(RexBuilder rexBuilder) {
    final RelDataTypeFactory typeFactory =
        rexBuilder.getTypeFactory();
    RelDataType bigIntType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    final RelDataType dataType =
        typeFactory.createTypeWithNullability(bigIntType, false);
    return dataType;
  }

  /**
   * Recognizes conditionally filtered aggregations.
   *
   * <pre>
   * AGG(CASE WHEN x = 'foo' THEN expr END)
   *  =>
   * AGG(expr) FILTER (x = 'foo')
   * </pre>
   */
  protected static class FilteredAggregationTransform
      extends ThreeArgCaseBasedAggregateCallTransform {
    @Override protected @Nullable RexNode transform(LocalAggBuilder lab,
        AggregateCall call, RexIf rexIf) {
      int argIdx = lab.projectBelowAgg(rexIf.left);
      int filterIdx = lab.projectCombinedFilter(call, rexIf.condition);
      AggregateCall agg =
          AggregateCall.create(call.getAggregation(), false, false, false, call.rexList,
          ImmutableList.of(argIdx), filterIdx, null, RelCollations.EMPTY,
          call.getType(), call.getName());
      return lab.addAggregation(agg);
    }

    @Override protected boolean matches(AggregateCall call, RexIf rexIf) {
      if (call.isDistinct()) {
        return false;
      }
      return isNullLiteral(rexIf.right)
          && call.getAggregation().allowsFilter();
    }
  }

  /**
   * Recognizes conditionally filtered summation.
   *
   * <pre>
   * SUM(CASE WHEN x = 'foo' THEN value ELSE 0 END) FILTER (F)
   *  =>
   * CASE WHEN COUNT() FILTER (F) = 0 THEN NULL ELSE SUM(value) FILTER (F AND x='foo') END
   * </pre>
   */
  protected static class FilteredSumTransform
      extends ThreeArgCaseBasedAggregateCallTransform {
    @Override protected @Nullable RexNode transform(LocalAggBuilder lab,
        AggregateCall call, RexIf rexIf) {

      RexBuilder rexBuilder = lab.getRexBuilder();
      int argIdx = lab.projectBelowAgg(rexIf.left);
      RexNode aggFilterExpr = lab.getAggFilterExpr(call);
      int countFilterIdx =
          aggFilterExpr == null ? -1 : lab.projectBelowAgg(aggFilterExpr);
      int sumFilterIdx =
          lab.projectCombinedFilter(call, rexIf.condition);
      RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
      AggregateCall sumAggCall =
          AggregateCall.create(call.getAggregation(),
          false, false, false, call.rexList,
          ImmutableList.of(argIdx), sumFilterIdx, null, RelCollations.EMPTY,
          typeFactory.createTypeWithNullability(call.getType(), true), call.getName() + "_sum");
      AggregateCall countAggCall =
          AggregateCall.create(
              SqlStdOperatorTable.COUNT, false, false, false, call.rexList,
              ImmutableList.of(), countFilterIdx, null, RelCollations.EMPTY,
              makeNotNullableBigIntType(rexBuilder), call.getName() + "_count");

      RexNode sumAgg = lab.addAggregation(sumAggCall);
      RexNode countAgg = lab.addAggregation(countAggCall);

      return rexBuilder.makeCall(SqlStdOperatorTable.CASE,
          rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, countAgg,
              rexBuilder.makeBigintLiteral(BigDecimal.ZERO)),
          rexBuilder.makeNullLiteral(sumAgg.getType()),
          rexBuilder.makeCall(SqlStdOperatorTable.COALESCE, sumAgg,
              rexBuilder.makeCast(sumAgg.getType(),
                  rexBuilder.makeBigintLiteral(BigDecimal.ZERO))));

    }

    @Override protected boolean matches(AggregateCall call, RexIf rexIf) {
      if (call.isDistinct()) {
        return false;
      }
      return isIntLiteral(rexIf.right, BigDecimal.ZERO)
          && call.getAggregation().getKind() == SqlKind.SUM
          && call.getAggregation().allowsFilter();
    }
  }

  private static boolean isIntLiteral(RexNode rexNode, BigDecimal value) {
    return rexNode instanceof RexLiteral
        && SqlTypeName.INT_TYPES.contains(rexNode.getType().getSqlTypeName())
        && value.equals(((RexLiteral) rexNode).getValueAs(BigDecimal.class));
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableDruidAggregateCaseToFilterRule.Config.builder().build()
        .withOperandSupplier(b0 -> b0.operand(Aggregate.class)
            .oneInput(b1 -> b1.operand(Project.class).anyInputs()));

    @Value.Default
    default List<AggregateCallTransform> transforms() {
      return DEFAULT_TRANSFORMS;
    }

    /** Sets {@link #transforms()}. */
    Config withTransforms(AggregateCallTransform... elements);

    /** Sets {@link #transforms()}. */
    Config withTransforms(Iterable<? extends AggregateCallTransform> elements);

    @Override default DruidAggregateCaseToFilterRule toRule() {
      return new DruidAggregateCaseToFilterRule(this);
    }
  }
}
