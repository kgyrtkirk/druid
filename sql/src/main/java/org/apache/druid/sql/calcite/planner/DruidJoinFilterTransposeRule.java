package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.SemiJoinFilterTransposeRule;
import org.apache.calcite.rel.rules.SubstitutionRule;

public class DruidJoinFilterTransposeRule
    extends RelRule<SemiJoinFilterTransposeRule.Config>
    implements SubstitutionRule {


  @Override
  public boolean autoPruneOld()
  {
    return true;

  }
  //~ Methods ----------------------------------------------------------------

//  public interface Config extends SemiJoinFilterTransposeRule.Config {
//
//  }
//    Config DEFAULT = Config
//        .withOperandSupplier(b0 ->
//            b0.operand(Aggregate.class).oneInput(b1 ->
//                b1.operand(Project.class).anyInputs()));
//
//
//    @Override default AggregateCaseToFilterRule toRule() {
//      return new AggregateCaseToFilterRule(this);
//    }
//  }


  protected DruidJoinFilterTransposeRule()
  {
    super(SemiJoinFilterTransposeRule.Config.DEFAULT
        .withOperandSupplier(b0 ->
        b0.operand(Join.class).inputs(b1 ->
            b1.operand(Filter.class).anyInputs()))
        .as(SemiJoinFilterTransposeRule.Config.class)
        );
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);
    final Filter filter = call.rel(1);

    Join newJoin = join.copy(join.getTraitSet(),
        ImmutableList.of(
              filter.getInput(),
              join.getInput(1)
            )
        );

    final RelFactories.FilterFactory factory =
        RelFactories.DEFAULT_FILTER_FACTORY;
    RelNode newFilter =
        factory.createFilter(newJoin, filter.getCondition(),
            ImmutableSet.of());

    call.transformTo(newFilter);
  }

}
