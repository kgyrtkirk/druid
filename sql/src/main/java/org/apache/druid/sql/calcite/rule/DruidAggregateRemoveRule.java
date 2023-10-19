package org.apache.druid.sql.calcite.rule;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

public class DruidAggregateRemoveRule extends AggregateRemoveRule
{
  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Aggregate aggregate = call.rel(0);
    final RelNode input = aggregate.getInput();
    final RelMetadataQuery mq = call.getMetadataQuery();
    List<RelCollation> collations = mq.collations(input);
    assert collations != null;
    boolean status = false;
    // checking if the input has a sort
    for (RelCollation r : collations) {
      final List<RelFieldCollation> fieldCollations = r.getFieldCollations();
      if (!fieldCollations.isEmpty()) {
        for (RelFieldCollation relFieldCollation : fieldCollations) {
          if (relFieldCollation.getDirection() != null){
            status = true;
          }
        }
      }
    }
    if (status)
      return;
    super.onMatch(call);
  }

  public static DruidAggregateRemoveRule toRule() {
    return new DruidAggregateRemoveRule(Config.DEFAULT);
  }

  protected DruidAggregateRemoveRule(Config config)
  {
    super(config);
  }

}
