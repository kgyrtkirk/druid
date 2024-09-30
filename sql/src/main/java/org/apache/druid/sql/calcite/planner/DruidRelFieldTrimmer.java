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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.druid.sql.calcite.rule.logical.LogicalUnnest;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.LinkedHashSet;
import java.util.Set;

public class DruidRelFieldTrimmer extends RelFieldTrimmer
{
  public DruidRelFieldTrimmer(@Nullable SqlValidator validator, RelBuilder relBuilder)
  {
    super(validator, relBuilder);
  }

  public TrimResult trimFields(LogicalUnnest unnest,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields)
  {
    LogicalUnnest filter = unnest;
    final RelDataType rowType = filter.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RexNode conditionExpr = filter.getFilter();
    final RelNode input = filter.getInput();

    // We use the fields used by the consumer, plus any fields used in the
    // filter.
    final Set<RelDataTypeField> inputExtraFields =
        new LinkedHashSet<>(extraFields);
    RelOptUtil.InputFinder inputFinder =
        new RelOptUtil.InputFinder(inputExtraFields, fieldsUsed);
    RexNode unnestExpr = filter.getUnnestExpr();
    unnestExpr.accept(inputFinder);
    if (conditionExpr != null) {
      conditionExpr.accept(inputFinder);
    }
    final ImmutableBitSet inputFieldsUsed = inputFinder.build();

    // Create input with trimmed columns.
    TrimResult trimResult =
        trimChild(filter, input, inputFieldsUsed, inputExtraFields);
    RelNode newInput = trimResult.left;
    final Mapping inputMapping = trimResult.right;

    // If the input is unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newInput == input
        && fieldsUsed.cardinality() == fieldCount) {
      return result(filter, Mappings.createIdentity(fieldCount));
    }

    // Build new logicalunnest and populate the mapping.
    final RexVisitor<RexNode> shuttle =
        new RexPermuteInputsShuttle(inputMapping, newInput);
    RexNode newConditionExpr = null;
    if(conditionExpr!=null) {
      newConditionExpr = conditionExpr.accept(shuttle);
    }
    RexNode newUnnestExpr = unnestExpr.accept(shuttle);

    // Build new filter with trimmed input and condition.
    RelBuilder relBuilder = null;
    relBuilder.push(newInput)
        .filter(filter.getVariablesSet(), newConditionExpr);

    // FIXME: inputMapping doesn't account for new col

    // The result has the same mapping as the input gave us. Sometimes we
    // return fields that the consumer didn't ask for, because the filter
    // needs them for its condition.
    return result(relBuilder.build(), inputMapping, filter);
  }

}
