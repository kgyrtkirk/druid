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
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
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

public TrimResult trimFields(LogicalCorrelate correlate,
      ImmutableBitSet fieldsUsed,
      Set<RelDataTypeField> extraFields)
  {

    final RelDataType rowType = correlate.getRowType();
    final int fieldCount = rowType.getFieldCount();
    final RelNode left = correlate.getLeft();
    final RelNode right = correlate.getRight();
    final RelDataType rightRowType = right.getRowType();
    final int rightFieldCount = rightRowType.getFieldCount();

    // We use the fields used by the consumer, plus any fields used in the
    // filter.
    final Set<RelDataTypeField> leftExtraFields =
        new LinkedHashSet<>(extraFields);
    final Set<RelDataTypeField> rightExtraFields =
        new LinkedHashSet<>(extraFields);
    RelOptUtil.InputFinder inputFinder =
        new RelOptUtil.InputFinder(leftExtraFields, fieldsUsed);
//    conditionExpr.accept(inputFinder);
    final ImmutableBitSet leftFieldsUsed = inputFinder.build();
    inputFinder = new RelOptUtil.InputFinder(rightExtraFields, fieldsUsed);
//    conditionExpr.accept(inputFinder);
    final ImmutableBitSet rightFieldsUsed = inputFinder.build();


    leftFieldsUsed = leftFieldsUsed.union(correlate.getRequiredColumns());

    //    leftFieldsUsed.(correlate.getRequiredColumns());
    // Create left input with trimmed columns.
    TrimResult leftTrimResult =
        trimChild(correlate, left, leftFieldsUsed, leftExtraFields);
    RelNode newLeft = leftTrimResult.left;
    final Mapping leftMapping = leftTrimResult.right;

    // Create right input with trimmed columns.
    TrimResult rightTrimResult =
        trimChild(correlate, right, rightFieldsUsed, rightExtraFields);
    RelNode newRight = rightTrimResult.left;
    final Mapping rightMapping = rightTrimResult.right;

    // If the inputs are unchanged, and we need to project all columns,
    // there's nothing we can do.
    if (newLeft == left
        && newRight == right
        && fieldsUsed.cardinality() == fieldCount) {
      return result(correlate, Mappings.createIdentity(fieldCount));
    }

    // Build new correlate and populate the mapping.
    final RexVisitor<RexNode> shuttle = null;

//        new RexPermuteInputsShuttle(leftMapping, newLeft, rightMapping, newRight);
//    RexNode newConditionExpr = conditionExpr.accept(shuttle);

    // Build new correlate with trimmed inputs and condition.
    final LogicalCorrelate newCorrelate =
        correlate.copy(correlate.getTraitSet(),
            newLeft,
            newRight, correlate.getCorrelationId(),
            correlate.getRequiredColumns(), correlate.getJoinType());

    return result(newCorrelate, mapping);
  }

}
