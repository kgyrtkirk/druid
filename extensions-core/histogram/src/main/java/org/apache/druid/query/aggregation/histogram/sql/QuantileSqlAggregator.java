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

package org.apache.druid.query.aggregation.histogram.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Optionality;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.histogram.ApproximateHistogram;
import org.apache.druid.query.aggregation.histogram.ApproximateHistogramAggregatorFactory;
import org.apache.druid.query.aggregation.histogram.ApproximateHistogramFoldingAggregatorFactory;
import org.apache.druid.query.aggregation.histogram.QuantilePostAggregator;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.aggregation.Aggregation;
import org.apache.druid.sql.calcite.aggregation.Aggregations;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.DefaultOperandTypeChecker;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.InputAccessor;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.List;

public class QuantileSqlAggregator implements SqlAggregator
{
  private static final SqlAggFunction FUNCTION_INSTANCE = new QuantileSqlAggFunction();
  private static final String NAME = "APPROX_QUANTILE";

  @Override
  public SqlAggFunction calciteFunction()
  {
    return FUNCTION_INSTANCE;
  }

  @Nullable
  @Override
  public Aggregation toDruidAggregation(
      final PlannerContext plannerContext,
      final VirtualColumnRegistry virtualColumnRegistry,
      final String name,
      final AggregateCall aggregateCall,
      final InputAccessor inputAccessor,
      final List<Aggregation> existingAggregations,
      final boolean finalizeAggregations
  )
  {
    final DruidExpression input = Aggregations.toDruidExpressionForNumericAggregator(
        plannerContext,
        inputAccessor.getInputRowSignature(),
        inputAccessor.getField(aggregateCall.getArgList().get(0))
    );
    if (input == null) {
      return null;
    }

    final AggregatorFactory aggregatorFactory;
    final String histogramName = StringUtils.format("%s:agg", name);
    final RexNode probabilityArg = inputAccessor.getField(aggregateCall.getArgList().get(1));

    if (!probabilityArg.isA(SqlKind.LITERAL)) {
      // Probability must be a literal in order to plan.
      return null;
    }

    final float probability = ((Number) RexLiteral.value(probabilityArg)).floatValue();
    final int resolution;

    if (aggregateCall.getArgList().size() >= 3) {
      final RexNode resolutionArg = inputAccessor.getField(aggregateCall.getArgList().get(2));

      if (!resolutionArg.isA(SqlKind.LITERAL)) {
        // Resolution must be a literal in order to plan.
        return null;
      }

      resolution = ((Number) RexLiteral.value(resolutionArg)).intValue();
    } else {
      resolution = ApproximateHistogram.DEFAULT_HISTOGRAM_SIZE;
    }

    final int numBuckets = ApproximateHistogram.DEFAULT_BUCKET_SIZE;
    final float lowerLimit = Float.NEGATIVE_INFINITY;
    final float upperLimit = Float.POSITIVE_INFINITY;

    // Look for existing matching aggregatorFactory.
    for (final Aggregation existing : existingAggregations) {
      for (AggregatorFactory factory : existing.getAggregatorFactories()) {
        if (factory instanceof ApproximateHistogramAggregatorFactory) {
          final ApproximateHistogramAggregatorFactory theFactory = (ApproximateHistogramAggregatorFactory) factory;

          // Check input for equivalence.
          final boolean inputMatches;
          final DruidExpression virtualInput =
              virtualColumnRegistry.findVirtualColumnExpressions(theFactory.requiredFields())
                                   .stream()
                                   .findFirst()
                                   .orElse(null);

          if (virtualInput == null) {
            inputMatches = input.isDirectColumnAccess()
                           && input.getDirectColumn().equals(theFactory.getFieldName());
          } else {
            inputMatches = virtualInput.equals(input);
          }

          final boolean matches = inputMatches
                                  && theFactory.getResolution() == resolution
                                  && theFactory.getNumBuckets() == numBuckets
                                  && theFactory.getLowerLimit() == lowerLimit
                                  && theFactory.getUpperLimit() == upperLimit;

          if (matches) {
            // Found existing one. Use this.
            return Aggregation.create(
                ImmutableList.of(),
                new QuantilePostAggregator(name, factory.getName(), probability)
            );
          }
        }
      }
    }

    // No existing match found. Create a new one.
    if (input.isDirectColumnAccess()) {
      if (inputAccessor.getInputRowSignature()
                       .getColumnType(input.getDirectColumn())
                       .map(type -> type.is(ValueType.COMPLEX))
                       .orElse(false)) {
        aggregatorFactory = new ApproximateHistogramFoldingAggregatorFactory(
            histogramName,
            input.getDirectColumn(),
            resolution,
            numBuckets,
            lowerLimit,
            upperLimit,
            false
        );
      } else {
        aggregatorFactory = new ApproximateHistogramAggregatorFactory(
            histogramName,
            input.getDirectColumn(),
            resolution,
            numBuckets,
            lowerLimit,
            upperLimit,
            false
        );
      }
    } else {
      final String virtualColumnName =
          virtualColumnRegistry.getOrCreateVirtualColumnForExpression(input, ColumnType.FLOAT);
      aggregatorFactory = new ApproximateHistogramAggregatorFactory(
          histogramName,
          virtualColumnName,
          resolution,
          numBuckets,
          lowerLimit,
          upperLimit,
          false
      );
    }

    return Aggregation.create(
        ImmutableList.of(aggregatorFactory),
        new QuantilePostAggregator(name, histogramName, probability)
    );
  }

  private static class QuantileSqlAggFunction extends SqlAggFunction
  {
    QuantileSqlAggFunction()
    {
      super(
          NAME,
          null,
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.explicit(SqlTypeName.DOUBLE),
          null,
          // Checks for signatures like 'APPROX_QUANTILE(column, probability)' and
          // 'APPROX_QUANTILE(column, probability, resolution)'
          DefaultOperandTypeChecker
              .builder()
              .operandNames("column", "probability", "resolution")
              .operandTypes(SqlTypeFamily.ANY, SqlTypeFamily.NUMERIC, SqlTypeFamily.EXACT_NUMERIC)
              .requiredOperandCount(2)
              .literalOperands(1, 2)
              .build(),
          SqlFunctionCategory.NUMERIC,
          false,
          false,
          Optionality.FORBIDDEN
      );
    }
  }
}
