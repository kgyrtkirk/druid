/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.druid.sql.calcite.planner.convertlet;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCoalesceFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.PlannerContext;

import com.google.common.collect.ImmutableList;

public class DruidCoalesceConvertlet implements DruidConvertletFactory, SqlRexConvertlet
{
  public static final DruidConvertletFactory CONVERTLET_INSTANCE = new DruidCoalesceConvertlet();

  @Override
  public SqlRexConvertlet createConvertlet(PlannerContext plannerContext)
  {
    return this;
  }

  @Override
  public List<SqlOperator> operators()
  {
    return ImmutableList.of(DruidCoalesceFunction.INSTANCE);
  }

  public static class DruidCoalesceFunction extends SqlCoalesceFunction
  {
    public static final DruidCoalesceFunction INSTANCE = new DruidCoalesceFunction();

    @Override
    public SqlNode rewriteCall(SqlValidator validator, SqlCall call)
    {
      validateQuantifier(validator, call); // check DISTINCT/ALL

      List<SqlNode> operands = call.getOperandList();

      if (operands.size() == 1) {
        return operands.get(0);
      }

      return call;
    }
  }

  @Override
  public RexNode convertCall(SqlRexContext cx, SqlCall call)
  {
    RexBuilder rexBuilder = cx.getRexBuilder();
    List<RexNode> exprList = new ArrayList<>();
    for (SqlNode operand : call.getOperandList()) {
      exprList.add(cx.convertExpression(operand));
    }
    RelDataType type = rexBuilder.deriveReturnType(call.getOperator(), exprList);
    return rexBuilder.makeCall(type, SqlStdOperatorTable.COALESCE, exprList);
  }

  public static class CoalesceOperatorConversion implements SqlOperatorConversion
  {
    @Override
    public SqlOperator calciteOperator()
    {
      return SqlStdOperatorTable.COALESCE;
    }

    @Override
    @Nullable
    public DruidExpression toDruidExpression(
        final PlannerContext plannerContext,
        final RowSignature rowSignature,
        final RexNode rexNode)
    {
      final RexCall call = (RexCall) rexNode;

      if (call.getOperands().size() == 2) {
        return OperatorConversions.convertDirectCall(plannerContext, rowSignature, call, "nvl");
      } else {
        throw new ISE("Unexpected number of arguments");
      }
    }
  }

}