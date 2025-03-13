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

package org.apache.druid.msq.dart.controller.sql;

import com.google.inject.Inject;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.tools.ValidationException;
import org.apache.druid.error.DruidException;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.planner.Stage10X;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.apache.druid.sql.calcite.rel.logical.DruidLogicalNode;
import org.apache.druid.sql.calcite.run.EngineFeature;
import org.apache.druid.sql.calcite.run.QueryMaker;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.destination.IngestDestination;

import java.util.Map;

public class QkSqlEngine implements SqlEngine
{
  final SqlEngine delegate;

  @Inject
  public QkSqlEngine(
      DartSqlEngine engine)
  {
    delegate = engine;
  }

  @Override
  public String name()
  {
    return "qk-" + delegate.name();
  }

  @Override
  public boolean featureAvailable(EngineFeature feature)
  {
    return delegate.featureAvailable(feature);
  }

  @Override
  public void validateContext(Map<String, Object> queryContext)
  {
    delegate.validateContext(queryContext);
  }

  @Override
  public RelDataType resultTypeForSelect(
      RelDataTypeFactory typeFactory,
      RelDataType validatedRowType,
      Map<String, Object> queryContext)
  {
    return delegate.resultTypeForSelect(typeFactory, validatedRowType, queryContext);
  }

  @Override
  public RelDataType resultTypeForInsert(
      RelDataTypeFactory typeFactory,
      RelDataType validatedRowType,
      Map<String, Object> queryContext)
  {
    return delegate.resultTypeForInsert(typeFactory, validatedRowType, queryContext);
  }

  @Override
  public QueryMaker buildQueryMakerForSelect(RelRoot relRoot, PlannerContext plannerContext) throws ValidationException
  {
    return new QkQueryMaker(
        plannerContext,
        delegate.buildQueryMakerForSelect(relRoot, plannerContext)
    );

  }

  @Override
  public QueryMaker buildQueryMakerForInsert(
      IngestDestination destination,
      RelRoot relRoot,
      PlannerContext plannerContext)
  {
    throw DruidException.defensive("Not yet supported in this mode");
  }

  static class QkQueryMaker implements QueryMaker ,Stage10X
  {

    private PlannerContext plannerContext;
    private DartQueryMaker dartQueryMaker;

    public QkQueryMaker(PlannerContext plannerContext, QueryMaker buildQueryMakerForSelect)
    {
      this.plannerContext = plannerContext;
      this.dartQueryMaker = (DartQueryMaker) buildQueryMakerForSelect;

    }

    @Override
    public QueryResponse<Object[]> runQuery(DruidQuery druidQuery)
    {
      return dartQueryMaker.runQuery(druidQuery);
    }

    @Override
    // DruidJoin
    //   Druid
    public PlannerResult buildPlannerResult(DruidLogicalNode newRoot)
    {
      QueryDefinition queryDef = null;
      QueryResponse<Object[]> a = dartQueryMaker.runQueryDef(queryDef, null);
      return new PlannerResult(() -> a, null);

    }

    @Override
    public PlannerResult buildPlannerResult2(DruidQuery druidQuery)
    {
      if(true)
      {
        throw new RuntimeException("FIXME: Unimplemented!");
      }
      return null;

    }

  }

  // @Override
  // public PlannerResult buildPlannerResult(RelNode newRoot)
  // {
  // throw DruidException.defensive("Not yet supported in this mode");
  //// Object queryDef = new MSQQueryGenerator().generateQuery(newRoot);
  //// return delegate.buildPlannerResult(newRoot);
  // }
  //
  // @Override
  // public PlannerResult buildPlannerResult2(DruidQuery druidQuery)
  // {
  // delegate.buildQueryMakerForInsert(null, null, null);
  // if (true) {
  // throw new RuntimeException("FIXME: Unimplemented!");
  // }
  // return null;
  //
  // }
}
