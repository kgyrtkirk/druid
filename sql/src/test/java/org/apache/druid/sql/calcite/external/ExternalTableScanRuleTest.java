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

package org.apache.druid.sql.calcite.external;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.parser.DruidSqlParser;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerToolbox;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.NamedDruidSchema;
import org.apache.druid.sql.calcite.schema.NamedViewSchema;
import org.apache.druid.sql.calcite.schema.ViewSchema;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.hook.DruidHookDispatcher;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class ExternalTableScanRuleTest
{
  @Test
  public void testMatchesWhenExternalScanUnsupported()
  {
    final NativeSqlEngine engine = CalciteTests.createMockSqlEngine(
        EasyMock.createMock(QuerySegmentWalker.class),
        EasyMock.createMock(QueryRunnerFactoryConglomerate.class)
    );
    final PlannerToolbox toolbox = new PlannerToolbox(
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        CalciteTests.getJsonMapper(),
        new PlannerConfig(),
        new DruidSchemaCatalog(
            EasyMock.createMock(SchemaPlus.class),
            ImmutableMap.of(
                "druid", new NamedDruidSchema(EasyMock.createMock(DruidSchema.class), "druid"),
                NamedViewSchema.NAME, new NamedViewSchema(EasyMock.createMock(ViewSchema.class))
            )
        ),
        CalciteTests.createJoinableFactoryWrapper(),
        CatalogResolver.NULL_RESOLVER,
        "druid",
        new CalciteRulesManager(ImmutableSet.of()),
        CalciteTests.TEST_AUTHORIZER_MAPPER,
        AuthConfig.newBuilder().build(),
        NoopPolicyEnforcer.instance(),
        new DruidHookDispatcher()
    );
    final PlannerContext plannerContext = PlannerContext.create(
        toolbox,
        "SELECT 1", // The actual query isn't important for this test
        DruidSqlParser.parse("SELECT 1", false).getMainStatement(),
        engine,
        Collections.emptyMap(),
        null
    );
    plannerContext.setQueryMaker(
        engine.buildQueryMakerForSelect(EasyMock.createMock(RelRoot.class), plannerContext)
    );

    ExternalTableScanRule rule = new ExternalTableScanRule(plannerContext);
    rule.matches(EasyMock.createMock(RelOptRuleCall.class));
    Assert.assertEquals(
        "Cannot use [EXTERN] with SQL engine [native].",
        plannerContext.getPlanningError()
    );
  }
}
