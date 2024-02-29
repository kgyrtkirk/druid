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

package org.apache.druid.sql.calcite;

import com.google.common.collect.ImmutableMap;
import junitparams.Parameters;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.DecoupledTestConfig.NativeQueryIgnore;
import org.apache.druid.sql.calcite.NotYetSupported.NotYetSupportedProcessor;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.PlannerComponentSupplier;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class DecoupledPlanningCalciteJoinQueryTest extends CalciteJoinQueryTest
{

  @Rule(order = 0)
  public NotYetSupportedProcessor decoupledIgnoreProcessor = new NotYetSupportedProcessor();

  private static final ImmutableMap<String, Object> CONTEXT_OVERRIDES =
      ImmutableMap.<String, Object>builder()
      .putAll(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
      .put(PlannerConfig.CTX_NATIVE_QUERY_SQL_PLANNING_MODE, PlannerConfig.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED)
      .put(QueryContexts.ENABLE_DEBUG, true)
      .build();

  @Override
  protected QueryTestBuilder testBuilder()
  {
    PlannerComponentSupplier componentSupplier = this;
    CalciteTestConfig testConfig = new CalciteTestConfig(CONTEXT_OVERRIDES)
    {
      @Override
      public SqlTestFramework.PlannerFixture plannerFixture(PlannerConfig plannerConfig, AuthConfig authConfig)
      {
        plannerConfig = plannerConfig.withOverrides(CONTEXT_OVERRIDES);
        return queryFramework().plannerFixture(componentSupplier, plannerConfig, authConfig);
      }
    };

    QueryTestBuilder builder = new QueryTestBuilder(testConfig)
        .cannotVectorize(cannotVectorize)
        .skipVectorize(skipVectorize);

    DecoupledTestConfig decTestConfig = queryFrameworkRule.getAnnotation(DecoupledTestConfig.class);

    if (decTestConfig != null && decTestConfig.nativeQueryIgnore().isPresent()) {
      builder.verifyNativeQueries(x -> false);
    }

    return builder;
  }

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  @DecoupledTestConfig(nativeQueryIgnore = NativeQueryIgnore.JOIN_LEFT_DIRECT_ACCESS)
  public void ensureDecoupledTestConfigAnnotationWorks(Map<String, Object> queryContext)
  {
    assertNotNull(queryFrameworkRule.getAnnotation(DecoupledTestConfig.class));
  }
}