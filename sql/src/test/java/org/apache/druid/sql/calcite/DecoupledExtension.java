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
import org.apache.druid.query.QueryContexts;
import org.apache.druid.quidem.DruidQTestInfo;
import org.apache.druid.quidem.ProjectPathUtils;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest.CalciteTestConfig;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.PlannerComponentSupplier;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;

import static org.junit.Assume.assumeTrue;

public class DecoupledExtension implements BeforeEachCallback
{
  private BaseCalciteQueryTest baseTest;

  public DecoupledExtension(BaseCalciteQueryTest baseTest)
  {
    this.baseTest = baseTest;
  }

  private File qCaseDir;

  @Override
  public void beforeEach(ExtensionContext context) throws Exception
  {
    Class<?> testClass = context.getTestClass().get();
    qCaseDir = ProjectPathUtils.getPathFromProjectRoot("sql/src/test/quidem/" + testClass.getName());
  }

  private static final ImmutableMap<String, Object> CONTEXT_OVERRIDES =
      ImmutableMap.<String, Object>builder()
      .putAll(BaseCalciteQueryTest.QUERY_CONTEXT_DEFAULT)
      .put(PlannerConfig.CTX_NATIVE_QUERY_SQL_PLANNING_MODE, PlannerConfig.NATIVE_QUERY_SQL_PLANNING_MODE_DECOUPLED)
      .put(QueryContexts.ENABLE_DEBUG, true)
      .build();

  public QueryTestBuilder testBuilder()
  {
    DecoupledTestConfig decTestConfig = BaseCalciteQueryTest.queryFrameworkRule.getAnnotation(DecoupledTestConfig.class);

    assumeTrue(BaseCalciteQueryTest.queryFrameworkRule.getConfig().numMergeBuffers() == 0);

    PlannerComponentSupplier componentSupplier = baseTest;

    boolean runQuidem = (decTestConfig != null && decTestConfig.quidem());

    CalciteTestConfig testConfig = baseTest.new CalciteTestConfig(CONTEXT_OVERRIDES)
    {

      @Override
      public SqlTestFramework.PlannerFixture plannerFixture(PlannerConfig plannerConfig, AuthConfig authConfig)
      {
        plannerConfig = plannerConfig.withOverrides(CONTEXT_OVERRIDES);

        return baseTest.queryFramework().plannerFixture(componentSupplier, plannerConfig, authConfig);
      }

      @Override
      public DruidQTestInfo getQTestInfo()
      {
        if(runQuidem) {
          return new DruidQTestInfo(qCaseDir, BaseCalciteQueryTest.queryFrameworkRule.testName());
        } else {
          return null;
        }
      }
    };

    QueryTestBuilder builder = new QueryTestBuilder(testConfig)
        .cannotVectorize(baseTest.cannotVectorize)
        .skipVectorize(baseTest.skipVectorize);

    if (decTestConfig != null && decTestConfig.nativeQueryIgnore().isPresent()) {
      builder.verifyNativeQueries(x -> false);
    }

    return builder;
  }
}