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

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.ServerInjectorBuilderTest.TestDruidModule;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.msq.exec.MSQDrillWindowQueryTest.DrillWindowQueryMSQComponentSupplier;
import org.apache.druid.msq.guice.MultiStageQuery;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.msq.test.CalciteMSQTestsHelper;
import org.apache.druid.msq.test.ExtractResultsFactory;
import org.apache.druid.msq.test.MSQTestOverlordServiceClient;
import org.apache.druid.msq.test.MSQTestTaskActionClient;
import org.apache.druid.msq.test.VerifyMSQSupportedNativeQueriesPredicate;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QuerySchedulerProvider;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.SqlLifecycleManager;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.SqlToolbox;
import org.apache.druid.sql.calcite.DrillWindowQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaName;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.guice.SqlModule;

@SqlTestFrameworkConfig.ComponentSupplier(DrillWindowQueryMSQComponentSupplier.class)
public class MSQDrillWindowQueryTest extends DrillWindowQueryTest
{
  public static class DrillWindowQueryMSQComponentSupplier extends DrillComponentSupplier
  {
    public DrillWindowQueryMSQComponentSupplier(TempDirProducer tempFolderProducer)
    {
      super(tempFolderProducer);
    }

    @Override
    public void configureGuice(DruidInjectorBuilder builder)
    {
      super.configureGuice(builder);
      builder.addModules(CalciteMSQTestsHelper.fetchModules(tempDirProducer::newTempFolder, TestGroupByBuffers.createDefault()).toArray(new Module[0]));
      builder.addModule(new TestSqlModule());
      builder.addModule(new TestMSQSqlModule());
    }

    static class TestSqlModule extends TestDruidModule
    {
      @Override
      public void configure(Binder binder)
      {
        binder.install(new SqlModule.SqlStatementFactoryModule());
        binder.bind(String.class)
            .annotatedWith(DruidSchemaName.class)
            .toInstance(CalciteTests.DRUID_SCHEMA_NAME);
        binder.bind(new TypeLiteral<Supplier<DefaultQueryConfig>>()
        {
        }).toInstance(Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of())));
        binder.bind(CalciteRulesManager.class).toInstance(new CalciteRulesManager(ImmutableSet.of()));
        TestRequestLogger testRequestLogger = new TestRequestLogger();
        binder.bind(RequestLogger.class).toInstance(testRequestLogger);
        binder.bind(CatalogResolver.class).toInstance(CatalogResolver.NULL_RESOLVER);
        binder.bind(ServiceEmitter.class).to(NoopServiceEmitter.class);
        binder.bind(QueryScheduler.class)
            .toProvider(QuerySchedulerProvider.class)
            .in(LazySingleton.class);
        binder.bind(AuthenticatorMapper.class).toInstance(CalciteTests.TEST_AUTHENTICATOR_MAPPER);
        binder.bind(AuthorizerMapper.class).toInstance(CalciteTests.TEST_AUTHORIZER_MAPPER);
        binder.bind(Escalator.class).toInstance(CalciteTests.TEST_AUTHENTICATOR_ESCALATOR);
      }

    }
    static class TestMSQSqlModule extends TestDruidModule {

//      @Provides
      @LazySingleton
      public SqlToolbox makeSqlToolbox(
          final PlannerFactory plannerFactory,
          final ServiceEmitter emitter,
          final RequestLogger requestLogger,
          final QueryScheduler queryScheduler,
          final Supplier<DefaultQueryConfig> defaultQueryConfig,
          final SqlLifecycleManager sqlLifecycleManager
      )
      {
        return new SqlToolbox(
            null,
            plannerFactory,
            emitter,
            requestLogger,
            queryScheduler,
            defaultQueryConfig.get(),
            sqlLifecycleManager
        );
      }

      @Provides
      @MultiStageQuery
      @LazySingleton
      public SqlStatementFactory makeNativeSqlStatementFactory(
          final MSQTaskSqlEngine sqlEngine,
          SqlToolbox toolbox
      )
      {
        return new SqlStatementFactory(toolbox.withEngine(sqlEngine));
      }

      @Provides
      @LazySingleton
      public MSQTaskSqlEngine createEngine(
          QueryLifecycleFactory qlf,
          ObjectMapper queryJsonMapper,
          Injector injector
      )
      {
        final WorkerMemoryParameters workerMemoryParameters =
            WorkerMemoryParameters.createInstance(
                WorkerMemoryParameters.PROCESSING_MINIMUM_BYTES * 50,
                2,
                10,
                2,
                0,
                0
            );
        final MSQTestOverlordServiceClient indexingServiceClient = new MSQTestOverlordServiceClient(
            queryJsonMapper,
            injector,
            new MSQTestTaskActionClient(queryJsonMapper, injector),
            workerMemoryParameters,
            ImmutableList.of()
        );
        return new MSQTaskSqlEngine(indexingServiceClient, queryJsonMapper);
      }
    }

    @Override
    public SqlEngine createEngine(
        QueryLifecycleFactory qlf,
        ObjectMapper queryJsonMapper,
        Injector injector
    )
    {
      if(true) {
        return injector.getInstance(MSQTaskSqlEngine.class);
      } else {
        final WorkerMemoryParameters workerMemoryParameters =
            WorkerMemoryParameters.createInstance(
                WorkerMemoryParameters.PROCESSING_MINIMUM_BYTES * 50,
                2,
                10,
                2,
                0,
                0
            );
        final MSQTestOverlordServiceClient indexingServiceClient = new MSQTestOverlordServiceClient(
            queryJsonMapper,
            injector,
            new MSQTestTaskActionClient(queryJsonMapper, injector),
            workerMemoryParameters,
            ImmutableList.of()
        );
        return new MSQTaskSqlEngine(indexingServiceClient, queryJsonMapper);
      }
    }
  }

  @Override
  protected QueryTestBuilder testBuilder()
  {
    return new QueryTestBuilder(new CalciteTestConfig(true))
        .addCustomRunner(new ExtractResultsFactory(() -> (MSQTestOverlordServiceClient) ((MSQTaskSqlEngine) queryFramework().engine()).overlordClient()))
        .skipVectorize(true)
        .verifyNativeQueries(new VerifyMSQSupportedNativeQueriesPredicate());
  }
}
