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

package org.apache.druid.sql.avatica;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import org.apache.calcite.avatica.server.AbstractAvaticaHandler;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.initialization.CoreInjectorBuilder;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.QuerySchedulerProvider;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.schema.DruidSchemaName;
import org.apache.druid.sql.calcite.schema.NamedSchema;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.guice.SqlModule;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Provides an JDBC connections to Druid test data.
 *
 * Extracted from DruidAvaticaHandlerTest; can be reused for some cases.
 */
public class DruidAvaticaConnectionRule
    implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback
{
  private static final int CONNECTION_LIMIT = 4;
  private static final int STATEMENT_LIMIT = 4;

  private final AvaticaServerConfig avaticaConfig;

  public DruidAvaticaConnectionRule()
  {
    avaticaConfig = new AvaticaServerConfig();
    avaticaConfig.maxConnections = CONNECTION_LIMIT;
    avaticaConfig.maxStatementsPerConnection = STATEMENT_LIMIT;
  }

  private static QueryRunnerFactoryConglomerate conglomerate;
  private static SpecificSegmentsQuerySegmentWalker walker;
  private static Closer resourceCloser;

  @Override
  public void beforeAll(ExtensionContext context) throws Exception
  {
    NullHandling.initializeForTests();
    ExpressionProcessing.initializeForTests();

    resourceCloser = Closer.create();
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(resourceCloser);
    File tempDir = FileUtils.createTempDir("FIXME");
    walker = CalciteTests.createMockWalker(conglomerate, tempDir);
    resourceCloser.register(walker);
  }

  @Override
  public void afterAll(ExtensionContext arg0) throws Exception
  {
    resourceCloser.close();
  }

  private final PlannerConfig plannerConfig = new PlannerConfig();
  private final DruidOperatorTable operatorTable = CalciteTests.createOperatorTable();
  private final ExprMacroTable macroTable = CalciteTests.createExprMacroTable();
  private ServerWrapper server;

  private DruidSchemaCatalog makeRootSchema()
  {
    return CalciteTests.createMockRootSchema(
        conglomerate,
        walker,
        plannerConfig,
        CalciteTests.TEST_AUTHORIZER_MAPPER
    );
  }

  private class ServerWrapper
  {
    final DruidMeta druidMeta;
    final Server server;
    final String url;

    ServerWrapper(final DruidMeta druidMeta) throws Exception
    {
      this.druidMeta = druidMeta;
      server = new Server(0);
      server.setHandler(getAvaticaHandler(druidMeta));
      server.start();
      url = StringUtils.format(
          "jdbc:avatica:remote:url=%s%s",
          server.getURI().toString(),
          StringUtils.maybeRemoveLeadingSlash(getJdbcUrlTail())
      );
    }

    public void close() throws Exception
    {
      druidMeta.closeAllConnections();
      server.stop();
    }
  }

  protected String getJdbcUrlTail()
  {
    return DruidAvaticaJsonHandler.AVATICA_PATH;
  }

  // Default implementation is for JSON to allow debugging of tests.
  protected AbstractAvaticaHandler getAvaticaHandler(final DruidMeta druidMeta)
  {
    return new DruidAvaticaJsonHandler(
        druidMeta,
        new DruidNode("dummy", "dummy", false, 1, null, true, false),
        new AvaticaMonitor()
    );
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception
  {

    DruidSchemaCatalog rootSchema = makeRootSchema();
    TestRequestLogger testRequestLogger = new TestRequestLogger();

    Injector injector = new CoreInjectorBuilder(new StartupInjectorBuilder().build())
        .addModule(
            binder -> {
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
              binder.bind(AuthenticatorMapper.class).toInstance(CalciteTests.TEST_AUTHENTICATOR_MAPPER);
              binder.bind(AuthorizerMapper.class).toInstance(CalciteTests.TEST_AUTHORIZER_MAPPER);
              binder.bind(Escalator.class).toInstance(CalciteTests.TEST_AUTHENTICATOR_ESCALATOR);
              binder.bind(RequestLogger.class).toInstance(testRequestLogger);
              binder.bind(DruidSchemaCatalog.class).toInstance(rootSchema);
              for (NamedSchema schema : rootSchema.getNamedSchemas().values()) {
                Multibinder.newSetBinder(binder, NamedSchema.class).addBinding().toInstance(schema);
              }
              binder.bind(QueryLifecycleFactory.class)
                  .toInstance(CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate));
              binder.bind(DruidOperatorTable.class).toInstance(operatorTable);
              binder.bind(ExprMacroTable.class).toInstance(macroTable);
              binder.bind(PlannerConfig.class).toInstance(plannerConfig);
              binder.bind(String.class)
                  .annotatedWith(DruidSchemaName.class)
                  .toInstance(CalciteTests.DRUID_SCHEMA_NAME);
              binder.bind(AvaticaServerConfig.class).toInstance(avaticaConfig);
              binder.bind(ServiceEmitter.class).to(NoopServiceEmitter.class);
              binder.bind(QuerySchedulerProvider.class).in(LazySingleton.class);
              binder.bind(QueryScheduler.class)
                  .toProvider(QuerySchedulerProvider.class)
                  .in(LazySingleton.class);
              binder.install(new SqlModule.SqlStatementFactoryModule());
              binder.bind(new TypeLiteral<Supplier<DefaultQueryConfig>>()
              {
              }).toInstance(Suppliers.ofInstance(new DefaultQueryConfig(ImmutableMap.of())));
              binder.bind(CalciteRulesManager.class).toInstance(new CalciteRulesManager(ImmutableSet.of()));
              binder.bind(JoinableFactoryWrapper.class).toInstance(CalciteTests.createJoinableFactoryWrapper());
              binder.bind(CatalogResolver.class).toInstance(CatalogResolver.NULL_RESOLVER);
            }
        )
        .build();

    DruidMeta druidMeta = injector.getInstance(DruidMeta.class);
    server = new ServerWrapper(druidMeta);
  }

  @Override
  public void afterEach(ExtensionContext arg0) throws Exception
  {
    if (server != null) {
      server.close();
      server = null;
    }
  }

  public Connection getConnection() throws SQLException
  {
    return getConnection("regularUser", "druid");
  }

  public Connection getConnection(String user, String password) throws SQLException
  {
    final Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);
    return getConnection(props);
  }

  public Connection getConnection(Properties info) throws SQLException
  {
    return DriverManager.getConnection(server.url, info);
  }

  public void ensureInited()
  {
    try {
      if (resourceCloser == null) {
        beforeAll(null);
      }
      if (server == null) {
        beforeEach(null);
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String getUrl()
  {
    return server.url;
  }

  public void closeAllConnections()
  {
    server.druidMeta.closeAllConnections();
  }
}
