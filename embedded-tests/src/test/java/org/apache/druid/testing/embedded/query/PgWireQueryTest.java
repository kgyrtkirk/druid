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

package org.apache.druid.testing.embedded.query;

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.guice.SleepModule;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.pgwire.druid.PgWireEndpointInitializer;
import org.apache.druid.pgwire.druid.PgWireModule;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.auth.EmbeddedBasicAuthResource;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Druid SQL over the PostgreSQL wire protocol, driven by the stock pgjdbc driver.
 */
public class PgWireQueryTest extends EmbeddedClusterTestBase
{
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  private final String table = EmbeddedClusterApis.createTestDatasourceName();
  private int port;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    broker.addProperty("druid.pgwire.port", "0");
    // The auth resource must precede the servers so its extension is registered before they start.
    return EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper()
                               .addResource(new EmbeddedBasicAuthResource())
                               .addExtension(PgWireModule.class)
                               .addExtension(SleepModule.class)
                               .useLatchableEmitter()
                               .addServer(coordinator)
                               .addServer(overlord)
                               .addServer(broker)
                               .addServer(new EmbeddedIndexer())
                               .addServer(new EmbeddedHistorical());
  }

  @BeforeAll
  public void ingest()
  {
    port = broker.bindings().getInstance(PgWireEndpointInitializer.class).getPort();
    final String taskId = IdUtils.getRandomId();
    final IndexTask task = MoreResources.Task.BASIC_INDEX.get().dataSource(table).withId(taskId);
    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
    cluster.callApi().waitForTaskToSucceed(taskId, overlord);
    cluster.callApi().waitForAllSegmentsToBeQueryable(table, coordinator, 100_000L);
  }

  @ParameterizedTest
  @ValueSource(strings = {"preferQueryMode=simple", "preferQueryMode=extended", "prepareThreshold=-1"})
  public void testSelect(final String mode) throws SQLException
  {
    try (Connection connection = connect(mode);
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(StringUtils.format(
             "SELECT __time, item, CAST(\"value\" AS BIGINT) AS v FROM \"%s\" ORDER BY __time LIMIT 2",
             table
         ))) {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      Assertions.assertEquals(Types.TIMESTAMP, metaData.getColumnType(1));
      Assertions.assertEquals(Types.VARCHAR, metaData.getColumnType(2));
      Assertions.assertEquals(Types.BIGINT, metaData.getColumnType(3));

      Assertions.assertTrue(resultSet.next());
      Assertions.assertEquals(LocalDateTime.of(2025, 6, 1, 0, 0), resultSet.getObject(1, LocalDateTime.class));
      Assertions.assertEquals("shirt", resultSet.getString(2));
      Assertions.assertEquals(105L, resultSet.getLong(3));
      Assertions.assertTrue(resultSet.next());
      Assertions.assertEquals("trousers", resultSet.getString(2));
      Assertions.assertFalse(resultSet.next());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"preferQueryMode=simple", "preferQueryMode=extended"})
  public void testSessionSetAppliesToLaterStatements(final String mode) throws SQLException
  {
    try (Connection connection = connect(mode); Statement statement = connection.createStatement()) {
      Assertions.assertFalse(statement.execute("SET sqlTimeZone = 'America/Los_Angeles'"));
      Assertions.assertEquals(LocalDateTime.of(2025, 5, 31, 17, 0), firstTime(statement));
    }
  }

  @Test
  public void testStartupOptionsSetContext() throws SQLException
  {
    try (Connection connection = connect("options=-c%20sqlTimeZone=Asia/Tokyo");
         Statement statement = connection.createStatement()) {
      Assertions.assertEquals(LocalDateTime.of(2025, 6, 1, 9, 0), firstTime(statement));
    }
  }

  @Test
  public void testTransactionsAreRejected() throws SQLException
  {
    try (Connection connection = connect(""); Statement statement = connection.createStatement()) {
      assertSqlState("0A000", () -> statement.execute("BEGIN"));
    }
  }

  @Test
  public void testDruidErrorsMapToSqlState() throws SQLException
  {
    try (Connection connection = connect(""); Statement statement = connection.createStatement()) {
      assertSqlState("42000", () -> statement.executeQuery("SELEC 1"));
      assertSqlState("42000", () -> statement.executeQuery("SELECT * FROM no_such_table"));
      assertSqlState("0A000", () -> statement.executeQuery("SELECT ARRAY['a', 'b']"));
      try (ResultSet resultSet = statement.executeQuery("SELECT COUNT(*) FROM \"" + table + "\"")) {
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals(10L, resultSet.getLong(1));
      }
    }
  }

  @Test
  public void testWrongPasswordIsRejected()
  {
    assertSqlState(
        "28P01",
        () -> DriverManager.getConnection(url(""), EmbeddedBasicAuthResource.ADMIN_USER, "wrong").close()
    );
  }

  @Test
  public void testCancel() throws Exception
  {
    try (Connection connection = connect(""); Statement statement = connection.createStatement()) {
      final CompletableFuture<SQLException> failure = CompletableFuture.supplyAsync(
          () -> Assertions.assertThrows(
              SQLException.class,
              () -> statement.executeQuery(StringUtils.format("SELECT sleep(2) FROM \"%s\"", table))
          )
      );
      Thread.sleep(1000L);
      statement.cancel();
      Assertions.assertEquals("57014", failure.get(30, TimeUnit.SECONDS).getSQLState());
    }
  }

  private LocalDateTime firstTime(final Statement statement) throws SQLException
  {
    try (ResultSet resultSet = statement.executeQuery(
        StringUtils.format("SELECT MIN(__time) FROM \"%s\"", table)
    )) {
      Assertions.assertTrue(resultSet.next());
      return resultSet.getObject(1, LocalDateTime.class);
    }
  }

  private Connection connect(final String params) throws SQLException
  {
    return DriverManager.getConnection(
        url(params),
        EmbeddedBasicAuthResource.ADMIN_USER,
        EmbeddedBasicAuthResource.ADMIN_PASSWORD
    );
  }

  private String url(final String params)
  {
    return StringUtils.format("jdbc:postgresql://localhost:%d/druid?sslmode=disable&%s", port, params);
  }

  private static void assertSqlState(final String sqlState, final Executable executable)
  {
    final SQLException failure = Assertions.assertThrows(SQLException.class, executable);
    Assertions.assertEquals(sqlState, failure.getSQLState(), failure.getMessage());
  }
}
