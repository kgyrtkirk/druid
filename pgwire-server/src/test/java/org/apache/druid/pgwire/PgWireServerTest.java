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

package org.apache.druid.pgwire;

import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class PgWireServerTest
{
  private static final int MAX_CONNECTIONS = 3;
  private static final String USER = "alice";
  private static final String PASSWORD = "secret";

  private static final List<PgColumn> COLUMNS = List.of(
      new PgColumn("s", PgType.VARCHAR),
      new PgColumn("i", PgType.INT4),
      new PgColumn("l", PgType.INT8),
      new PgColumn("f", PgType.FLOAT4),
      new PgColumn("d", PgType.FLOAT8),
      new PgColumn("b", PgType.BOOL),
      new PgColumn("day", PgType.DATE),
      new PgColumn("ts", PgType.TIMESTAMP)
  );
  private static final LocalDate DAY = LocalDate.of(2026, 10, 19);
  private static final LocalDateTime TS = LocalDateTime.of(1999, 12, 31, 23, 59, 58, 123456000);
  private static final List<Object[]> ROWS = List.of(
      new Object[]{"héllo", 42, 1L << 40, 1.5f, -2.25d, true, DAY, TS},
      new Object[]{null, null, null, null, null, false, null, null}
  );

  private final FakeBackend backend = new FakeBackend();
  private PgWireServer server;

  @BeforeEach
  public void setUp()
  {
    server = new PgWireServer(backend, 0, MAX_CONNECTIONS);
    server.start();
  }

  @AfterEach
  public void tearDown()
  {
    server.stop();
  }

  @ParameterizedTest
  @ValueSource(strings = {"preferQueryMode=simple", "preferQueryMode=extended", "prepareThreshold=-1"})
  public void testRowsInAllQueryModes(final String mode) throws SQLException
  {
    try (Connection connection = connect(mode);
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery("rows")) {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      Assertions.assertEquals(COLUMNS.size(), metaData.getColumnCount());
      Assertions.assertEquals("s", metaData.getColumnName(1));
      Assertions.assertEquals(Types.VARCHAR, metaData.getColumnType(1));
      Assertions.assertEquals(Types.BIGINT, metaData.getColumnType(3));
      Assertions.assertEquals(Types.TIMESTAMP, metaData.getColumnType(8));

      Assertions.assertTrue(resultSet.next());
      Assertions.assertEquals("héllo", resultSet.getString("s"));
      Assertions.assertEquals(42, resultSet.getInt("i"));
      Assertions.assertEquals(1L << 40, resultSet.getLong("l"));
      Assertions.assertEquals(1.5f, resultSet.getFloat("f"));
      Assertions.assertEquals(-2.25d, resultSet.getDouble("d"));
      Assertions.assertTrue(resultSet.getBoolean("b"));
      Assertions.assertEquals(DAY, resultSet.getObject("day", LocalDate.class));
      Assertions.assertEquals(TS, resultSet.getObject("ts", LocalDateTime.class));

      Assertions.assertTrue(resultSet.next());
      Assertions.assertNull(resultSet.getString("s"));
      Assertions.assertNull(resultSet.getObject("l"));
      Assertions.assertFalse(resultSet.getBoolean("b"));
      Assertions.assertNull(resultSet.getObject("ts", LocalDateTime.class));

      Assertions.assertFalse(resultSet.next());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"preferQueryMode=simple", "preferQueryMode=extended"})
  public void testBackendErrorKeepsConnectionUsable(final String mode) throws SQLException
  {
    try (Connection connection = connect(mode); Statement statement = connection.createStatement()) {
      final SQLException failure = Assertions.assertThrows(SQLException.class, () -> statement.executeQuery("fail"));
      Assertions.assertEquals("42000", failure.getSQLState());
      Assertions.assertTrue(failure.getMessage().contains("Bad query [fail]"), failure.getMessage());

      final SQLException unexpected = Assertions.assertThrows(SQLException.class, () -> statement.executeQuery("boom"));
      Assertions.assertEquals(PgException.INTERNAL_ERROR, unexpected.getSQLState());

      try (ResultSet resultSet = statement.executeQuery("rows")) {
        Assertions.assertTrue(resultSet.next());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"preferQueryMode=simple", "preferQueryMode=extended"})
  public void testCommandWithoutRows(final String mode) throws SQLException
  {
    try (Connection connection = connect(mode); Statement statement = connection.createStatement()) {
      Assertions.assertFalse(statement.execute("SET x = 1"));
      Assertions.assertEquals(
          List.of("SET application_name = 'PostgreSQL JDBC Driver'", "SET x = 1"),
          backend.executed
      );
    }
  }

  @Test
  public void testWrongPasswordIsRejected()
  {
    final SQLException failure = Assertions.assertThrows(
        SQLException.class,
        () -> DriverManager.getConnection(url(""), USER, "wrong")
    );
    Assertions.assertEquals(PgException.INVALID_PASSWORD, failure.getSQLState());
  }

  @Test
  public void testStartupParamsReachBackend() throws SQLException
  {
    try (Connection ignored = connect("options=-c%20sqlTimeZone=UTC")) {
      Assertions.assertEquals("-c sqlTimeZone=UTC", backend.startupParams.get("options"));
      Assertions.assertEquals(USER, backend.startupParams.get("user"));
    }
  }

  @Test
  public void testCancel() throws Exception
  {
    try (Connection connection = connect(""); Statement statement = connection.createStatement()) {
      final CompletableFuture<SQLException> failure = CompletableFuture.supplyAsync(
          () -> Assertions.assertThrows(SQLException.class, () -> statement.executeQuery("slow"))
      );
      Assertions.assertTrue(backend.slowStarted.await(10, TimeUnit.SECONDS));
      statement.cancel();
      Assertions.assertEquals("57014", failure.get(10, TimeUnit.SECONDS).getSQLState());
    }
  }

  @Test
  public void testExecuteRowLimitIsNotSupported() throws SQLException
  {
    try (Connection connection = connect("preferQueryMode=extended");
         Statement statement = connection.createStatement()) {
      statement.setMaxRows(1);
      final SQLException failure = Assertions.assertThrows(SQLException.class, () -> statement.executeQuery("rows"));
      Assertions.assertEquals(PgException.FEATURE_NOT_SUPPORTED, failure.getSQLState());
    }
  }

  @Test
  public void testParametersAreNotSupported() throws SQLException
  {
    try (Connection connection = connect("");
         PreparedStatement statement = connection.prepareStatement("rows ?")) {
      statement.setInt(1, 1);
      final SQLException failure = Assertions.assertThrows(SQLException.class, statement::executeQuery);
      Assertions.assertEquals(PgException.FEATURE_NOT_SUPPORTED, failure.getSQLState());
    }
  }

  @Test
  public void testConnectionLimit() throws SQLException
  {
    final Connection[] connections = new Connection[MAX_CONNECTIONS];
    try {
      for (int i = 0; i < MAX_CONNECTIONS; i++) {
        connections[i] = connect("");
      }
      final SQLException failure = Assertions.assertThrows(SQLException.class, () -> connect(""));
      Assertions.assertEquals(PgException.TOO_MANY_CONNECTIONS, failure.getSQLState());
    }
    finally {
      for (Connection connection : connections) {
        if (connection != null) {
          connection.close();
        }
      }
    }
  }

  private Connection connect(final String params) throws SQLException
  {
    return DriverManager.getConnection(url(params), USER, PASSWORD);
  }

  private String url(final String params)
  {
    return "jdbc:postgresql://localhost:" + server.getPort() + "/druid?sslmode=disable&" + params;
  }

  private static class FakeBackend implements PgBackend
  {
    final List<String> executed = new CopyOnWriteArrayList<>();
    final CountDownLatch slowStarted = new CountDownLatch(1);
    volatile Map<String, String> startupParams;

    @Override
    public PgSession authenticate(final String user, final String password, final Map<String, String> startupParams)
    {
      if (!USER.equals(user) || !PASSWORD.equals(password)) {
        throw new PgException(PgException.INVALID_PASSWORD, "Authentication failed for user [%s]", user);
      }
      this.startupParams = startupParams;
      return new FakeSession();
    }

    private class FakeSession implements PgSession
    {
      private final CountDownLatch cancelled = new CountDownLatch(1);

      @Override
      public PgResult execute(final String sql)
      {
        return prepare(sql).execute();
      }

      @Override
      public PgStatement prepare(final String sql)
      {
        if (sql.startsWith("SET ")) {
          return statement(List.of(), () -> {
            executed.add(sql);
            return PgResult.command("SET");
          });
        }
        return switch (sql) {
          case "rows" -> statement(COLUMNS, () -> PgResult.rows(COLUMNS, CloseableIterators.withEmptyBaggage(ROWS.iterator())));
          case "slow" -> statement(COLUMNS, () -> PgResult.rows(COLUMNS, new BlockingIterator()));
          case "boom" -> throw new IllegalStateException("boom");
          default -> throw new PgException("42000", "Bad query [%s]", sql);
        };
      }

      @Override
      public void cancel()
      {
        cancelled.countDown();
      }

      @Override
      public void close()
      {
      }

      private class BlockingIterator implements CloseableIterator<Object[]>
      {
        @Override
        public boolean hasNext()
        {
          slowStarted.countDown();
          try {
            cancelled.await();
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          throw new PgException("57014", "Query cancelled");
        }

        @Override
        public Object[] next()
        {
          throw new NoSuchElementException();
        }

        @Override
        public void close()
        {
        }
      }
    }

    private static PgStatement statement(final List<PgColumn> columns, final Supplier<PgResult> run)
    {
      return new PgStatement()
      {
        @Override
        public List<PgColumn> getColumns()
        {
          return columns;
        }

        @Override
        public PgResult execute()
        {
          return run.get();
        }
      };
    }
  }
}
