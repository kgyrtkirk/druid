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

package org.apache.druid.jdbc;

import com.google.common.base.Preconditions;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.avatica.DruidJdbcResultSet.ResultFetcherFactory;
import org.apache.druid.sql.avatica.DruidJdbcStatement;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SqlTestFramework;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collections;

public class MyStatement implements Statement
{

  private static final int DEFAULT_FETCH_TIMEOUT_MS = 5000;
  private SqlTestFramework frameWork;
  private BaseCalciteQueryTest testHost;
  private DruidJdbcStatement djStmt;

  public MyStatement(BaseCalciteQueryTest testHost, SqlTestFramework frameWork)
  {
    this.testHost = testHost;
    this.frameWork = frameWork;

    djStmt = new DruidJdbcStatement(
        "",
        0,
        Collections.emptyMap(),
        testHost.getSqlStatementFactory(testHost.PLANNER_CONFIG_DEFAULT),
        new ResultFetcherFactory(DEFAULT_FETCH_TIMEOUT_MS)
    );

  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException
  {
    // res=testHost.testBuilder().sql(sql).run();
    final SqlQueryPlus sqlQuery = SqlQueryPlus.builder(sql)
        .sqlParameters(testHost.DEFAULT_PARAMETERS)
        .auth(CalciteTests.REGULAR_USER_AUTH_RESULT)
        .build();

    djStmt.execute(sqlQuery, -1);

    Signature signature = djStmt.getSignature();
    Frame nextFrame = djStmt.nextFrame(0, 10000);
    Preconditions.checkState(nextFrame.done);
    return new MyResultset(signature, nextFrame);
  }

  @Override
  public int executeUpdate(String sql) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public void close() throws SQLException
  {
    djStmt.closeResultSet();
  }

  @Override
  public int getMaxFieldSize() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public int getMaxRows() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public void setMaxRows(int max) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public int getQueryTimeout() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void cancel() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public SQLWarning getWarnings() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public void clearWarnings() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void setCursorName(String name) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public boolean execute(String sql) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public ResultSet getResultSet() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public int getUpdateCount() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public boolean getMoreResults() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public void setFetchDirection(int direction) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public int getFetchDirection() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public void setFetchSize(int rows) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public int getFetchSize() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public int getResultSetConcurrency() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public int getResultSetType() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public void addBatch(String sql) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void clearBatch() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public int[] executeBatch() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Connection getConnection() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public boolean getMoreResults(int current) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public int getResultSetHoldability() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public boolean isClosed() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public boolean isPoolable() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public void closeOnCompletion() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

}
