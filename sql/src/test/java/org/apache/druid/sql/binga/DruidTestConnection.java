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

package org.apache.druid.sql.binga;

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.SqlTestFramework;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class DruidTestConnection implements Connection
{
  private BaseCalciteQueryTest testHost;
  private SqlTestFramework frameWork;

  SqlTestFrameworkConfig.Rule frameworkRule;

  public DruidTestConnection()
  {
    CalciteTestBase.setupCalciteProperties();
    frameworkRule = new SqlTestFrameworkConfig.Rule();
    testHost = new BaseCalciteQueryTest()
    {
      public SqlTestFramework queryFramework()
      {
        return frameWork;
      }
    };
    testHost.setCaseTempDir(FileUtils.createTempDir("connectionFactory").toPath());
    frameworkRule.testHost = testHost;
    frameWork = frameworkRule.get(frameworkRule.defaultConfig());
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException
  {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException
  {
    return false;
  }

  @Override
  public Statement createStatement() throws SQLException
  {
    return new MyStatement(testHost, frameWork);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;
  }

  @Override
  public String nativeSQL(String sql) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public boolean getAutoCommit() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public void commit() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void rollback() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void close() throws SQLException
  {

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
  public DatabaseMetaData getMetaData() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public boolean isReadOnly() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public void setCatalog(String catalog) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public String getCatalog() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public int getTransactionIsolation() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

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
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void setHoldability(int holdability) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public int getHoldability() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public Savepoint setSavepoint() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Clob createClob() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Blob createBlob() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public NClob createNClob() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public SQLXML createSQLXML() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public boolean isValid(int timeout) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public String getClientInfo(String name) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Properties getClientInfo() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public void setSchema(String schema) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public String getSchema() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public void abort(Executor executor) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public int getNetworkTimeout() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

}
