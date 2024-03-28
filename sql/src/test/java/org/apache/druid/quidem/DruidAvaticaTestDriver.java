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

package org.apache.druid.quidem;

import org.apache.druid.sql.avatica.DruidAvaticaConnectionRule;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class DruidAvaticaTestDriver implements Driver
{
  static {
    new DruidAvaticaTestDriver().register();
  }

  private static final String UNIMPLEMENTED_MESSAGE = "Unimplemented method!";
  private static final String prefix = "druidtest://";
  private DruidAvaticaConnectionRule rule;
  public static final String DEFAULT_URI = prefix;

  public DruidAvaticaTestDriver()
  {
    rule = new DruidAvaticaConnectionRule();
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException
  {
    if (!acceptsURL(url)) {
      return null;
    }
    rule.ensureInited();
    return rule.getConnection(info);
  }

  private void register()
  {
    try {
      DriverManager.registerDriver(this);
    }
    catch (SQLException e) {
      System.out.println("Error occurred while registering JDBC driver " + this + ": " + e.toString());
    }
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException
  {
    return url.startsWith(prefix);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException
  {
    throw new RuntimeException(UNIMPLEMENTED_MESSAGE);
  }

  @Override
  public int getMajorVersion()
  {
    return 0;
  }

  @Override
  public int getMinorVersion()
  {
    return 0;
  }

  @Override
  public boolean jdbcCompliant()
  {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException
  {
    return Logger.getLogger("");
  }
}
