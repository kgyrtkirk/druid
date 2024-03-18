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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class MyJdbcDriver implements Driver
{
  @Override
  public Connection connect(String url, Properties info) throws SQLException
  {
    return new DruidTestConnection();
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException
  {
    if(true)
    {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException
  {
    if(true)
    {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public int getMajorVersion()
  {
    if(true)
    {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public int getMinorVersion()
  {
    if(true)
    {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public boolean jdbcCompliant()
  {
    if(true)
    {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException
  {
    if(true)
    {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

}
