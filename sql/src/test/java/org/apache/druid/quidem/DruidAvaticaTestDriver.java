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
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    SqlTestFrameworkConfig config = buildConfigfromURIParams(url);

    return rule.getConnection(info);
  }

  private SqlTestFrameworkConfig buildConfigfromURIParams(String url) throws SQLException
  {
    Map<String,String> queryParams ;
        queryParams =new HashMap<>();
    try {
      List<NameValuePair> params = URLEncodedUtils.parse(new URI(url), StandardCharsets.UTF_8);
      for (NameValuePair pair : params) {
        queryParams.put(pair.getName(), pair.getValue());
      }
      // possible caveat: duplicate entries overwrite earlier ones
    }
    catch (URISyntaxException e) {
      throw new SQLException("Can't decode URI", e);
    }

    return MapToInterfaceHandler.newInstanceFor(SqlTestFrameworkConfig.class, queryParams);
  }

  /**
   *
   *
   */
  static class MapToInterfaceHandler implements InvocationHandler
  {
    private Map<String, String> backingMap;

    @SuppressWarnings("unchecked")
    public static <T> T newInstanceFor(Class<T> clazz, Map<String, String> queryParams)
    {
      return (T) Proxy.newProxyInstance(
          clazz.getClassLoader(),
          new Class[] {clazz},
          new MapToInterfaceHandler(queryParams)
      );
    }

    private MapToInterfaceHandler(Map<String, String> backingMap)
    {
      this.backingMap = backingMap;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable
    {
      Class<?> returnType = method.getReturnType();
      String obj = backingMap.get(method.getName());
      if (obj == null) {
        return method.getDefaultValue();
      } else {
        return returnType.cast(obj);
      }
    }
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
