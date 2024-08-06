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

import net.hydromatic.quidem.Quidem.ConnectionFactory;
import net.hydromatic.quidem.Quidem.PropertyHandler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;

public class DruidQuidemConnectionFactory implements ConnectionFactory, PropertyHandler
{
  private Properties props = new Properties();
  private Map<String, String> engineProperties;

  public DruidQuidemConnectionFactory()
  {
    // ensure driver loaded
    new DruidAvaticaTestDriver();
  }

  @Override
  public Connection connect(String name, boolean reference) throws Exception
  {
    if (name.startsWith("druidtest://")) {
      Connection connection = DriverManager.getConnection(name, props);
      engineProperties = unwrapEngineProperties(connection);
      return connection;
    }
    throw new RuntimeException("unknown connection '" + name + "'");
  }

  private Map<String, String> unwrapEngineProperties(Connection connection)
  {
    if(connection instanceof DruidConnectionExtras) {
      DruidConnectionExtras extras = ((DruidConnectionExtras) connection);
      return extras.getEngineProperties();
    }
    return null;
  }

  @Override
  public void onSet(String key, Object value)
  {
    props.setProperty(key, value.toString());
  }

  public Object getEnv(String env)
  {
    if (engineProperties == null) {
      return null;
    }
    return engineProperties.get(env);
  }
}
