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

import org.apache.calcite.avatica.server.AbstractAvaticaHandler;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.DruidNode;
import org.apache.druid.sql.avatica.AvaticaMonitor;
import org.apache.druid.sql.avatica.DruidAvaticaJsonHandler;
import org.apache.druid.sql.avatica.DruidMeta;
import com.google.inject.Injector;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.http.client.utils.URIBuilder;
import org.eclipse.jetty.server.Server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class AvaticaTestConnection
{

  private SqlTestFramework framework;
  private ServerWrapper server;

  public AvaticaTestConnection(SqlTestFramework framework) throws Exception
  {
    this.framework = framework;
    Injector injector = framework.injector();

    DruidMeta druidMeta = injector.getInstance(DruidMeta.class);
    server = new ServerWrapper(druidMeta);

  }

  // Default implementation is for JSON to allow debugging of tests.
// FIXME: should come via Inject module
  protected AbstractAvaticaHandler getAvaticaHandler(final DruidMeta druidMeta)
  {
    return new DruidAvaticaJsonHandler(
        druidMeta,
        new DruidNode("dummy", "dummy", false, 1, null, true, false),
        new AvaticaMonitor()
    );
  }

  //FIXME: should be created via Inject module
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
          "jdbc:avatica:remote:url=%s",
          new URIBuilder(server.getURI()).setPath(DruidAvaticaJsonHandler.AVATICA_PATH).build()
      );
    }

    public void close() throws Exception
    {
      druidMeta.closeAllConnections();
      server.stop();
    }
  }



  public Connection getConnection(Properties info) throws SQLException
  {
    return DriverManager.getConnection(server.url, info);
  }

}
