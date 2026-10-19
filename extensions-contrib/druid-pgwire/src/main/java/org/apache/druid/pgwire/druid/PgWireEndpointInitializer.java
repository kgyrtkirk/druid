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

package org.apache.druid.pgwire.druid;

import org.apache.druid.guice.ManageLifecycleServer;
import org.apache.druid.guice.annotations.NativeQuery;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.pgwire.PgWireServer;
import org.apache.druid.query.QueryConfigProvider;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.sql.SqlStatementFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;

/**
 * Runs the PostgreSQL wire protocol endpoint on the Broker when {@code druid.pgwire.port} is set.
 */
@ManageLifecycleServer
public class PgWireEndpointInitializer
{
  private static final Logger log = new Logger(PgWireEndpointInitializer.class);

  private final PgWireConfig config;
  private final DruidPgBackend backend;

  @Nullable
  private PgWireServer server;

  @Inject
  public PgWireEndpointInitializer(
      final PgWireConfig config,
      final @NativeQuery SqlStatementFactory sqlStatementFactory,
      final QueryConfigProvider queryConfigProvider,
      final AuthenticatorMapper authMapper
  )
  {
    this.config = config;
    this.backend = new DruidPgBackend(
        authMapper.getAuthenticatorChain(),
        sqlStatementFactory,
        queryConfigProvider.getContext()
    );
  }

  @LifecycleStart
  public void start()
  {
    final Integer port = config.getPort();
    if (port == null) {
      log.info("PostgreSQL wire protocol endpoint disabled; set [%s.port] to enable it.", PgWireConfig.CONFIG_BASE);
      return;
    }
    server = new PgWireServer(backend, port, config.getMaxConnections());
    server.start();
  }

  @LifecycleStop
  public void stop()
  {
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  /**
   * Bound port of the running endpoint.
   */
  public int getPort()
  {
    if (server == null) {
      throw new IllegalStateException("PostgreSQL wire protocol endpoint is not running");
    }
    return server.getPort();
  }
}
