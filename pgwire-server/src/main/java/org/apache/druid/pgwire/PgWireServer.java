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

import com.google.common.base.Preconditions;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.shardingsphere.database.protocol.codec.PacketCodec;
import org.apache.shardingsphere.database.protocol.postgresql.codec.PostgreSQLPacketCodecEngine;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Serves a {@link PgBackend} over the PostgreSQL v3 wire protocol with cleartext password authentication.
 */
public final class PgWireServer
{
  private static final Logger log = new Logger(PgWireServer.class);
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

  private final PgBackend backend;
  private final int port;
  private final int maxConnections;
  private final PgCancelRegistry cancelRegistry = new PgCancelRegistry();
  private final AtomicInteger openConnections = new AtomicInteger();

  @Nullable
  private EventLoopGroup eventLoopGroup;
  @Nullable
  private ExecutorService statementExecutor;
  @Nullable
  private Channel serverChannel;

  /**
   * @param port 0 binds an ephemeral port, see {@link #getPort()}
   */
  public PgWireServer(final PgBackend backend, final int port, final int maxConnections)
  {
    Preconditions.checkArgument(maxConnections > 0, "maxConnections must be positive");
    this.backend = backend;
    this.port = port;
    this.maxConnections = maxConnections;
  }

  public void start()
  {
    Preconditions.checkState(serverChannel == null, "Server already started");
    eventLoopGroup = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
    statementExecutor = Execs.multiThreaded(maxConnections, "pgwire-statement-%d");
    final ExecutorService executor = statementExecutor;
    serverChannel = new ServerBootstrap()
        .group(eventLoopGroup)
        .channel(NioServerSocketChannel.class)
        .childHandler(new ChannelInitializer<SocketChannel>()
        {
          @Override
          protected void initChannel(final SocketChannel channel)
          {
            channel.pipeline().addLast(
                new PacketCodec(new PostgreSQLPacketCodecEngine()),
                new PgConnectionHandler(backend, cancelRegistry, openConnections, maxConnections, executor)
            );
          }
        })
        .bind(port)
        .syncUninterruptibly()
        .channel();
    log.info("PostgreSQL wire protocol server listening on port [%d].", getPort());
  }

  /**
   * Bound port; valid once {@link #start()} returned.
   */
  public int getPort()
  {
    Preconditions.checkState(serverChannel != null, "Server not started");
    return ((InetSocketAddress) serverChannel.localAddress()).getPort();
  }

  public void stop()
  {
    if (serverChannel != null) {
      serverChannel.close().syncUninterruptibly();
      serverChannel = null;
    }
    if (eventLoopGroup != null) {
      eventLoopGroup.shutdownGracefully(0, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS).syncUninterruptibly();
      eventLoopGroup = null;
    }
    if (statementExecutor != null) {
      statementExecutor.shutdownNow();
      statementExecutor = null;
    }
  }
}
