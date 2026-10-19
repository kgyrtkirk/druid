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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.shardingsphere.database.protocol.constant.CommonConstants;
import org.apache.shardingsphere.database.protocol.postgresql.constant.PostgreSQLMessageSeverityLevel;
import org.apache.shardingsphere.database.protocol.postgresql.constant.PostgreSQLValueFormat;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.PostgreSQLCommandPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.PostgreSQLCommandPacketFactory;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.PostgreSQLCommandPacketType;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.PostgreSQLColumnDescription;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.PostgreSQLEmptyQueryResponsePacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.PostgreSQLNoDataPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.PostgreSQLParameterDescriptionPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.PostgreSQLRowDescriptionPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.extended.PostgreSQLAggregatedCommandPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.extended.PostgreSQLColumnType;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.extended.bind.PostgreSQLBindCompletePacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.extended.bind.PostgreSQLComBindPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.extended.close.PostgreSQLCloseCompletePacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.extended.close.PostgreSQLComClosePacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.extended.describe.PostgreSQLComDescribePacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.extended.execute.PostgreSQLComExecutePacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.extended.flush.PostgreSQLComFlushPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.extended.parse.PostgreSQLComParsePacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.extended.parse.PostgreSQLParseCompletePacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.extended.sync.PostgreSQLComSyncPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.command.query.simple.PostgreSQLComQueryPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.generic.PostgreSQLComTerminationPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.generic.PostgreSQLCommandCompletePacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.generic.PostgreSQLErrorResponsePacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.generic.PostgreSQLReadyForQueryPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.handshake.PostgreSQLAuthenticationOKPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.handshake.PostgreSQLParameterStatusPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.handshake.PostgreSQLPasswordMessagePacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.handshake.PostgreSQLSSLUnwillingPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.handshake.authentication.PostgreSQLPasswordAuthenticationPacket;
import org.apache.shardingsphere.database.protocol.postgresql.payload.PostgreSQLPacketPayload;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Protocol state machine of one client connection.
 * <p>
 * Messages are handled in arrival order on a per-connection sequential view of a shared executor, never on the
 * event loop, so a backend may block. Row streaming pauses while the channel is not writable.
 */
final class PgConnectionHandler extends ChannelInboundHandlerAdapter
{
  private static final Logger log = new Logger(PgConnectionHandler.class);

  private static final int PROTOCOL_VERSION_3 = 196608;
  private static final int CANCEL_REQUEST_CODE = 80877102;
  private static final int SSL_REQUEST_CODE = 80877103;
  private static final int MAX_BIND_PARAMETERS = 65535;
  private static final Map<String, String> SERVER_PARAMETERS = ImmutableMap.<String, String>builder()
      .put("server_version", "16.0")
      .put("server_encoding", "UTF8")
      .put("client_encoding", "UTF8")
      .put("DateStyle", "ISO, MDY")
      .put("TimeZone", "UTC")
      .put("integer_datetimes", "on")
      .put("standard_conforming_strings", "on")
      .build();

  private enum Phase
  {
    STARTUP,
    PASSWORD,
    READY
  }

  private record Portal(PgStatement statement, boolean[] binary)
  {
  }

  private final PgBackend backend;
  private final PgCancelRegistry cancelRegistry;
  private final AtomicInteger openConnections;
  private final int maxConnections;
  private final Executor serialExecutor;
  private final Object writabilityLock = new Object();

  // Confined to serialExecutor.
  private final Map<String, PgStatement> statements = new HashMap<>();
  private final Map<String, Portal> portals = new HashMap<>();
  private Phase phase = Phase.STARTUP;
  private Map<String, String> startupParams = Map.of();
  @Nullable
  private PgSession session;
  @Nullable
  private PgCancelRegistry.Key cancelKey;
  private boolean skipUntilSync;

  private volatile boolean rejected;

  PgConnectionHandler(
      final PgBackend backend,
      final PgCancelRegistry cancelRegistry,
      final AtomicInteger openConnections,
      final int maxConnections,
      final Executor sharedExecutor
  )
  {
    this.backend = backend;
    this.cancelRegistry = cancelRegistry;
    this.openConnections = openConnections;
    this.maxConnections = maxConnections;
    this.serialExecutor = MoreExecutors.newSequentialExecutor(sharedExecutor);
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx)
  {
    ctx.channel().attr(CommonConstants.CHARSET_ATTRIBUTE_KEY).set(StandardCharsets.UTF_8);
    if (openConnections.incrementAndGet() > maxConnections) {
      rejected = true;
      closeWithError(ctx, new PgException(
          PgException.TOO_MANY_CONNECTIONS,
          "Connection limit [%s] reached",
          maxConnections
      ));
    }
    ctx.fireChannelActive();
  }

  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg)
  {
    final ByteBuf message = (ByteBuf) msg;
    if (rejected) {
      message.release();
      return;
    }
    serialExecutor.execute(() -> {
      try {
        handle(ctx, message);
      }
      catch (Throwable t) {
        closeWithError(ctx, t);
      }
      finally {
        message.release();
      }
    });
  }

  @Override
  public void channelWritabilityChanged(final ChannelHandlerContext ctx)
  {
    wakeWriters();
    ctx.fireChannelWritabilityChanged();
  }

  @Override
  public void channelInactive(final ChannelHandlerContext ctx)
  {
    openConnections.decrementAndGet();
    wakeWriters();
    serialExecutor.execute(this::closeSession);
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause)
  {
    serialExecutor.execute(() -> closeWithError(ctx, cause));
  }

  private void handle(final ChannelHandlerContext ctx, final ByteBuf message)
  {
    final PostgreSQLPacketPayload payload = new PostgreSQLPacketPayload(message, StandardCharsets.UTF_8);
    switch (phase) {
      case STARTUP -> handleStartup(ctx, payload);
      case PASSWORD -> handlePassword(ctx, payload);
      case READY -> handleCommand(ctx, payload);
    }
  }

  private void handleStartup(final ChannelHandlerContext ctx, final PostgreSQLPacketPayload payload)
  {
    payload.readInt4();
    final int code = payload.readInt4();
    switch (code) {
      case SSL_REQUEST_CODE -> ctx.writeAndFlush(new PostgreSQLSSLUnwillingPacket());
      case CANCEL_REQUEST_CODE -> {
        cancelRegistry.cancel(new PgCancelRegistry.Key(payload.readInt4(), payload.readInt4()));
        ctx.close();
      }
      case PROTOCOL_VERSION_3 -> {
        startupParams = readStartupParams(payload);
        phase = Phase.PASSWORD;
        ctx.writeAndFlush(new PostgreSQLPasswordAuthenticationPacket());
      }
      default -> throw new PgException(
          PgException.FEATURE_NOT_SUPPORTED,
          "Startup request code [%s] is not supported",
          code
      );
    }
  }

  private static Map<String, String> readStartupParams(final PostgreSQLPacketPayload payload)
  {
    final Map<String, String> params = new HashMap<>();
    while (payload.bytesBeforeZero() > 0) {
      params.put(payload.readStringNul(), payload.readStringNul());
    }
    return params;
  }

  private void handlePassword(final ChannelHandlerContext ctx, final PostgreSQLPacketPayload payload)
  {
    final int type = payload.readInt1();
    if (type != PostgreSQLCommandPacketType.PASSWORD.getValue()) {
      throw new PgException(
          PgException.PROTOCOL_VIOLATION,
          "Expected password message but got message type [%s]",
          (char) type
      );
    }
    final String password = new PostgreSQLPasswordMessagePacket(payload).getDigest();
    session = backend.authenticate(startupParams.get("user"), password, startupParams);
    cancelKey = cancelRegistry.register(session);
    phase = Phase.READY;
    ctx.write(new PostgreSQLAuthenticationOKPacket());
    SERVER_PARAMETERS.forEach((key, value) -> ctx.write(new PostgreSQLParameterStatusPacket(key, value)));
    ctx.write(new PgBackendKeyDataPacket(cancelKey));
    ctx.writeAndFlush(PostgreSQLReadyForQueryPacket.NOT_IN_TRANSACTION);
  }

  private void handleCommand(final ChannelHandlerContext ctx, final PostgreSQLPacketPayload payload)
  {
    final ByteBuf buf = payload.getByteBuf();
    final PostgreSQLCommandPacketType type = PostgreSQLCommandPacketType.valueOf(buf.getUnsignedByte(buf.readerIndex()));
    final PostgreSQLCommandPacket packet = PostgreSQLCommandPacketFactory.newInstance(type, payload);
    switch (packet) {
      case PostgreSQLComQueryPacket query -> simpleQuery(ctx, query.getSQL());
      case PostgreSQLAggregatedCommandPacket aggregated -> {
        for (PostgreSQLCommandPacket each : aggregated.getPackets()) {
          extendedQuery(ctx, each);
        }
        ctx.flush();
      }
      case PostgreSQLComTerminationPacket ignored -> ctx.close();
      default -> throw new PgException(PgException.FEATURE_NOT_SUPPORTED, "Message type [%s] is not supported", type);
    }
  }

  private void simpleQuery(final ChannelHandlerContext ctx, final String sql)
  {
    try {
      if (sql.isBlank()) {
        ctx.write(new PostgreSQLEmptyQueryResponsePacket());
      } else {
        try (PgResult result = session().execute(sql)) {
          final List<PgColumn> columns = result.getColumns();
          final boolean[] binary = new boolean[columns.size()];
          if (!columns.isEmpty()) {
            ctx.write(rowDescription(columns, binary));
          }
          writeRowsAndCompletion(ctx, columns, binary, result);
        }
      }
    }
    catch (Exception e) {
      ctx.write(errorResponse(PostgreSQLMessageSeverityLevel.ERROR, e));
    }
    ctx.writeAndFlush(PostgreSQLReadyForQueryPacket.NOT_IN_TRANSACTION);
  }

  /**
   * After an error, messages are discarded until Sync, which then reports readiness again.
   */
  private void extendedQuery(final ChannelHandlerContext ctx, final PostgreSQLCommandPacket packet)
  {
    if (skipUntilSync && !(packet instanceof PostgreSQLComSyncPacket)) {
      return;
    }
    try {
      switch (packet) {
        case PostgreSQLComParsePacket parse -> parse(ctx, parse);
        case PostgreSQLComBindPacket bind -> bind(ctx, bind);
        case PostgreSQLComDescribePacket describe -> describe(ctx, describe);
        case PostgreSQLComExecutePacket execute -> execute(ctx, execute);
        case PostgreSQLComClosePacket close -> close(ctx, close);
        case PostgreSQLComFlushPacket ignored -> ctx.flush();
        case PostgreSQLComSyncPacket ignored -> {
          skipUntilSync = false;
          portals.clear();
          ctx.write(PostgreSQLReadyForQueryPacket.NOT_IN_TRANSACTION);
        }
        default -> throw new PgException(
            PgException.FEATURE_NOT_SUPPORTED,
            "Message type [%s] is not supported",
            packet.getIdentifier().getValue()
        );
      }
    }
    catch (Exception e) {
      ctx.write(errorResponse(PostgreSQLMessageSeverityLevel.ERROR, e));
      skipUntilSync = true;
    }
  }

  private void parse(final ChannelHandlerContext ctx, final PostgreSQLComParsePacket parse)
  {
    final String sql = parse.getSQL();
    if (!parse.readParameterTypes().isEmpty()) {
      throw parametersNotSupported();
    }
    statements.put(parse.getStatementId(), session().prepare(sql));
    ctx.write(PostgreSQLParseCompletePacket.getInstance());
  }

  private void bind(final ChannelHandlerContext ctx, final PostgreSQLComBindPacket bind)
  {
    final PgStatement statement = lookup(statements, bind.getStatementId(), "Prepared statement");
    if (!bind.readParameters(Collections.nCopies(MAX_BIND_PARAMETERS, PostgreSQLColumnType.UNSPECIFIED)).isEmpty()) {
      throw parametersNotSupported();
    }
    final boolean[] binary = resultFormats(bind.readResultFormats(), statement.getColumns().size());
    portals.put(bind.getPortal(), new Portal(statement, binary));
    ctx.write(PostgreSQLBindCompletePacket.getInstance());
  }

  private void describe(final ChannelHandlerContext ctx, final PostgreSQLComDescribePacket describe)
  {
    switch (describe.getType()) {
      case 'S' -> {
        final PgStatement statement = lookup(statements, describe.getName(), "Prepared statement");
        ctx.write(new PostgreSQLParameterDescriptionPacket(List.of()));
        writeRowDescriptionOrNoData(ctx, statement.getColumns(), new boolean[statement.getColumns().size()]);
      }
      case 'P' -> {
        final Portal portal = lookup(portals, describe.getName(), "Portal");
        writeRowDescriptionOrNoData(ctx, portal.statement().getColumns(), portal.binary());
      }
      default -> throw new PgException(
          PgException.PROTOCOL_VIOLATION,
          "Describe target [%s] is not valid",
          describe.getType()
      );
    }
  }

  private void execute(final ChannelHandlerContext ctx, final PostgreSQLComExecutePacket execute)
  {
    final Portal portal = lookup(portals, execute.getPortal(), "Portal");
    if (execute.getMaxRows() > 0) {
      // FIXME: support portal suspension (pgjdbc setMaxRows)
      throw new PgException(
          PgException.FEATURE_NOT_SUPPORTED,
          "Row limit [%s] on Execute is not supported",
          execute.getMaxRows()
      );
    }
    try (PgResult result = portal.statement().execute()) {
      writeRowsAndCompletion(ctx, portal.statement().getColumns(), portal.binary(), result);
    }
  }

  private void close(final ChannelHandlerContext ctx, final PostgreSQLComClosePacket close)
  {
    switch (close.getType()) {
      case PREPARED_STATEMENT -> statements.remove(close.getName());
      case PORTAL -> portals.remove(close.getName());
    }
    ctx.write(PostgreSQLCloseCompletePacket.getInstance());
  }

  private void writeRowsAndCompletion(
      final ChannelHandlerContext ctx,
      final List<PgColumn> columns,
      final boolean[] binary,
      final PgResult result
  )
  {
    long rowCount = 0;
    final CloseableIterator<Object[]> rows = result.getRows();
    while (rows.hasNext()) {
      awaitWritable(ctx);
      ctx.write(new PgDataRowPacket(columns, binary, rows.next()));
      rowCount++;
    }
    ctx.write(new PostgreSQLCommandCompletePacket(result.getCommandTag(), rowCount));
  }

  private void awaitWritable(final ChannelHandlerContext ctx)
  {
    final Channel channel = ctx.channel();
    if (channel.isWritable()) {
      return;
    }
    ctx.flush();
    synchronized (writabilityLock) {
      while (!channel.isWritable()) {
        if (!channel.isActive()) {
          throw new PgException(PgException.INTERNAL_ERROR, "Client disconnected while receiving rows");
        }
        try {
          writabilityLock.wait();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new PgException(e, PgException.INTERNAL_ERROR, "Interrupted while sending rows");
        }
      }
    }
  }

  private void wakeWriters()
  {
    synchronized (writabilityLock) {
      writabilityLock.notifyAll();
    }
  }

  private void writeRowDescriptionOrNoData(
      final ChannelHandlerContext ctx,
      final List<PgColumn> columns,
      final boolean[] binary
  )
  {
    if (columns.isEmpty()) {
      ctx.write(PostgreSQLNoDataPacket.getInstance());
    } else {
      ctx.write(rowDescription(columns, binary));
    }
  }

  private static PostgreSQLRowDescriptionPacket rowDescription(final List<PgColumn> columns, final boolean[] binary)
  {
    final List<PostgreSQLColumnDescription> descriptions = new ArrayList<>(columns.size());
    for (int i = 0; i < columns.size(); i++) {
      final PgType type = columns.get(i).type();
      descriptions.add(new PostgreSQLColumnDescription(
          columns.get(i).name(),
          0,
          jdbcType(type),
          typeLength(type),
          null,
          (binary[i] ? PostgreSQLValueFormat.BINARY : PostgreSQLValueFormat.TEXT).getCode()
      ));
    }
    return new PostgreSQLRowDescriptionPacket(descriptions);
  }

  private static int jdbcType(final PgType type)
  {
    return switch (type) {
      case VARCHAR -> Types.VARCHAR;
      case INT4 -> Types.INTEGER;
      case INT8 -> Types.BIGINT;
      case FLOAT4 -> Types.REAL;
      case FLOAT8 -> Types.DOUBLE;
      case BOOL -> Types.BOOLEAN;
      case DATE -> Types.DATE;
      case TIMESTAMP -> Types.TIMESTAMP;
    };
  }

  private static int typeLength(final PgType type)
  {
    return switch (type) {
      case VARCHAR -> -1;
      case BOOL -> 1;
      case INT4, FLOAT4, DATE -> 4;
      case INT8, FLOAT8, TIMESTAMP -> 8;
    };
  }

  private static boolean[] resultFormats(final List<PostgreSQLValueFormat> formats, final int columnCount)
  {
    final boolean[] binary = new boolean[columnCount];
    if (formats.size() == 1) {
      Arrays.fill(binary, formats.get(0) == PostgreSQLValueFormat.BINARY);
    } else if (formats.size() == columnCount) {
      for (int i = 0; i < columnCount; i++) {
        binary[i] = formats.get(i) == PostgreSQLValueFormat.BINARY;
      }
    } else if (!formats.isEmpty()) {
      throw new PgException(
          PgException.PROTOCOL_VIOLATION,
          "Bind has [%s] result formats for [%s] columns",
          formats.size(),
          columnCount
      );
    }
    return binary;
  }

  private static <T> T lookup(final Map<String, T> map, final String name, final String kind)
  {
    final T value = map.get(name);
    if (value == null) {
      throw new PgException(PgException.PROTOCOL_VIOLATION, "%s [%s] does not exist", kind, name);
    }
    return value;
  }

  private static PgException parametersNotSupported()
  {
    return new PgException(PgException.FEATURE_NOT_SUPPORTED, "Statement parameters are not supported");
  }

  private PgSession session()
  {
    if (session == null) {
      throw new PgException(PgException.PROTOCOL_VIOLATION, "Connection is not authenticated");
    }
    return session;
  }

  private void closeWithError(final ChannelHandlerContext ctx, final Throwable t)
  {
    log.debug(t, "Closing connection [%s] after error", ctx.channel().remoteAddress());
    if (ctx.channel().isActive()) {
      ctx.writeAndFlush(errorResponse(PostgreSQLMessageSeverityLevel.FATAL, t))
         .addListener(ChannelFutureListener.CLOSE);
    }
  }

  private void closeSession()
  {
    if (cancelKey != null) {
      cancelRegistry.unregister(cancelKey);
      cancelKey = null;
    }
    if (session != null) {
      session.close();
      session = null;
    }
    statements.clear();
    portals.clear();
  }

  private static PostgreSQLErrorResponsePacket errorResponse(final String severity, final Throwable t)
  {
    final String sqlState;
    if (t instanceof PgException pgException) {
      sqlState = pgException.getSqlState();
    } else {
      sqlState = PgException.INTERNAL_ERROR;
      log.warn(t, "Statement failed with an unexpected error");
    }
    final String message = Strings.isNullOrEmpty(t.getMessage()) ? t.getClass().getName() : t.getMessage();
    return PostgreSQLErrorResponsePacket.newBuilder(severity, sqlState, message).build();
  }
}
