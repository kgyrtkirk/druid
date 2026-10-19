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

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSetOption;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.druid.error.InvalidSqlInput;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.pgwire.PgColumn;
import org.apache.druid.pgwire.PgException;
import org.apache.druid.pgwire.PgResult;
import org.apache.druid.pgwire.PgSession;
import org.apache.druid.pgwire.PgStatement;
import org.apache.druid.query.context.QueryContextParameterValidator;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.PreparedStatement;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.parser.DruidSqlParser;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Druid SQL session of one PostgreSQL connection. Session-level {@code SET k = v} statements accumulate into the
 * user-provided query context of every later statement.
 */
final class DruidPgSession implements PgSession
{
  private static final Pattern TRANSACTION_COMMAND = Pattern.compile(
      "^\\s*(BEGIN|START\\s+TRANSACTION|COMMIT|END|ROLLBACK|ABORT|SAVEPOINT|RELEASE)\\b",
      Pattern.CASE_INSENSITIVE
  );
  private static final String SET_TAG = "SET";

  private final SqlStatementFactory statementFactory;
  private final Map<String, Object> systemDefaultContext;
  private final AuthenticationResult authResult;
  private final Map<String, Object> sessionContext = new ConcurrentHashMap<>();

  @Nullable
  private volatile DirectStatement runningStatement;

  DruidPgSession(
      final SqlStatementFactory statementFactory,
      final Map<String, Object> systemDefaultContext,
      final AuthenticationResult authResult,
      final Map<String, Object> initialContext
  )
  {
    this.statementFactory = statementFactory;
    this.systemDefaultContext = systemDefaultContext;
    this.authResult = authResult;
    this.sessionContext.putAll(initialContext);
  }

  @Override
  public PgResult execute(final String sql)
  {
    rejectTransactionCommand(sql);
    final Map<String, Object> settings = parseSessionSettings(sql);
    if (settings != null) {
      sessionContext.putAll(settings);
      return PgResult.command(SET_TAG);
    }
    return run(statementFactory.directStatement(queryPlus(sql)));
  }

  @Override
  public PgStatement prepare(final String sql)
  {
    rejectTransactionCommand(sql);
    if (parseSessionSettings(sql) != null) {
      return statement(List.of(), sql);
    }
    final PreparedStatement prepared = statementFactory.preparedStatement(queryPlus(sql));
    final List<PgColumn> columns;
    try {
      columns = DruidPgTypes.columns(prepared.prepare().getReturnedRowType());
    }
    catch (Throwable t) {
      prepared.closeWithError(t);
      throw DruidPgErrors.translate(t);
    }
    // Druid re-plans on every execution anyway, so only the result shape is kept.
    prepared.close();
    return statement(columns, sql);
  }

  @Override
  public void cancel()
  {
    final DirectStatement statement = runningStatement;
    if (statement != null) {
      statement.cancel();
    }
  }

  @Override
  public void close()
  {
    cancel();
  }

  private PgStatement statement(final List<PgColumn> columns, final String sql)
  {
    return new PgStatement()
    {
      @Override
      public List<PgColumn> getColumns()
      {
        return columns;
      }

      @Override
      public PgResult execute()
      {
        return DruidPgSession.this.execute(sql);
      }
    };
  }

  private SqlQueryPlus queryPlus(final String sql)
  {
    try {
      return SqlQueryPlus.builder(sql)
                         .systemDefaultContext(systemDefaultContext)
                         .queryContext(Map.copyOf(sessionContext))
                         .auth(authResult)
                         .build();
    }
    catch (RuntimeException e) {
      throw DruidPgErrors.translate(e);
    }
  }

  private PgResult run(final DirectStatement statement)
  {
    runningStatement = statement;
    try {
      final DirectStatement.ResultSet plan = statement.plan();
      final List<PgColumn> columns = DruidPgTypes.columns(statement.prepareResult().getReturnedRowType());
      final Yielder<Object[]> yielder = Yielders.each(plan.run().getResults());
      return PgResult.rows(columns, new StatementRows(statement, columns, yielder));
    }
    catch (Throwable t) {
      runningStatement = null;
      statement.closeWithError(t);
      throw DruidPgErrors.translate(t);
    }
  }

  private static void rejectTransactionCommand(final String sql)
  {
    if (TRANSACTION_COMMAND.matcher(sql).find()) {
      throw new PgException(PgException.FEATURE_NOT_SUPPORTED, "Transactions are not supported");
    }
  }

  /**
   * Returns the settings of a statement made only of {@code SET} statements, or null for any other statement.
   */
  @Nullable
  private static Map<String, Object> parseSessionSettings(final String sql)
  {
    final SqlNodeList statements;
    try {
      statements = SqlParser.create(sql, DruidSqlParser.PARSER_CONFIG).parseStmtList();
    }
    catch (SqlParseException e) {
      // Not a SET statement; Druid's own planning path reports the parse error with its usual translation.
      return null;
    }
    final Map<String, Object> settings = new LinkedHashMap<>();
    for (SqlNode node : statements) {
      if (!(node instanceof SqlSetOption option)) {
        return null;
      }
      if (!(option.getValue() instanceof SqlLiteral literal)) {
        throw DruidPgErrors.translate(
            InvalidSqlInput.exception("Assigned value must be a literal for SET statement[%s]", option)
        );
      }
      settings.put(option.getName().getSimple(), DruidSqlParser.sqlLiteralToJavaValue(literal, SET_TAG));
    }
    try {
      QueryContextParameterValidator.validate(settings);
    }
    catch (RuntimeException e) {
      throw DruidPgErrors.translate(e);
    }
    return settings;
  }

  /**
   * Rows of a running statement; closing ends the statement and emits its metrics.
   */
  private final class StatementRows implements CloseableIterator<Object[]>
  {
    private final DirectStatement statement;
    private final List<PgColumn> columns;
    private Yielder<Object[]> yielder;
    @Nullable
    private Throwable failure;

    private StatementRows(final DirectStatement statement, final List<PgColumn> columns, final Yielder<Object[]> yielder)
    {
      this.statement = statement;
      this.columns = columns;
      this.yielder = yielder;
    }

    @Override
    public boolean hasNext()
    {
      return !yielder.isDone();
    }

    @Override
    public Object[] next()
    {
      if (yielder.isDone()) {
        throw new NoSuchElementException();
      }
      final Object[] row = DruidPgTypes.convertRow(columns, yielder.get());
      try {
        yielder = yielder.next(null);
      }
      catch (Throwable t) {
        failure = t;
        throw DruidPgErrors.translate(t);
      }
      return row;
    }

    @Override
    public void close() throws IOException
    {
      try {
        yielder.close();
      }
      finally {
        runningStatement = null;
        if (failure == null) {
          statement.close();
        } else {
          statement.closeWithError(failure);
        }
      }
    }
  }
}
