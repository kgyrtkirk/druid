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

import com.google.common.base.Splitter;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.pgwire.PgBackend;
import org.apache.druid.pgwire.PgException;
import org.apache.druid.pgwire.PgSession;
import org.apache.druid.query.context.QueryContextParameterValidator;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.sql.SqlStatementFactory;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Authenticates PostgreSQL clients through Druid's authenticator chain, as Avatica JDBC connections are.
 */
final class DruidPgBackend implements PgBackend
{
  private static final Logger log = new Logger(DruidPgBackend.class);
  private static final String OPTIONS_PARAM = "options";
  private static final Splitter WHITESPACE = Splitter.onPattern("\\s+").omitEmptyStrings();

  private final List<Authenticator> authenticators;
  private final SqlStatementFactory statementFactory;
  private final Map<String, Object> systemDefaultContext;

  DruidPgBackend(
      final List<Authenticator> authenticators,
      final SqlStatementFactory statementFactory,
      final Map<String, Object> systemDefaultContext
  )
  {
    this.authenticators = authenticators;
    this.statementFactory = statementFactory;
    this.systemDefaultContext = systemDefaultContext;
  }

  /**
   * Startup option {@code -c k=v} (or {@code --k=v}) presets query context parameter {@code k}.
   */
  @Override
  public PgSession authenticate(final String user, final String password, final Map<String, String> startupParams)
  {
    final AuthenticationResult authResult = authenticate(user, password);
    final Map<String, Object> initialContext = parseOptions(startupParams.get(OPTIONS_PARAM));
    try {
      QueryContextParameterValidator.validate(initialContext);
    }
    catch (RuntimeException e) {
      throw DruidPgErrors.translate(e);
    }
    return new DruidPgSession(statementFactory, systemDefaultContext, authResult, initialContext);
  }

  private AuthenticationResult authenticate(@Nullable final String user, final String password)
  {
    final Map<String, Object> credentials = new HashMap<>();
    credentials.put("user", user);
    credentials.put("password", password);
    for (Authenticator authenticator : authenticators) {
      final AuthenticationResult result;
      try {
        result = authenticator.authenticateJDBCContext(credentials);
      }
      catch (RuntimeException e) {
        log.debug(e, "Authenticator [%s] rejected user [%s]", authenticator.getClass().getName(), user);
        throw invalidPassword(user);
      }
      if (result != null) {
        return result;
      }
    }
    throw invalidPassword(user);
  }

  private static PgException invalidPassword(@Nullable final String user)
  {
    return new PgException(PgException.INVALID_PASSWORD, "Password authentication failed for user [%s]", user);
  }

  private static Map<String, Object> parseOptions(@Nullable final String options)
  {
    final Map<String, Object> context = new LinkedHashMap<>();
    if (options == null) {
      return context;
    }
    final Iterator<String> tokens = WHITESPACE.split(options).iterator();
    while (tokens.hasNext()) {
      final String token = tokens.next();
      final String setting;
      if ("-c".equals(token) && tokens.hasNext()) {
        setting = tokens.next();
      } else if (token.startsWith("-c")) {
        setting = token.substring(2);
      } else if (token.startsWith("--")) {
        setting = token.substring(2);
      } else {
        throw new PgException(PgException.PROTOCOL_VIOLATION, "Startup option [%s] is not supported", token);
      }
      final int eq = setting.indexOf('=');
      if (eq <= 0) {
        throw new PgException(PgException.PROTOCOL_VIOLATION, "Startup option [%s] must have the form k=v", setting);
      }
      context.put(setting.substring(0, eq), setting.substring(eq + 1));
    }
    return context;
  }
}
