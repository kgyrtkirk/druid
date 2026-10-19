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

import org.apache.druid.java.util.common.StringUtils;

/**
 * Failure reported to the client as an ErrorResponse carrying {@link #getSqlState()}.
 */
public class PgException extends RuntimeException
{
  public static final String FEATURE_NOT_SUPPORTED = "0A000";
  public static final String INVALID_PASSWORD = "28P01";
  public static final String PROTOCOL_VIOLATION = "08P01";
  public static final String TOO_MANY_CONNECTIONS = "53300";
  public static final String INTERNAL_ERROR = "XX000";

  private final String sqlState;

  public PgException(final String sqlState, final String format, final Object... args)
  {
    super(StringUtils.nonStrictFormat(format, args));
    this.sqlState = sqlState;
  }

  public PgException(final Throwable cause, final String sqlState, final String format, final Object... args)
  {
    super(StringUtils.nonStrictFormat(format, args), cause);
    this.sqlState = sqlState;
  }

  public String getSqlState()
  {
    return sqlState;
  }
}
