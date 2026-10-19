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

import org.apache.druid.error.DruidException;
import org.apache.druid.pgwire.PgException;
import org.apache.druid.query.QueryException;
import org.apache.druid.server.security.ForbiddenException;

import javax.annotation.Nullable;

/**
 * Translates Druid failures into {@link PgException}s by exception type, never by message.
 */
final class DruidPgErrors
{
  private static final String INVALID_PARAMETER_VALUE = "22023";
  private static final String QUERY_CANCELED = "57014";
  private static final String INVALID_AUTHORIZATION = "28000";
  private static final String SYNTAX_OR_ACCESS_RULE = "42000";
  private static final String INSUFFICIENT_PRIVILEGE = "42501";
  private static final String UNDEFINED_OBJECT = "42704";
  private static final String INSUFFICIENT_RESOURCES = "53000";
  private static final String OBJECT_NOT_IN_STATE = "55000";
  private static final String CANNOT_CONNECT_NOW = "57P03";

  private DruidPgErrors()
  {
  }

  /**
   * Unrecognized failures are returned unchanged so the server reports them as internal errors.
   */
  static RuntimeException translate(final Throwable t)
  {
    if (t instanceof PgException pgException) {
      return pgException;
    }
    if (t instanceof DruidException druidException) {
      return new PgException(t, sqlState(druidException.getCategory()), "%s", t.getMessage());
    }
    if (t instanceof QueryException queryException) {
      return new PgException(t, sqlState(queryException.getErrorCode()), "%s", t.getMessage());
    }
    if (t instanceof ForbiddenException) {
      return new PgException(t, INSUFFICIENT_PRIVILEGE, "%s", t.getMessage());
    }
    return t instanceof RuntimeException runtimeException ? runtimeException : new RuntimeException(t);
  }

  private static String sqlState(final DruidException.Category category)
  {
    return switch (category) {
      case INVALID_INPUT -> SYNTAX_OR_ACCESS_RULE;
      case UNAUTHORIZED -> INVALID_AUTHORIZATION;
      case FORBIDDEN -> INSUFFICIENT_PRIVILEGE;
      case NOT_FOUND -> UNDEFINED_OBJECT;
      case CONFLICT -> OBJECT_NOT_IN_STATE;
      case CAPACITY_EXCEEDED -> INSUFFICIENT_RESOURCES;
      case CANCELED, TIMEOUT -> QUERY_CANCELED;
      case SERVICE_UNAVAILABLE -> CANNOT_CONNECT_NOW;
      case UNSUPPORTED -> PgException.FEATURE_NOT_SUPPORTED;
      case DEFENSIVE, RUNTIME_FAILURE, UNCATEGORIZED -> PgException.INTERNAL_ERROR;
    };
  }

  private static String sqlState(@Nullable final String errorCode)
  {
    if (errorCode == null) {
      return PgException.INTERNAL_ERROR;
    }
    return switch (errorCode) {
      case QueryException.QUERY_CANCELED_ERROR_CODE, QueryException.QUERY_TIMEOUT_ERROR_CODE -> QUERY_CANCELED;
      case QueryException.QUERY_CAPACITY_EXCEEDED_ERROR_CODE,
           QueryException.RESOURCE_LIMIT_EXCEEDED_ERROR_CODE -> INSUFFICIENT_RESOURCES;
      case QueryException.UNSUPPORTED_OPERATION_ERROR_CODE,
           QueryException.QUERY_UNSUPPORTED_ERROR_CODE,
           QueryException.SQL_QUERY_UNSUPPORTED_ERROR_CODE -> PgException.FEATURE_NOT_SUPPORTED;
      case QueryException.SQL_PARSE_FAILED_ERROR_CODE,
           QueryException.PLAN_VALIDATION_FAILED_ERROR_CODE -> SYNTAX_OR_ACCESS_RULE;
      case QueryException.BAD_QUERY_CONTEXT_ERROR_CODE -> INVALID_PARAMETER_VALUE;
      case QueryException.UNAUTHORIZED_ERROR_CODE -> INVALID_AUTHORIZATION;
      default -> PgException.INTERNAL_ERROR;
    };
  }
}
