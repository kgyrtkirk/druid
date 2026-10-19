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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.druid.pgwire.PgColumn;
import org.apache.druid.pgwire.PgException;
import org.apache.druid.pgwire.PgType;

import javax.annotation.Nullable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * Maps Druid SQL result types and values onto {@link PgType}.
 */
final class DruidPgTypes
{
  private DruidPgTypes()
  {
  }

  /**
   * @throws PgException with {@link PgException#FEATURE_NOT_SUPPORTED} for ARRAY, COMPLEX and other non-scalar columns
   */
  static List<PgColumn> columns(final RelDataType rowType)
  {
    final List<PgColumn> columns = new ArrayList<>(rowType.getFieldCount());
    for (RelDataTypeField field : rowType.getFieldList()) {
      columns.add(new PgColumn(field.getName(), type(field)));
    }
    return columns;
  }

  /**
   * Converts a SQL result row in place; TIMESTAMP and DATE cells arrive in Calcite's session-local epoch encoding.
   */
  static Object[] convertRow(final List<PgColumn> columns, final Object[] row)
  {
    for (int i = 0; i < row.length; i++) {
      row[i] = convertValue(columns.get(i).type(), row[i]);
    }
    return row;
  }

  @Nullable
  private static Object convertValue(final PgType type, @Nullable final Object value)
  {
    if (value == null) {
      return null;
    }
    return switch (type) {
      case TIMESTAMP -> {
        final long millis = ((Number) value).longValue();
        yield LocalDateTime.ofEpochSecond(
            Math.floorDiv(millis, 1000L),
            (int) Math.floorMod(millis, 1000L) * 1_000_000,
            ZoneOffset.UTC
        );
      }
      case DATE -> LocalDate.ofEpochDay(((Number) value).longValue());
      default -> value;
    };
  }

  private static PgType type(final RelDataTypeField field)
  {
    return switch (field.getType().getSqlTypeName()) {
      case CHAR, VARCHAR -> PgType.VARCHAR;
      case TINYINT, SMALLINT, INTEGER -> PgType.INT4;
      case BIGINT -> PgType.INT8;
      case FLOAT, REAL -> PgType.FLOAT4;
      case DOUBLE, DECIMAL -> PgType.FLOAT8;
      case BOOLEAN -> PgType.BOOL;
      case DATE -> PgType.DATE;
      case TIMESTAMP -> PgType.TIMESTAMP;
      default -> throw new PgException(
          PgException.FEATURE_NOT_SUPPORTED,
          "Column [%s] has type [%s], which is not supported over the PostgreSQL protocol",
          field.getName(),
          field.getType().getSqlTypeName()
      );
    };
  }
}
