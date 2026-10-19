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

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;

/**
 * Encodes non-null cell values in PostgreSQL text or binary format.
 */
final class PgValueEncoder
{
  private static final LocalDate PG_EPOCH_DATE = LocalDate.of(2000, 1, 1);
  private static final LocalDateTime PG_EPOCH = PG_EPOCH_DATE.atStartOfDay();
  private static final DateTimeFormatter TIMESTAMP_TEXT = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral(' ')
      .appendPattern("HH:mm:ss")
      .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
      .toFormatter();

  private PgValueEncoder()
  {
  }

  static byte[] text(final PgType type, final Object value)
  {
    final String text = switch (type) {
      case VARCHAR -> (String) value;
      case INT4 -> String.valueOf(((Number) value).intValue());
      case INT8 -> String.valueOf(((Number) value).longValue());
      case FLOAT4 -> String.valueOf(((Number) value).floatValue());
      case FLOAT8 -> String.valueOf(((Number) value).doubleValue());
      case BOOL -> (Boolean) value ? "t" : "f";
      case DATE -> ((LocalDate) value).toString();
      case TIMESTAMP -> TIMESTAMP_TEXT.format((LocalDateTime) value);
    };
    return StringUtils.toUtf8(text);
  }

  static byte[] binary(final PgType type, final Object value)
  {
    return switch (type) {
      case VARCHAR -> StringUtils.toUtf8((String) value);
      case INT4 -> ByteBuffer.allocate(Integer.BYTES).putInt(((Number) value).intValue()).array();
      case INT8 -> ByteBuffer.allocate(Long.BYTES).putLong(((Number) value).longValue()).array();
      case FLOAT4 -> ByteBuffer.allocate(Float.BYTES).putFloat(((Number) value).floatValue()).array();
      case FLOAT8 -> ByteBuffer.allocate(Double.BYTES).putDouble(((Number) value).doubleValue()).array();
      case BOOL -> new byte[]{(byte) ((Boolean) value ? 1 : 0)};
      case DATE -> ByteBuffer.allocate(Integer.BYTES)
                             .putInt(Math.toIntExact(ChronoUnit.DAYS.between(PG_EPOCH_DATE, (LocalDate) value)))
                             .array();
      case TIMESTAMP -> ByteBuffer.allocate(Long.BYTES)
                                  .putLong(ChronoUnit.MICROS.between(PG_EPOCH, (LocalDateTime) value))
                                  .array();
    };
  }
}
