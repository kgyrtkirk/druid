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

import org.apache.shardingsphere.database.protocol.postgresql.packet.identifier.PostgreSQLIdentifierPacket;
import org.apache.shardingsphere.database.protocol.postgresql.packet.identifier.PostgreSQLIdentifierTag;
import org.apache.shardingsphere.database.protocol.postgresql.packet.identifier.PostgreSQLMessagePacketType;
import org.apache.shardingsphere.database.protocol.postgresql.payload.PostgreSQLPacketPayload;

import java.util.List;

/**
 * DataRow message. ShardingSphere's own packet hex-encodes {@code byte[]} cells and its binary codecs are
 * time-zone dependent, so cells are encoded by {@link PgValueEncoder} instead.
 */
final class PgDataRowPacket extends PostgreSQLIdentifierPacket
{
  private static final int NULL_LENGTH = -1;

  private final List<PgColumn> columns;
  private final boolean[] binary;
  private final Object[] row;

  PgDataRowPacket(final List<PgColumn> columns, final boolean[] binary, final Object[] row)
  {
    this.columns = columns;
    this.binary = binary;
    this.row = row;
  }

  @Override
  protected void write(final PostgreSQLPacketPayload payload)
  {
    payload.writeInt2(row.length);
    for (int i = 0; i < row.length; i++) {
      if (row[i] == null) {
        payload.writeInt4(NULL_LENGTH);
        continue;
      }
      final PgType type = columns.get(i).type();
      final byte[] bytes = binary[i] ? PgValueEncoder.binary(type, row[i]) : PgValueEncoder.text(type, row[i]);
      payload.writeInt4(bytes.length);
      payload.writeBytes(bytes);
    }
  }

  @Override
  public PostgreSQLIdentifierTag getIdentifier()
  {
    return PostgreSQLMessagePacketType.DATA_ROW;
  }
}
