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

/**
 * BackendKeyData message; ShardingSphere declares the tag but ships no packet for it.
 */
final class PgBackendKeyDataPacket extends PostgreSQLIdentifierPacket
{
  private final PgCancelRegistry.Key key;

  PgBackendKeyDataPacket(final PgCancelRegistry.Key key)
  {
    this.key = key;
  }

  @Override
  protected void write(final PostgreSQLPacketPayload payload)
  {
    payload.writeInt4(key.processId());
    payload.writeInt4(key.secret());
  }

  @Override
  public PostgreSQLIdentifierTag getIdentifier()
  {
    return PostgreSQLMessagePacketType.BACKEND_KEY_DATA;
  }
}
