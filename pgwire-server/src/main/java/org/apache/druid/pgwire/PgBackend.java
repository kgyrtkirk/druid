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

import java.util.Map;

/**
 * Query engine served by {@link PgWireServer}; knows nothing of the wire protocol.
 */
public interface PgBackend
{
  /**
   * Opens a session for one client connection.
   *
   * @param startupParams parameters of the client's StartupMessage, e.g. {@code user}, {@code options}
   * @throws PgException with {@link PgException#INVALID_PASSWORD} when the credentials are rejected
   */
  PgSession authenticate(String user, String password, Map<String, String> startupParams);
}
