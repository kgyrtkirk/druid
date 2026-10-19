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

import java.io.Closeable;

/**
 * State of one authenticated client connection.
 * <p>
 * Statements are issued by a single thread at a time; {@link #cancel()} may arrive from any thread.
 * Failures surface as {@link PgException}; any other exception is reported as {@link PgException#INTERNAL_ERROR}.
 */
public interface PgSession extends Closeable
{
  /**
   * Runs one SQL statement; the caller closes the result.
   */
  PgResult execute(String sql);

  /**
   * Validates a statement and resolves its result columns without running it.
   */
  PgStatement prepare(String sql);

  /**
   * Aborts the statement currently running in this session, if any.
   */
  void cancel();

  @Override
  void close();
}
