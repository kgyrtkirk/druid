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

import java.security.SecureRandom;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Resolves CancelRequest keys, which arrive on a separate connection, to live sessions.
 */
final class PgCancelRegistry
{
  record Key(int processId, int secret)
  {
  }

  private final SecureRandom random = new SecureRandom();
  private final AtomicInteger nextProcessId = new AtomicInteger();
  private final Map<Integer, Registration> sessions = new ConcurrentHashMap<>();

  Key register(final PgSession session)
  {
    final Key key = new Key(nextProcessId.incrementAndGet(), random.nextInt());
    sessions.put(key.processId(), new Registration(key, session));
    return key;
  }

  void unregister(final Key key)
  {
    sessions.remove(key.processId());
  }

  /**
   * Cancels the session's running statement; unknown or forged keys are ignored, as PostgreSQL does.
   */
  void cancel(final Key key)
  {
    final Registration registration = sessions.get(key.processId());
    if (registration != null && registration.key().equals(key)) {
      registration.session().cancel();
    }
  }

  private record Registration(Key key, PgSession session)
  {
  }
}
