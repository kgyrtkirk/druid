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

/**
 * Column types a {@link PgBackend} may report; each constant names the Java type its non-null cell values must have.
 */
public enum PgType
{
  /** {@link String}. */
  VARCHAR,
  /** {@link Number}, narrowed to int. */
  INT4,
  /** {@link Number}, narrowed to long. */
  INT8,
  /** {@link Number}, narrowed to float. */
  FLOAT4,
  /** {@link Number}, narrowed to double. */
  FLOAT8,
  /** {@link Boolean}. */
  BOOL,
  /** {@link java.time.LocalDate}. */
  DATE,
  /** {@link java.time.LocalDateTime}. */
  TIMESTAMP
}
