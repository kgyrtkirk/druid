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

import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Outcome of one statement: either a row stream or a bare command acknowledgement.
 */
public final class PgResult implements Closeable
{
  private static final String SELECT_TAG = "SELECT";

  private final String commandTag;
  private final List<PgColumn> columns;
  private final CloseableIterator<Object[]> rows;

  private PgResult(final String commandTag, final List<PgColumn> columns, final CloseableIterator<Object[]> rows)
  {
    this.commandTag = commandTag;
    this.columns = columns;
    this.rows = rows;
  }

  /**
   * Row stream; each row holds one cell per column, typed per {@link PgType}. Ownership of {@code rows} moves here.
   */
  public static PgResult rows(final List<PgColumn> columns, final CloseableIterator<Object[]> rows)
  {
    return new PgResult(SELECT_TAG, columns, rows);
  }

  /**
   * Acknowledgement of a statement that produces no rows, e.g. {@code SET}.
   */
  public static PgResult command(final String commandTag)
  {
    return new PgResult(commandTag, List.of(), CloseableIterators.withEmptyBaggage(List.<Object[]>of().iterator()));
  }

  public String getCommandTag()
  {
    return commandTag;
  }

  /**
   * Empty for command results.
   */
  public List<PgColumn> getColumns()
  {
    return columns;
  }

  public CloseableIterator<Object[]> getRows()
  {
    return rows;
  }

  @Override
  public void close()
  {
    try {
      rows.close();
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
