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

package org.apache.druid.segment.join;

import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.segment.CloseableShapeshifter;
import org.apache.druid.segment.StorageAdapter;

import java.io.IOException;
import java.util.Collection;

public class XRowsAndColumns implements CloseableShapeshifter, RowsAndColumns
{

  private final StorageAdapter storageAdapter;

  public XRowsAndColumns(StorageAdapter storageAdapter)
  {
    this.storageAdapter = storageAdapter;
  }

  @Override
  public void close() throws IOException
  {
    // FIXME close?
  }

  @Override
  public <T> T as(Class<T> clazz)
  {
    if (StorageAdapter.class == clazz) {
      return (T) storageAdapter;
    }
    return null;

  }

  @Override
  public Collection<String> getColumnNames()
  {
    return storageAdapter.getRowSignature().getColumnNames();
  }

  @Override
  public int numRows()
  {
    return storageAdapter.getNumRows();
  }

  @Override
  public Column findColumn(String name)
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;
  }

}
