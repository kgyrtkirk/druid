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

package org.apache.druid.frame.write;

import org.apache.druid.error.DruidException;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.MemoryAllocatorFactory;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.write.columnar.ColumnarFrameWriterFactory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Outward-facing utility methods for {@link FrameWriterFactory} and {@link FrameWriter} users.
 */
public class FrameWriters
{
  private FrameWriters()
  {
    // No instantiation.
  }

  /**
   * Creates a {@link FrameWriterFactory}.
   *
   * @param frameType        frame type to write
   * @param allocatorFactory supplier of allocators, which ultimately determine frame size. Frames are closed and
   *                         written once the allocator runs out of memory.
   * @param signature        signature of the frames
   * @param sortColumns      sort columns for the frames. If nonempty, {@link FrameSort#sort} is used to sort the
   *                         resulting frames.
   * @param removeNullBytes  whether null bytes should be removed from strings as part of writing to the frame.
   *                         Can only be set to "true" for row-based frame types.
   */
  public static FrameWriterFactory makeFrameWriterFactory(
      final FrameType frameType,
      final MemoryAllocatorFactory allocatorFactory,
      final RowSignature signature,
      final List<KeyColumn> sortColumns,
      final boolean removeNullBytes
  )
  {
    if (frameType.isRowBased()) {
      return new RowBasedFrameWriterFactory(allocatorFactory, frameType, signature, sortColumns, removeNullBytes);
    } else {
      // Columnar.
      if (removeNullBytes) {
        // Defensive exception because user-provided "removeNullBytes" should never make it this far. Calling code
        // should take care to not request columnar writers with removeNullBytes = true.
        throw DruidException.defensive("Cannot use removeNullBytes with frameType[%s]", frameType);
      }

      return new ColumnarFrameWriterFactory(allocatorFactory, signature, sortColumns);
    }
  }

  public static FrameWriterFactory makeColumnBasedFrameWriterFactory(
      final MemoryAllocatorFactory allocatorFactory,
      final RowSignature signature,
      final List<KeyColumn> sortColumns
  )
  {
    return new ColumnarFrameWriterFactory(allocatorFactory, signature, sortColumns);
  }

  /**
   * Returns a copy of "signature" with columns rearranged so the provided sortColumns appear as a prefix.
   * Throws an error if any of the sortColumns are not present in the input signature, or if any of their
   * types are unknown.
   *
   * This is useful because sort columns must appear
   */
  public static RowSignature sortableSignature(
      final RowSignature signature,
      final List<KeyColumn> keyColumns
  )
  {
    final RowSignature.Builder builder = RowSignature.builder();

    for (final KeyColumn columnName : keyColumns) {
      final Optional<ColumnType> columnType = signature.getColumnType(columnName.columnName());
      if (!columnType.isPresent()) {
        throw new IAE("Column [%s] not present in signature", columnName);
      }

      builder.add(columnName.columnName(), columnType.get());
    }

    final Set<String> sortColumnNames =
        keyColumns.stream().map(KeyColumn::columnName).collect(Collectors.toSet());

    for (int i = 0; i < signature.size(); i++) {
      final String columnName = signature.getColumnName(i);
      if (!sortColumnNames.contains(columnName)) {
        builder.add(columnName, signature.getColumnType(i).orElse(null));
      }
    }

    return builder.build();
  }
}
