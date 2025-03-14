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

package org.apache.druid.segment.data;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

public class CompressedVariableSizedBlobColumnSupplier implements Supplier<CompressedVariableSizedBlobColumn>
{
  public static final byte VERSION = 0x01;

  public static CompressedVariableSizedBlobColumnSupplier fromByteBuffer(
      String filenameBase,
      ByteBuffer buffer,
      ByteOrder compressionOrder,
      ByteOrder valueOrder,
      SmooshedFileMapper mapper
  ) throws IOException
  {
    return fromByteBuffer(filenameBase, buffer, compressionOrder, valueOrder, false, mapper);
  }

  public static CompressedVariableSizedBlobColumnSupplier fromByteBuffer(
      String filenameBase,
      ByteBuffer buffer,
      ByteOrder compressionOrder,
      ByteOrder valueOrder,
      boolean copyValuesOnRead,
      SmooshedFileMapper mapper
  ) throws IOException
  {
    byte versionFromBuffer = buffer.get();
    if (versionFromBuffer == VERSION) {
      final int numElements = buffer.getInt();
      // offsets and blobs are stored in their own files
      final ByteBuffer offsetsBuffer = mapper.mapFile(
          CompressedVariableSizedBlobColumnSerializer.getCompressedOffsetsFileName(filenameBase)
      );
      final ByteBuffer dataBuffer = mapper.mapFile(
          CompressedVariableSizedBlobColumnSerializer.getCompressedBlobsFileName(filenameBase)
      );
      return new CompressedVariableSizedBlobColumnSupplier(
          offsetsBuffer,
          dataBuffer,
          compressionOrder,
          valueOrder,
          numElements,
          copyValuesOnRead
      );
    }
    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  private final int numElements;

  private final Supplier<CompressedLongsReader> offsetReaderSupplier;
  private final Supplier<CompressedBlockReader> blockDataReaderSupplier;

  private CompressedVariableSizedBlobColumnSupplier(
      ByteBuffer offsetsBuffer,
      ByteBuffer dataBuffer,
      ByteOrder compressionOrder,
      ByteOrder valueOrder,
      int numElements,
      boolean copyValuesOnRead
  )
  {
    this.numElements = numElements;
    this.offsetReaderSupplier = CompressedLongsReader.fromByteBuffer(offsetsBuffer, compressionOrder);
    this.blockDataReaderSupplier = CompressedBlockReader.fromByteBuffer(dataBuffer, compressionOrder, valueOrder, copyValuesOnRead);
  }

  @Override
  public CompressedVariableSizedBlobColumn get()
  {
    return new CompressedVariableSizedBlobColumn(
        numElements,
        offsetReaderSupplier.get(),
        blockDataReaderSupplier.get()
    );
  }
}
