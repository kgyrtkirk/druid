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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMedium;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class CompressedLongsAutoEncodingSerdeTest
{
  public static Iterable<Object[]> compressionStrategies()
  {
    List<Object[]> data = new ArrayList<>();
    for (long bpv : BITS_PER_VALUE_PARAMETERS) {
      for (CompressionStrategy strategy : CompressionStrategy.values()) {
        data.add(new Object[]{bpv, strategy, ByteOrder.BIG_ENDIAN});
        data.add(new Object[]{bpv, strategy, ByteOrder.LITTLE_ENDIAN});
      }
    }
    return data;
  }

  private static final long[] BITS_PER_VALUE_PARAMETERS = new long[]{1, 2, 4, 7, 11, 14, 18, 23, 31, 39, 46, 55, 62};

  protected final CompressionFactory.LongEncodingStrategy encodingStrategy = CompressionFactory.LongEncodingStrategy.AUTO;
  protected CompressionStrategy compressionStrategy;
  protected ByteOrder order;
  protected long bitsPerValue;

  public void initCompressedLongsAutoEncodingSerdeTest(
      long bitsPerValue,
      CompressionStrategy compressionStrategy,
      ByteOrder order
  )
  {
    this.bitsPerValue = bitsPerValue;
    this.compressionStrategy = compressionStrategy;
    this.order = order;
  }

  @MethodSource("compressionStrategies")
  @ParameterizedTest(name = "{0} {1} {2}")
  public void testFidelity(long bitsPerValue, CompressionStrategy compressionStrategy, ByteOrder order) throws Exception
  {
    initCompressedLongsAutoEncodingSerdeTest(bitsPerValue, compressionStrategy, order);
    final long bound = 1L << bitsPerValue;
    // big enough to have at least 2 blocks, and a handful of sizes offset by 1 from each other
    int blockSize = 1 << 16;
    int numBits = (Long.SIZE - Long.numberOfLeadingZeros(1 << (bitsPerValue - 1)));
    double numValuesPerByte = 8.0 / (double) numBits;

    final ThreadLocalRandom currRand = ThreadLocalRandom.current();
    int numRows = (int) (blockSize * numValuesPerByte) * 2 + currRand.nextInt(1, 101);
    long[] chunk = new long[numRows];
    for (int i = 0; i < numRows; i++) {
      chunk[i] = currRand.nextLong(bound);
    }
    testValues(chunk);

    numRows++;
    chunk = new long[numRows];
    for (int i = 0; i < numRows; i++) {
      chunk[i] = currRand.nextLong(bound);
    }
    testValues(chunk);
  }

  public void testValues(long[] values) throws Exception
  {
    ColumnarLongsSerializer serializer = CompressionFactory.getLongSerializer(
        "test",
        new OffHeapMemorySegmentWriteOutMedium(),
        "test",
        order,
        encodingStrategy,
        compressionStrategy
    );
    serializer.open();

    for (long value : values) {
      serializer.add(value);
    }
    Assertions.assertEquals(values.length, serializer.size());

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    serializer.writeTo(Channels.newChannel(baos), null);
    Assertions.assertEquals(baos.size(), serializer.getSerializedSize());
    CompressedColumnarLongsSupplier supplier =
        CompressedColumnarLongsSupplier.fromByteBuffer(ByteBuffer.wrap(baos.toByteArray()), order);
    ColumnarLongs longs = supplier.get();

    assertIndexMatchesVals(longs, values);
    longs.close();
  }

  private void assertIndexMatchesVals(ColumnarLongs indexed, long[] vals)
  {
    Assertions.assertEquals(vals.length, indexed.size());
    for (int i = 0; i < indexed.size(); ++i) {
      Assertions.assertEquals(
          vals[i],
          indexed.get(i),
          StringUtils.format(
              "Value [%d] at row '%d' does not match [%d]",
              indexed.get(i),
              i,
              vals[i]
          )
      );
    }
  }
}
