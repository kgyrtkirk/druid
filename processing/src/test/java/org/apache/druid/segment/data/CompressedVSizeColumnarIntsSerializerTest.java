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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.Smoosh;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.WriteOutBytes;
import org.apache.druid.utils.CloseableUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class CompressedVSizeColumnarIntsSerializerTest
{
  private static final int[] MAX_VALUES = new int[]{0xFF, 0xFFFF, 0xFFFFFF, 0x0FFFFFFF};
  private final SegmentWriteOutMedium segmentWriteOutMedium = new OffHeapMemorySegmentWriteOutMedium();
  private CompressionStrategy compressionStrategy;
  private ByteOrder byteOrder;
  private final Random rand = new Random(0);
  private int[] vals;

  @TempDir
  public File temporaryFolder;

  public void initCompressedVSizeColumnarIntsSerializerTest(
      CompressionStrategy compressionStrategy,
      ByteOrder byteOrder
  )
  {
    this.compressionStrategy = compressionStrategy;
    this.byteOrder = byteOrder;
  }

  public static Iterable<Object[]> compressionStrategiesAndByteOrders()
  {
    Set<List<Object>> combinations = Sets.cartesianProduct(
        Sets.newHashSet(CompressionStrategy.noNoneValues()),
        Sets.newHashSet(ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN)
    );

    return Iterables.transform(
        combinations,
        (Function<List, Object[]>) input -> new Object[]{input.get(0), input.get(1)}
    );
  }

  @BeforeEach
  public void setUp()
  {
    vals = null;
  }

  @AfterEach
  public void tearDown() throws Exception
  {
    segmentWriteOutMedium.close();
  }

  private void generateVals(final int totalSize, final int maxValue)
  {
    vals = new int[totalSize];
    for (int i = 0; i < vals.length; ++i) {
      vals[i] = rand.nextInt(maxValue);
    }
  }

  private void checkSerializedSizeAndData(int chunkSize) throws Exception
  {
    FileSmoosher smoosher = new FileSmoosher(newFolder(temporaryFolder, "junit"));
    final String columnName = "test";
    CompressedVSizeColumnarIntsSerializer writer = new CompressedVSizeColumnarIntsSerializer(
        columnName,
        segmentWriteOutMedium,
        "test",
        vals.length > 0 ? Ints.max(vals) : 0,
        chunkSize,
        byteOrder,
        compressionStrategy
    );
    CompressedVSizeColumnarIntsSupplier supplierFromList = CompressedVSizeColumnarIntsSupplier.fromList(
        IntArrayList.wrap(vals),
        vals.length > 0 ? Ints.max(vals) : 0,
        chunkSize,
        byteOrder,
        compressionStrategy,
        segmentWriteOutMedium.getCloser()
    );
    writer.open();
    for (int val : vals) {
      writer.addValue(val);
    }
    long writtenLength = writer.getSerializedSize();
    final WriteOutBytes writeOutBytes = segmentWriteOutMedium.makeWriteOutBytes();
    writer.writeTo(writeOutBytes, smoosher);
    smoosher.close();

    Assertions.assertEquals(writtenLength, supplierFromList.getSerializedSize());

    // read from ByteBuffer and check values
    CompressedVSizeColumnarIntsSupplier supplierFromByteBuffer = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
        ByteBuffer.wrap(IOUtils.toByteArray(writeOutBytes.asInputStream())),
        byteOrder
    );
    ColumnarInts columnarInts = supplierFromByteBuffer.get();
    for (int i = 0; i < vals.length; ++i) {
      Assertions.assertEquals(vals[i], columnarInts.get(i));
    }
    CloseableUtils.closeAndWrapExceptions(columnarInts);
  }

  @MethodSource("compressionStrategiesAndByteOrders")
  @ParameterizedTest(name = "{index}: compression={0}, byteOrder={1}")
  public void testSmallData(CompressionStrategy compressionStrategy, ByteOrder byteOrder) throws Exception
  {
    initCompressedVSizeColumnarIntsSerializerTest(compressionStrategy, byteOrder);
    // less than one chunk
    for (int maxValue : MAX_VALUES) {
      final int maxChunkSize = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);
      generateVals(rand.nextInt(maxChunkSize), maxValue);
      checkSerializedSizeAndData(maxChunkSize);
    }
  }

  @MethodSource("compressionStrategiesAndByteOrders")
  @ParameterizedTest(name = "{index}: compression={0}, byteOrder={1}")
  public void testLargeData(CompressionStrategy compressionStrategy, ByteOrder byteOrder) throws Exception
  {
    initCompressedVSizeColumnarIntsSerializerTest(compressionStrategy, byteOrder);
    // more than one chunk
    for (int maxValue : MAX_VALUES) {
      final int maxChunkSize = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);
      generateVals((rand.nextInt(5) + 5) * maxChunkSize + rand.nextInt(maxChunkSize), maxValue);
      checkSerializedSizeAndData(maxChunkSize);
    }
  }


  // this test takes ~18 minutes to run
  @Disabled
  @MethodSource("compressionStrategiesAndByteOrders")
  @ParameterizedTest(name = "{index}: compression={0}, byteOrder={1}")
  public void testTooManyValues(CompressionStrategy compressionStrategy, ByteOrder byteOrder) throws IOException
  {
    Throwable exception = assertThrows(ColumnCapacityExceededException.class, () -> {
      initCompressedVSizeColumnarIntsSerializerTest(compressionStrategy, byteOrder);
      final int maxValue = 0x0FFFFFFF;
      final int maxChunkSize = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);
      try (
        SegmentWriteOutMedium segmentWriteOutMedium =
              TmpFileSegmentWriteOutMediumFactory.instance().makeSegmentWriteOutMedium(newFolder(temporaryFolder, "junit"))
          ) {
        GenericIndexedWriter genericIndexed = GenericIndexedWriter.ofCompressedByteBuffers(
            segmentWriteOutMedium,
            "test",
            compressionStrategy,
            Long.BYTES * 10000
        );
        CompressedVSizeColumnarIntsSerializer serializer = new CompressedVSizeColumnarIntsSerializer(
            "test",
            segmentWriteOutMedium,
            maxValue,
            maxChunkSize,
            byteOrder,
            compressionStrategy,
            genericIndexed
        );
        serializer.open();

        final long numRows = Integer.MAX_VALUE + 100L;
        for (long i = 0L; i < numRows; i++) {
          serializer.addValue(ThreadLocalRandom.current().nextInt(0, Integer.MAX_VALUE));
        }
      }
    });
    assertTrue(exception.getMessage().contains(ColumnCapacityExceededException.formatMessage("test")));
  }

  @MethodSource("compressionStrategiesAndByteOrders")
  @ParameterizedTest(name = "{index}: compression={0}, byteOrder={1}")
  public void testEmpty(CompressionStrategy compressionStrategy, ByteOrder byteOrder) throws Exception
  {
    initCompressedVSizeColumnarIntsSerializerTest(compressionStrategy, byteOrder);
    vals = new int[0];
    checkSerializedSizeAndData(2);
  }

  private void checkV2SerializedSizeAndData(int chunkSize) throws Exception
  {
    File tmpDirectory = newFolder(temporaryFolder, "junit");
    FileSmoosher smoosher = new FileSmoosher(tmpDirectory);
    final String columnName = "test";
    GenericIndexedWriter genericIndexed = GenericIndexedWriter.ofCompressedByteBuffers(
        segmentWriteOutMedium,
        "test",
        compressionStrategy,
        Long.BYTES * 10000
    );
    CompressedVSizeColumnarIntsSerializer writer = new CompressedVSizeColumnarIntsSerializer(
        columnName,
        segmentWriteOutMedium,
        vals.length > 0 ? Ints.max(vals) : 0,
        chunkSize,
        byteOrder,
        compressionStrategy,
        genericIndexed
    );
    writer.open();
    for (int val : vals) {
      writer.addValue(val);
    }

    final SmooshedWriter channel = smoosher.addWithSmooshedWriter(
        "test",
        writer.getSerializedSize()
    );
    writer.writeTo(channel, smoosher);
    channel.close();
    smoosher.close();

    SmooshedFileMapper mapper = Smoosh.map(tmpDirectory);

    CompressedVSizeColumnarIntsSupplier supplierFromByteBuffer = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
        mapper.mapFile("test"),
        byteOrder
    );

    ColumnarInts columnarInts = supplierFromByteBuffer.get();
    for (int i = 0; i < vals.length; ++i) {
      Assertions.assertEquals(vals[i], columnarInts.get(i));
    }
    CloseableUtils.closeAll(columnarInts, mapper);
  }

  @MethodSource("compressionStrategiesAndByteOrders")
  @ParameterizedTest(name = "{index}: compression={0}, byteOrder={1}")
  public void testMultiValueFileLargeData(CompressionStrategy compressionStrategy, ByteOrder byteOrder) throws Exception
  {
    initCompressedVSizeColumnarIntsSerializerTest(compressionStrategy, byteOrder);
    for (int maxValue : MAX_VALUES) {
      final int maxChunkSize = CompressedVSizeColumnarIntsSupplier.maxIntsInBufferForValue(maxValue);
      generateVals((rand.nextInt(5) + 5) * maxChunkSize + rand.nextInt(maxChunkSize), maxValue);
      checkV2SerializedSizeAndData(maxChunkSize);
    }
  }

  private static File newFolder(File root, String... subDirs) throws IOException {
    String subFolder = String.join("/", subDirs);
    File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }

}
