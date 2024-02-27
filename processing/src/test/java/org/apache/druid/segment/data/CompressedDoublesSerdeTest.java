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

import com.google.common.base.Supplier;
import com.google.common.primitives.Doubles;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMedium;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.utils.CloseableUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This is a copy-pasta of {@link CompressedFloatsSerdeTest} without {@link CompressedFloatsSerdeTest#testSupplierSerde}
 * because doubles do not have a supplier serde (e.g. {@link CompressedColumnarFloatsSupplier} or
 * {@link CompressedColumnarLongsSupplier}).
 *
 * It is not important that it remain a copy, the committer is just lazy
 */
public class CompressedDoublesSerdeTest
{
  public static Iterable<Object[]> compressionStrategies()
  {
    List<Object[]> data = new ArrayList<>();
    for (CompressionStrategy strategy : CompressionStrategy.values()) {
      data.add(new Object[]{strategy, ByteOrder.BIG_ENDIAN});
      data.add(new Object[]{strategy, ByteOrder.LITTLE_ENDIAN});
    }
    return data;
  }

  private static final double DELTA = 0.00001;

  @TempDir
  public File temporaryFolder;

  protected CompressionStrategy compressionStrategy;
  protected ByteOrder order;

  private final double[] values0 = {};
  private final double[] values1 = {0, 1, 1, 0, 1, 1, 1, 1, 0, 0, 1, 1};
  private final double[] values2 = {13.2, 6.1, 0.001, 123, 12572, 123.1, 784.4, 6892.8634, 8.341111};
  private final double[] values3 = {0.001, 0.001, 0.001, 0.001, 0.001, 100, 100, 100, 100, 100};
  private final double[] values4 = {0, 0, 0, 0, 0.01, 0, 0, 0, 21.22, 0, 0, 0, 0, 0, 0};
  private final double[] values5 = {123.16, 1.12, 62.00, 462.12, 517.71, 56.54, 971.32, 824.22, 472.12, 625.26};
  private final double[] values6 = {1000000, 1000001, 1000002, 1000003, 1000004, 1000005, 1000006, 1000007, 1000008};
  private final double[] values7 = {
      Double.POSITIVE_INFINITY,
      Double.NEGATIVE_INFINITY,
      12378.5734,
      -12718243.7496,
      -93653653.1,
      12743153.385534,
      21431.414538,
      65487435436632.123,
      -43734526234564.65
  };

  public void initCompressedDoublesSerdeTest(
      CompressionStrategy compressionStrategy,
      ByteOrder order
  )
  {
    this.compressionStrategy = compressionStrategy;
    this.order = order;
  }

  @MethodSource("compressionStrategies")
  @ParameterizedTest(name = "{0} {1} {2}")
  public void testValueSerde(CompressionStrategy compressionStrategy, ByteOrder order) throws Exception
  {
    initCompressedDoublesSerdeTest(compressionStrategy, order);
    testWithValues(values0);
    testWithValues(values1);
    testWithValues(values2);
    testWithValues(values3);
    testWithValues(values4);
    testWithValues(values5);
    testWithValues(values6);
    testWithValues(values7);
  }

  @MethodSource("compressionStrategies")
  @ParameterizedTest(name = "{0} {1} {2}")
  public void testChunkSerde(CompressionStrategy compressionStrategy, ByteOrder order) throws Exception
  {
    initCompressedDoublesSerdeTest(compressionStrategy, order);
    double[] chunk = new double[10000];
    for (int i = 0; i < 10000; i++) {
      chunk[i] = i;
    }
    testWithValues(chunk);
  }

  // this test takes ~45 minutes to run
  @Disabled
  @MethodSource("compressionStrategies")
  @ParameterizedTest(name = "{0} {1} {2}")
  public void testTooManyValues(CompressionStrategy compressionStrategy, ByteOrder order) throws IOException
  {
    Throwable exception = assertThrows(ColumnCapacityExceededException.class, () -> {
      initCompressedDoublesSerdeTest(compressionStrategy, order);
      try (
        SegmentWriteOutMedium segmentWriteOutMedium =
              TmpFileSegmentWriteOutMediumFactory.instance().makeSegmentWriteOutMedium(newFolder(temporaryFolder, "junit"))
          ) {
        ColumnarDoublesSerializer serializer = CompressionFactory.getDoubleSerializer(
            "test",
            segmentWriteOutMedium,
            "test",
            order,
            compressionStrategy
        );
        serializer.open();

        final long numRows = Integer.MAX_VALUE + 100L;
        for (long i = 0L; i < numRows; i++) {
          serializer.add(ThreadLocalRandom.current().nextDouble());
        }
      }
    });
    assertTrue(exception.getMessage().contains(ColumnCapacityExceededException.formatMessage("test")));
  }

  public void testWithValues(double[] values) throws Exception
  {
    ColumnarDoublesSerializer serializer = CompressionFactory.getDoubleSerializer(
        "test",
        new OffHeapMemorySegmentWriteOutMedium(),
        "test",
        order,
        compressionStrategy
    );
    serializer.open();

    for (double value : values) {
      serializer.add(value);
    }
    Assertions.assertEquals(values.length, serializer.size());

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    serializer.writeTo(Channels.newChannel(baos), null);
    Assertions.assertEquals(baos.size(), serializer.getSerializedSize());
    Supplier<ColumnarDoubles> supplier = CompressedColumnarDoublesSuppliers
        .fromByteBuffer(ByteBuffer.wrap(baos.toByteArray()), order);
    try (ColumnarDoubles doubles = supplier.get()) {
      assertIndexMatchesVals(doubles, values);
      for (int i = 0; i < 10; i++) {
        int a = (int) (ThreadLocalRandom.current().nextDouble() * values.length);
        int b = (int) (ThreadLocalRandom.current().nextDouble() * values.length);
        int start = a < b ? a : b;
        int end = a < b ? b : a;
        tryFill(doubles, values, start, end - start);
      }
      testConcurrentThreadReads(supplier, doubles, values);
    }
  }

  private void tryFill(ColumnarDoubles indexed, double[] vals, final int startIndex, final int size)
  {
    double[] filled = new double[size];
    indexed.get(filled, startIndex, filled.length);

    for (int i = startIndex; i < filled.length; i++) {
      Assertions.assertEquals(vals[i + startIndex], filled[i], DELTA);
    }
  }

  private void assertIndexMatchesVals(ColumnarDoubles indexed, double[] vals)
  {
    Assertions.assertEquals(vals.length, indexed.size());

    // sequential access
    int[] indices = new int[vals.length];
    for (int i = 0; i < indexed.size(); ++i) {
      Assertions.assertEquals(vals[i], indexed.get(i), DELTA);
      indices[i] = i;
    }

    // random access, limited to 1000 elements for large lists (every element would take too long)
    IntArrays.shuffle(indices, ThreadLocalRandom.current());
    final int limit = Math.min(indexed.size(), 1000);
    for (int i = 0; i < limit; ++i) {
      int k = indices[i];
      Assertions.assertEquals(vals[k], indexed.get(k), DELTA);
    }
  }

  // This test attempts to cause a race condition with the DirectByteBuffers, it's non-deterministic in causing it,
  // which sucks but I can't think of a way to deterministically cause it...
  private void testConcurrentThreadReads(
      final Supplier<ColumnarDoubles> supplier,
      final ColumnarDoubles indexed,
      final double[] vals
  ) throws Exception
  {
    final AtomicReference<String> reason = new AtomicReference<String>("none");

    final int numRuns = 1000;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch stopLatch = new CountDownLatch(2);
    final AtomicBoolean failureHappened = new AtomicBoolean(false);
    new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        try {
          startLatch.await();
        }
        catch (InterruptedException e) {
          failureHappened.set(true);
          reason.set("interrupt.");
          stopLatch.countDown();
          return;
        }

        try {
          for (int i = 0; i < numRuns; ++i) {
            for (int j = 0; j < indexed.size(); ++j) {
              final double val = vals[j];
              final double indexedVal = indexed.get(j);
              if (Doubles.compare(val, indexedVal) != 0) {
                failureHappened.set(true);
                reason.set(StringUtils.format("Thread1[%d]: %f != %f", j, val, indexedVal));
                stopLatch.countDown();
                return;
              }
            }
          }
        }
        catch (Exception e) {
          e.printStackTrace();
          failureHappened.set(true);
          reason.set(e.getMessage());
        }

        stopLatch.countDown();
      }
    }).start();

    final ColumnarDoubles indexed2 = supplier.get();
    try {
      new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          try {
            startLatch.await();
          }
          catch (InterruptedException e) {
            stopLatch.countDown();
            return;
          }

          try {
            for (int i = 0; i < numRuns; ++i) {
              for (int j = indexed2.size() - 1; j >= 0; --j) {
                final double val = vals[j];
                final double indexedVal = indexed2.get(j);
                if (Doubles.compare(val, indexedVal) != 0) {
                  failureHappened.set(true);
                  reason.set(StringUtils.format("Thread2[%d]: %f != %f", j, val, indexedVal));
                  stopLatch.countDown();
                  return;
                }
              }
            }
          }
          catch (Exception e) {
            e.printStackTrace();
            reason.set(e.getMessage());
            failureHappened.set(true);
          }

          stopLatch.countDown();
        }
      }).start();

      startLatch.countDown();

      stopLatch.await();
    }
    finally {
      CloseableUtils.closeAndWrapExceptions(indexed2);
    }

    if (failureHappened.get()) {
      Assertions.fail("Failure happened.  Reason: " + reason.get());
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
