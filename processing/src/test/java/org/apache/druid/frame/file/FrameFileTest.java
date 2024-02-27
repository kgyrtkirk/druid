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

package org.apache.druid.frame.file;

import com.google.common.math.IntMath;
import it.unimi.dsi.fastutil.ints.IntObjectPair;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.TestArrayStorageAdapter;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameStorageAdapter;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.Matchers;
import org.hamcrest.junit.MatcherAssume;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FrameFileTest extends InitializedNullHandlingTest
{
  // Partition every 99 rows if "partitioned" is true.
  private static final int PARTITION_SIZE = 99;

  // Skip unlucky partition #13.
  private static final int SKIP_PARTITION = 13;

  enum AdapterType
  {
    INCREMENTAL {
      @Override
      StorageAdapter getAdapter()
      {
        return new IncrementalIndexStorageAdapter(TestIndex.getNoRollupIncrementalTestIndex());
      }
    },
    MMAP {
      @Override
      StorageAdapter getAdapter()
      {
        return new QueryableIndexStorageAdapter(TestIndex.getNoRollupMMappedTestIndex());
      }
    },
    MV_AS_STRING_ARRAYS {
      @Override
      StorageAdapter getAdapter()
      {
        return new TestArrayStorageAdapter(TestIndex.getNoRollupMMappedTestIndex());
      }
    },
    EMPTY {
      @Override
      StorageAdapter getAdapter()
      {
        return new RowBasedSegment<>(
            SegmentId.dummy("EMPTY"),
            Sequences.empty(),
            RowAdapters.standardRow(),
            RowSignature.empty()
        ).asStorageAdapter();
      }
    };

    abstract StorageAdapter getAdapter();
  }

  @TempDir
  public File temporaryFolder;

  private FrameType frameType;
  private int maxRowsPerFrame;
  private boolean partitioned;
  private AdapterType adapterType;
  private int maxMmapSize;

  private StorageAdapter adapter;
  private File file;

  public void initFrameFileTest(
      final FrameType frameType,
      final int maxRowsPerFrame,
      final boolean partitioned,
      final AdapterType adapterType,
      final int maxMmapSize
  )
  {
    this.frameType = frameType;
    this.maxRowsPerFrame = maxRowsPerFrame;
    this.partitioned = partitioned;
    this.adapterType = adapterType;
    this.maxMmapSize = maxMmapSize;
  }

  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    for (FrameType frameType : FrameType.values()) {
      for (int maxRowsPerFrame : new int[]{1, 17, 50, PARTITION_SIZE, Integer.MAX_VALUE}) {
        for (boolean partitioned : new boolean[]{true, false}) {
          for (AdapterType adapterType : AdapterType.values()) {
            final int[] maxMmapSizes;

            if (maxRowsPerFrame == 1) {
              maxMmapSizes = new int[]{1_000, 10_000, Integer.MAX_VALUE};
            } else {
              maxMmapSizes = new int[]{Integer.MAX_VALUE};
            }

            for (int maxMmapSize : maxMmapSizes) {
              constructors.add(new Object[]{frameType, maxRowsPerFrame, partitioned, adapterType, maxMmapSize});
            }
          }
        }
      }
    }

    return constructors;
  }

  @BeforeEach
  public void setUp() throws IOException
  {
    adapter = adapterType.getAdapter();

    if (partitioned) {
      // Partition every PARTITION_SIZE rows.
      file = FrameTestUtil.writeFrameFileWithPartitions(
          FrameSequenceBuilder.fromAdapter(adapter).frameType(frameType).maxRowsPerFrame(maxRowsPerFrame).frames().map(
              new Function<Frame, IntObjectPair<Frame>>()
              {
                private int rows = 0;

                @Override
                public IntObjectPair<Frame> apply(final Frame frame)
                {
                  final int partitionNum = rows / PARTITION_SIZE;
                  rows += frame.numRows();
                  return IntObjectPair.of(
                      partitionNum >= SKIP_PARTITION ? partitionNum + 1 : partitionNum,
                      frame
                  );
                }
              }
          ),
          File.createTempFile("junit", null, temporaryFolder)
      );

    } else {
      file = FrameTestUtil.writeFrameFile(
          FrameSequenceBuilder.fromAdapter(adapter).frameType(frameType).maxRowsPerFrame(maxRowsPerFrame).frames(),
          File.createTempFile("junit", null, temporaryFolder)
      );
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "frameType = {0}, "
      + "maxRowsPerFrame = {1}, "
      + "partitioned = {2}, "
      + "adapter = {3}, "
      + "maxMmapSize = {4}")
  public void test_numFrames(final FrameType frameType, final int maxRowsPerFrame, final boolean partitioned, final AdapterType adapterType, final int maxMmapSize) throws IOException
  {
    initFrameFileTest(frameType, maxRowsPerFrame, partitioned, adapterType, maxMmapSize);
    try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
      Assertions.assertEquals(computeExpectedNumFrames(), frameFile.numFrames());
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "frameType = {0}, "
      + "maxRowsPerFrame = {1}, "
      + "partitioned = {2}, "
      + "adapter = {3}, "
      + "maxMmapSize = {4}")
  public void test_numPartitions(final FrameType frameType, final int maxRowsPerFrame, final boolean partitioned, final AdapterType adapterType, final int maxMmapSize) throws IOException
  {
    initFrameFileTest(frameType, maxRowsPerFrame, partitioned, adapterType, maxMmapSize);
    try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
      Assertions.assertEquals(computeExpectedNumPartitions(), frameFile.numPartitions());
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "frameType = {0}, "
      + "maxRowsPerFrame = {1}, "
      + "partitioned = {2}, "
      + "adapter = {3}, "
      + "maxMmapSize = {4}")
  public void test_frame_first(final FrameType frameType, final int maxRowsPerFrame, final boolean partitioned, final AdapterType adapterType, final int maxMmapSize) throws IOException
  {
    initFrameFileTest(frameType, maxRowsPerFrame, partitioned, adapterType, maxMmapSize);
    try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
      // Skip test for empty files.
      MatcherAssume.assumeThat(frameFile.numFrames(), Matchers.greaterThan(0));

      final Frame firstFrame = frameFile.frame(0);
      Assertions.assertEquals(Math.min(adapter.getNumRows(), maxRowsPerFrame), firstFrame.numRows());
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "frameType = {0}, "
      + "maxRowsPerFrame = {1}, "
      + "partitioned = {2}, "
      + "adapter = {3}, "
      + "maxMmapSize = {4}")
  public void test_frame_last(final FrameType frameType, final int maxRowsPerFrame, final boolean partitioned, final AdapterType adapterType, final int maxMmapSize) throws IOException
  {
    initFrameFileTest(frameType, maxRowsPerFrame, partitioned, adapterType, maxMmapSize);
    try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
      // Skip test for empty files.
      MatcherAssume.assumeThat(frameFile.numFrames(), Matchers.greaterThan(0));

      final Frame lastFrame = frameFile.frame(frameFile.numFrames() - 1);
      Assertions.assertEquals(
          adapter.getNumRows() % maxRowsPerFrame != 0
          ? adapter.getNumRows() % maxRowsPerFrame
          : Math.min(adapter.getNumRows(), maxRowsPerFrame),
          lastFrame.numRows()
      );
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "frameType = {0}, "
      + "maxRowsPerFrame = {1}, "
      + "partitioned = {2}, "
      + "adapter = {3}, "
      + "maxMmapSize = {4}")
  public void test_frame_outOfBoundsNegative(final FrameType frameType, final int maxRowsPerFrame, final boolean partitioned, final AdapterType adapterType, final int maxMmapSize) throws IOException
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      initFrameFileTest(frameType, maxRowsPerFrame, partitioned, adapterType, maxMmapSize);
      try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Frame [-1] out of bounds");
        frameFile.frame(-1);
      }
    });
    assertTrue(exception.getMessage().contains("Frame [-1] out of bounds"));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "frameType = {0}, "
      + "maxRowsPerFrame = {1}, "
      + "partitioned = {2}, "
      + "adapter = {3}, "
      + "maxMmapSize = {4}")
  public void test_frame_outOfBoundsTooLarge(final FrameType frameType, final int maxRowsPerFrame, final boolean partitioned, final AdapterType adapterType, final int maxMmapSize) throws IOException
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      initFrameFileTest(frameType, maxRowsPerFrame, partitioned, adapterType, maxMmapSize);
      try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(StringUtils.format("Frame [%,d] out of bounds", frameFile.numFrames()));
        frameFile.frame(frameFile.numFrames());
      }
    });
    assertTrue(exception.getMessage().contains(StringUtils.format("Frame [%,d] out of bounds", frameFile.numFrames())));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "frameType = {0}, "
      + "maxRowsPerFrame = {1}, "
      + "partitioned = {2}, "
      + "adapter = {3}, "
      + "maxMmapSize = {4}")
  public void test_frame_readAllDataViaStorageAdapter(final FrameType frameType, final int maxRowsPerFrame, final boolean partitioned, final AdapterType adapterType, final int maxMmapSize) throws IOException
  {
    initFrameFileTest(frameType, maxRowsPerFrame, partitioned, adapterType, maxMmapSize);
    final FrameReader frameReader = FrameReader.create(adapter.getRowSignature());

    try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
      final Sequence<List<Object>> frameFileRows = Sequences.concat(
          () -> IntStream.range(0, frameFile.numFrames())
                         .mapToObj(frameFile::frame)
                         .map(frame -> new FrameStorageAdapter(frame, frameReader, Intervals.ETERNITY))
                         .map(adapter -> FrameTestUtil.readRowsFromAdapter(adapter, null, true))
                         .iterator()
      );

      final Sequence<List<Object>> adapterRows = FrameTestUtil.readRowsFromAdapter(adapter, null, true);
      FrameTestUtil.assertRowsEqual(adapterRows, frameFileRows);
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "frameType = {0}, "
      + "maxRowsPerFrame = {1}, "
      + "partitioned = {2}, "
      + "adapter = {3}, "
      + "maxMmapSize = {4}")
  public void test_getPartitionStartFrame(final FrameType frameType, final int maxRowsPerFrame, final boolean partitioned, final AdapterType adapterType, final int maxMmapSize) throws IOException
  {
    initFrameFileTest(frameType, maxRowsPerFrame, partitioned, adapterType, maxMmapSize);
    try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
      if (partitioned) {
        for (int partitionNum = 0; partitionNum < frameFile.numPartitions(); partitionNum++) {
          Assertions.assertEquals(
              Math.min(
                  IntMath.divide(
                      (partitionNum >= SKIP_PARTITION ? partitionNum + 1 : partitionNum) * PARTITION_SIZE,
                      maxRowsPerFrame,
                      RoundingMode.CEILING
                  ),
                  frameFile.numFrames()
              ),
              frameFile.getPartitionStartFrame(partitionNum),
              "partition #" + partitionNum
          );
        }
      } else {
        Assertions.assertEquals(frameFile.numFrames(), frameFile.getPartitionStartFrame(0));
      }
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "frameType = {0}, "
      + "maxRowsPerFrame = {1}, "
      + "partitioned = {2}, "
      + "adapter = {3}, "
      + "maxMmapSize = {4}")
  public void test_file(final FrameType frameType, final int maxRowsPerFrame, final boolean partitioned, final AdapterType adapterType, final int maxMmapSize) throws IOException
  {
    initFrameFileTest(frameType, maxRowsPerFrame, partitioned, adapterType, maxMmapSize);
    try (final FrameFile frameFile = FrameFile.open(file, maxMmapSize, null)) {
      Assertions.assertEquals(file, frameFile.file());
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "frameType = {0}, "
      + "maxRowsPerFrame = {1}, "
      + "partitioned = {2}, "
      + "adapter = {3}, "
      + "maxMmapSize = {4}")
  public void test_open_withDeleteOnClose(final FrameType frameType, final int maxRowsPerFrame, final boolean partitioned, final AdapterType adapterType, final int maxMmapSize) throws IOException
  {
    initFrameFileTest(frameType, maxRowsPerFrame, partitioned, adapterType, maxMmapSize);
    FrameFile.open(file, maxMmapSize, null).close();
    Assertions.assertTrue(file.exists());

    FrameFile.open(file, null, FrameFile.Flag.DELETE_ON_CLOSE).close();
    Assertions.assertFalse(file.exists());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "frameType = {0}, "
      + "maxRowsPerFrame = {1}, "
      + "partitioned = {2}, "
      + "adapter = {3}, "
      + "maxMmapSize = {4}")
  public void test_newReference(final FrameType frameType, final int maxRowsPerFrame, final boolean partitioned, final AdapterType adapterType, final int maxMmapSize) throws IOException
  {
    Throwable exception = assertThrows(IllegalStateException.class, () -> {
      initFrameFileTest(frameType, maxRowsPerFrame, partitioned, adapterType, maxMmapSize);
      final FrameFile frameFile1 = FrameFile.open(file, null, FrameFile.Flag.DELETE_ON_CLOSE);
      final FrameFile frameFile2 = frameFile1.newReference();
      final FrameFile frameFile3 = frameFile2.newReference();

      // Closing original file does nothing; must wait for other files to be closed.
      frameFile1.close();
      Assertions.assertTrue(file.exists());

      // Can still get a reference after frameFile1 is closed, just because others are still open. Strange but true.
      final FrameFile frameFile4 = frameFile1.newReference();

      // Repeated calls to "close" are deduped.
      frameFile2.close();
      frameFile2.close();
      frameFile2.close();
      frameFile2.close();
      frameFile2.close();
      frameFile2.close();
      Assertions.assertTrue(file.exists());

      frameFile3.close();
      Assertions.assertTrue(file.exists());

      // Final reference is closed; file is now gone.
      frameFile4.close();
      Assertions.assertFalse(file.exists());
      frameFile1.newReference();
    });
    assertTrue(exception.getMessage().contains("Frame file is closed"));
  }

  private int computeExpectedNumFrames()
  {
    return IntMath.divide(countRows(adapter), maxRowsPerFrame, RoundingMode.CEILING);
  }

  private int computeExpectedNumPartitions()
  {
    if (partitioned) {
      return Math.min(
          computeExpectedNumFrames(),
          IntMath.divide(countRows(adapter), PARTITION_SIZE, RoundingMode.CEILING)
      );
    } else {
      // 0 = not partitioned.
      return 0;
    }
  }

  private static int countRows(final StorageAdapter adapter)
  {
    // Not using adapter.getNumRows(), because RowBasedStorageAdapter doesn't support it.
    return FrameTestUtil.readRowsFromAdapter(adapter, RowSignature.empty(), false)
                        .accumulate(0, (i, in) -> i + 1);
  }
}
