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

package org.apache.druid.frame;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;



public class FrameTest
{
  // Tests that use good frames built from a standard test file.
  @Nested
  public class GoodFramesTest extends InitializedNullHandlingTest
  {
    private Frame columnarFrame;
    private Frame rowBasedSortedFrame;

    @BeforeEach
    public void setUp()
    {
      final StorageAdapter adapter = new QueryableIndexStorageAdapter(TestIndex.getNoRollupMMappedTestIndex());

      final List<KeyColumn> sortBy = ImmutableList.of(
          new KeyColumn("quality", KeyOrder.DESCENDING),
          new KeyColumn("__time", KeyOrder.ASCENDING)
      );

      columnarFrame = Iterables.getOnlyElement(
          FrameSequenceBuilder
              .fromAdapter(adapter)
              .frameType(FrameType.COLUMNAR)
              .frames()
              .toList()
      );

      rowBasedSortedFrame = Iterables.getOnlyElement(
          FrameSequenceBuilder
              .fromAdapter(adapter)
              .frameType(FrameType.ROW_BASED)
              .sortBy(sortBy)
              .frames()
              .toList()
      );
    }

    @Test
    public void test_numRows()
    {
      Assertions.assertEquals(1209, columnarFrame.numRows());
      Assertions.assertEquals(1209, rowBasedSortedFrame.numRows());
    }

    @Test
    public void test_numRegions()
    {
      Assertions.assertEquals(21, columnarFrame.numRegions());
      Assertions.assertEquals(2, rowBasedSortedFrame.numRegions());
    }

    @Test
    public void test_isPermuted()
    {
      Assertions.assertFalse(columnarFrame.isPermuted());
      Assertions.assertTrue(rowBasedSortedFrame.isPermuted());
    }

    @Test
    public void test_physicalRow_standard()
    {
      for (int i = 0; i < columnarFrame.numRows(); i++) {
        Assertions.assertEquals(i, columnarFrame.physicalRow(i));
      }
    }

    @Test
    public void test_physicalRow_standard_outOfBoundsTooLow()
    {
      Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
        columnarFrame.physicalRow(-1);
      });
      assertTrue(exception.getMessage().contains("Row [-1] out of bounds"));
    }

    @Test
    public void test_physicalRow_standard_outOfBoundsTooHigh()
    {
      Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
        columnarFrame.physicalRow(Ints.checkedCast(columnarFrame.numRows()));
      });
      assertTrue(exception.getMessage().contains("Row [1,209] out of bounds"));
    }

    @Test
    public void test_physicalRow_sorted_outOfBoundsTooLow()
    {
      Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
        rowBasedSortedFrame.physicalRow(-1);
      });
      assertTrue(exception.getMessage().contains("Row [-1] out of bounds"));
    }

    @Test
    public void test_physicalRow_sorted_outOfBoundsTooHigh()
    {
      Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
        rowBasedSortedFrame.physicalRow(Ints.checkedCast(columnarFrame.numRows()));
      });
      assertTrue(exception.getMessage().contains("Row [1,209] out of bounds"));
    }
  }

  // Tests that explore "wrap", "decompress", and "writeTo" with different kinds of backing memory.
  @Nested
  public class WrapAndWriteTest extends InitializedNullHandlingTest
  {
    private static byte[] FRAME_DATA;
    private static byte[] FRAME_DATA_COMPRESSED;

    enum MemType
    {
      ARRAY {
        @Override
        Frame wrap(Closer closer)
        {
          return Frame.wrap(FRAME_DATA);
        }

        @Override
        Frame decompress(Closer closer)
        {
          return Frame.decompress(Memory.wrap(FRAME_DATA_COMPRESSED), 0, FRAME_DATA_COMPRESSED.length);
        }
      },
      BB_HEAP {
        @Override
        Frame wrap(Closer closer)
        {
          return Frame.wrap(ByteBuffer.wrap(FRAME_DATA));
        }

        @Override
        Frame decompress(Closer closer)
        {
          return Frame.decompress(
              Memory.wrap(ByteBuffer.wrap(FRAME_DATA_COMPRESSED), ByteOrder.LITTLE_ENDIAN),
              0,
              FRAME_DATA_COMPRESSED.length
          );
        }
      },
      BB_DIRECT {
        @Override
        Frame wrap(Closer closer)
        {
          final ByteBuffer buf = ByteBuffer.allocateDirect(FRAME_DATA.length);
          buf.put(FRAME_DATA, 0, FRAME_DATA.length);
          closer.register(() -> ByteBufferUtils.free(buf));
          return Frame.wrap(buf);
        }

        @Override
        Frame decompress(Closer closer)
        {
          final ByteBuffer buf = ByteBuffer.allocateDirect(FRAME_DATA_COMPRESSED.length);
          buf.put(FRAME_DATA_COMPRESSED, 0, FRAME_DATA_COMPRESSED.length);
          closer.register(() -> ByteBufferUtils.free(buf));
          return Frame.decompress(Memory.wrap(buf, ByteOrder.LITTLE_ENDIAN), 0, FRAME_DATA_COMPRESSED.length);
        }
      },
      BB_MAPPED {
        @Override
        Frame wrap(Closer closer) throws IOException
        {
          final File file = File.createTempFile("frame-test", "");
          closer.register(file::delete);
          Files.write(FRAME_DATA, file);
          final MappedByteBuffer buf = Files.map(file);
          closer.register(() -> ByteBufferUtils.unmap(buf));
          return Frame.wrap(buf);
        }

        @Override
        Frame decompress(Closer closer) throws IOException
        {
          final File file = File.createTempFile("frame-test", "");
          closer.register(file::delete);
          Files.write(FRAME_DATA_COMPRESSED, file);
          final MappedByteBuffer buf = Files.map(file);
          closer.register(() -> ByteBufferUtils.unmap(buf));
          return Frame.decompress(Memory.wrap(buf, ByteOrder.LITTLE_ENDIAN), 0, buf.capacity());
        }
      },
      MEMORY_ARRAY {
        @Override
        Frame wrap(Closer closer)
        {
          return Frame.wrap(Memory.wrap(FRAME_DATA, ByteOrder.LITTLE_ENDIAN));
        }

        @Override
        Frame decompress(Closer closer)
        {
          // Offset by 1 to make sure we handle memory regions properly.
          final byte[] copyFrameData = new byte[FRAME_DATA_COMPRESSED.length + 2];
          System.arraycopy(FRAME_DATA_COMPRESSED, 0, copyFrameData, 1, FRAME_DATA_COMPRESSED.length);
          return Frame.decompress(
              Memory.wrap(copyFrameData, ByteOrder.LITTLE_ENDIAN),
              1,
              FRAME_DATA_COMPRESSED.length
          );
        }
      },
      MEMORY_BB {
        @Override
        Frame wrap(Closer closer)
        {
          return Frame.wrap(Memory.wrap(ByteBuffer.wrap(FRAME_DATA), ByteOrder.LITTLE_ENDIAN));
        }

        @Override
        Frame decompress(Closer closer)
        {
          // Offset by 1 to make sure we handle memory regions properly.
          final byte[] copyFrameData = new byte[FRAME_DATA_COMPRESSED.length + 2];
          System.arraycopy(FRAME_DATA_COMPRESSED, 0, copyFrameData, 1, FRAME_DATA_COMPRESSED.length);
          return Frame.decompress(
              Memory.wrap(
                  ByteBuffer.wrap(copyFrameData),
                  ByteOrder.LITTLE_ENDIAN
              ),
              1,
              FRAME_DATA_COMPRESSED.length
          );
        }
      };

      abstract Frame wrap(Closer closer) throws IOException;

      abstract Frame decompress(Closer closer) throws IOException;
    }

    private MemType memType;
    private boolean compressed;
    private final Closer closer = Closer.create();

    public void initWrapAndWriteTest(final MemType memType, final boolean compressed)
    {
      this.memType = memType;
      this.compressed = compressed;
    }

    public static Iterable<Object[]> constructorFeeder()
    {
      final List<Object[]> constructors = new ArrayList<>();

      for (MemType memType : MemType.values()) {
        for (boolean compressed : new boolean[]{true, false}) {
          constructors.add(new Object[]{memType, compressed});
        }
      }

      return constructors;
    }

    @BeforeAll
    public static void setUpClass() throws Exception
    {
      final StorageAdapter adapter = new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());
      final Frame frame =
          Iterables.getOnlyElement(FrameSequenceBuilder.fromAdapter(adapter)
                                                       .frameType(FrameType.COLUMNAR)
                                                       .frames()
                                                       .toList());
      FRAME_DATA = frameToByteArray(frame, false);
      FRAME_DATA_COMPRESSED = frameToByteArray(frame, true);
    }

    @AfterAll
    public static void tearDownClass()
    {
      FRAME_DATA = null;
      FRAME_DATA_COMPRESSED = null;
    }

    @AfterEach
    public void tearDown() throws IOException
    {
      closer.close();
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "memType = {0}, compressed = {1}")
    public void testWrapAndWrite(final MemType memType, final boolean compressed) throws Exception
    {
      initWrapAndWriteTest(memType, compressed);
      final Frame frame = compressed ? memType.decompress(closer) : memType.wrap(closer);

      // And write.
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      frame.writeTo(
          Channels.newChannel(baos),
          compressed,
          ByteBuffer.allocate(Frame.compressionBufferSize((int) frame.numBytes())),
          ByteTracker.unboundedTracker()
      );

      if (!compressed) {
        Assertions.assertArrayEquals(FRAME_DATA, baos.toByteArray());
      } else {
        // Decompress and check.
        final byte[] compressedData = baos.toByteArray();
        final Frame frame2 = Frame.decompress(Memory.wrap(baos.toByteArray()), 0, compressedData.length);
        Assertions.assertArrayEquals(FRAME_DATA, frameToByteArray(frame2, false));
      }
    }
  }

  // Tests that use bad frames.
  @Nested
  public class BadFramesTest extends InitializedNullHandlingTest
  {
    @Test
    public void testGoodFrameIsActuallyGood() throws Exception
    {
      // Can't take anything for granted.
      final Frame frame = makeGoodFrame();
      final Memory compressedFrameMemory = Memory.wrap(frameToByteArray(frame, true));

      Assertions.assertEquals(
          frame.writableMemory(),
          Frame.decompress(compressedFrameMemory, 0, compressedFrameMemory.getCapacity()).writableMemory()
      );
    }

    @Test
    public void testBadChecksum() throws Exception
    {
      final Frame frame = makeGoodFrame();
      final WritableMemory compressedFrameMemory = WritableMemory.writableWrap(frameToByteArray(frame, true));

      // Tweak a byte.
      compressedFrameMemory.putByte(100L, (byte) 0);

      final IllegalStateException e = Assertions.assertThrows(
          IllegalStateException.class,
          () -> Frame.decompress(compressedFrameMemory, 0, compressedFrameMemory.getCapacity())
      );

      assertThat(e, ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Checksum mismatch")));
    }

    private static Frame makeGoodFrame()
    {
      final StorageAdapter adapter = new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());
      return Iterables.getOnlyElement(FrameSequenceBuilder.fromAdapter(adapter)
                                                          .frameType(FrameType.COLUMNAR)
                                                          .frames()
                                                          .toList());
    }
  }

  private static byte[] frameToByteArray(final Frame frame, final boolean compressed) throws Exception
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    frame.writeTo(
        Channels.newChannel(baos),
        compressed,
        ByteBuffer.allocate(Frame.compressionBufferSize((int) frame.numBytes())),
        ByteTracker.unboundedTracker()
    );
    return baos.toByteArray();
  }
}
