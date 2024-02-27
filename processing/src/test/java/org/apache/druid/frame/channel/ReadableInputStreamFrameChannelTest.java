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

package org.apache.druid.frame.channel;

import com.google.common.io.ByteStreams;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReadableInputStreamFrameChannelTest extends InitializedNullHandlingTest
{

  @TempDir
  public File temporaryFolder;

  final IncrementalIndexStorageAdapter adapter =
      new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());

  ExecutorService executorService = Execs.singleThreaded("input-stream-fetcher-test");

  @Test
  public void testSimpleFrameFile()
  {
    InputStream inputStream = getInputStream();
    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
        inputStream,
        "readSimpleFrameFile",
        executorService,
        false
    );

    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromAdapter(adapter, null, false),
        FrameTestUtil.readRowsFromFrameChannel(
            readableInputStreamFrameChannel,
            FrameReader.create(adapter.getRowSignature())
        )
    );
    Assertions.assertTrue(readableInputStreamFrameChannel.isFinished());
    readableInputStreamFrameChannel.close();

  }


  @Test
  public void testEmptyFrameFile() throws IOException
  {
    final File file = FrameTestUtil.writeFrameFile(Sequences.empty(), File.createTempFile("junit", null, temporaryFolder));
    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
        Files.newInputStream(file.toPath()),
        "readEmptyFrameFile",
        executorService,
        false
    );

    Assertions.assertEquals(FrameTestUtil.readRowsFromFrameChannel(
        readableInputStreamFrameChannel,
        FrameReader.create(adapter.getRowSignature())
    ).toList().size(), 0);
    Assertions.assertTrue(readableInputStreamFrameChannel.isFinished());
    readableInputStreamFrameChannel.close();
  }

  @Test
  public void testZeroBytesFrameFile() throws IOException
  {
    final File file = File.createTempFile("junit", null, temporaryFolder);
    FileOutputStream outputStream = new FileOutputStream(file);
    outputStream.write(new byte[0]);
    outputStream.close();

    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
        Files.newInputStream(file.toPath()),
        "testZeroBytesFrameFile",
        executorService,
        false
    );

    final IllegalStateException e = Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            FrameTestUtil.readRowsFromFrameChannel(
                readableInputStreamFrameChannel,
                FrameReader.create(adapter.getRowSignature())
            ).toList()
    );

    assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Incomplete or missing frame at end of stream"))
    );
  }

  @Test
  public void testTruncatedFrameFile() throws IOException
  {
    Throwable exception = assertThrows(ISE.class, () -> {
      final int allocatorSize = 64000;
      final int truncatedSize = 30000; // Holds two full frames + one partial frame, after compression.

      final IncrementalIndexStorageAdapter adapter =
          new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());

      final File file = FrameTestUtil.writeFrameFile(
          FrameSequenceBuilder.fromAdapter(adapter)
              .allocator(ArenaMemoryAllocator.create(ByteBuffer.allocate(allocatorSize)))
              .frameType(FrameType.ROW_BASED)
              .frames(),
          File.createTempFile("junit", null, temporaryFolder)
      );

      final byte[] truncatedFile = new byte[truncatedSize];

      try (final FileInputStream in = new FileInputStream(file)) {
        ByteStreams.readFully(in, truncatedFile);
      }


      ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
          new ByteArrayInputStream(truncatedFile),
          "readTruncatedFrameFile",
          executorService,
          false
      );

      Assertions.assertEquals(FrameTestUtil.readRowsFromFrameChannel(
          readableInputStreamFrameChannel,
          FrameReader.create(adapter.getRowSignature())
      ).toList().size(), 0);
      Assertions.assertTrue(readableInputStreamFrameChannel.isFinished());
      readableInputStreamFrameChannel.close();
    });
    assertTrue(exception.getMessage().contains("Incomplete or missing frame at end of stream"));
  }

  @Test
  public void testIncorrectFrameFile() throws IOException
  {
    Throwable exception = assertThrows(ISE.class, () -> {
      final File file = File.createTempFile("junit", null, temporaryFolder);
      FileOutputStream outputStream = new FileOutputStream(file);
      outputStream.write(10);
      outputStream.close();

      ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
          Files.newInputStream(file.toPath()),
          "readIncorrectFrameFile",
          executorService,
          false
      );

      Assertions.assertEquals(FrameTestUtil.readRowsFromFrameChannel(
          readableInputStreamFrameChannel,
          FrameReader.create(adapter.getRowSignature())
      ).toList().size(), 0);
      Assertions.assertTrue(readableInputStreamFrameChannel.isFinished());
      readableInputStreamFrameChannel.close();

    });
    assertTrue(exception.getMessage().contains("Incomplete or missing frame at end of stream"));

  }


  @Test
  public void closeInputStreamWhileReading() throws IOException
  {
    Throwable exception = assertThrows(ISE.class, () -> {
      InputStream inputStream = getInputStream();
      ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
          inputStream,
          "closeInputStreamWhileReading",
          executorService,
          false
      );
      inputStream.close();
      FrameTestUtil.assertRowsEqual(
          FrameTestUtil.readRowsFromAdapter(adapter, null, false),
          FrameTestUtil.readRowsFromFrameChannel(
              readableInputStreamFrameChannel,
              FrameReader.create(adapter.getRowSignature())
          )
      );
      Assertions.assertTrue(readableInputStreamFrameChannel.isFinished());
      readableInputStreamFrameChannel.close();
    });
    assertTrue(exception.getMessage().contains("Found error while reading input stream"));
  }

  @Test
  public void closeInputStreamWhileReadingCheckError() throws IOException, InterruptedException
  {
    Throwable exception = assertThrows(ISE.class, () -> {
      InputStream inputStream = getInputStream();
      ReadableInputStreamFrameChannel readableInputStreamFrameChannel = ReadableInputStreamFrameChannel.open(
          inputStream,
          "closeInputStreamWhileReadingCheckError",
          executorService,
          false
      );

      inputStream.close();

      while (!readableInputStreamFrameChannel.canRead()) {
        Thread.sleep(10);
      }
      readableInputStreamFrameChannel.read();
      Assertions.assertTrue(readableInputStreamFrameChannel.isFinished());
      readableInputStreamFrameChannel.close();
    });
    assertTrue(exception.getMessage().contains("Found error while reading input stream"));
  }

  private InputStream getInputStream()
  {
    try {
      final File file = FrameTestUtil.writeFrameFile(
          FrameSequenceBuilder.fromAdapter(adapter).maxRowsPerFrame(10).frameType(FrameType.ROW_BASED).frames(),
          File.createTempFile("junit", null, temporaryFolder)
      );
      return Files.newInputStream(file.toPath());
    }
    catch (IOException e) {
      throw new ISE(e, "Unable to create file input stream");
    }
  }
}
