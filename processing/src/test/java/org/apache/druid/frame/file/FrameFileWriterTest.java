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

import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import static org.hamcrest.MatcherAssert.assertThat;

public class FrameFileWriterTest extends InitializedNullHandlingTest
{
  @TempDir
  public File temporaryFolder;

  @Test
  public void test_abort_afterAllFrames() throws IOException
  {
    final Sequence<Frame> frames = FrameSequenceBuilder.fromAdapter(new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex()))
                                                       .allocator(ArenaMemoryAllocator.createOnHeap(1000000))
                                                       .frameType(FrameType.ROW_BASED)
                                                       .frames();

    final File file = File.createTempFile("junit", null, temporaryFolder);
    final FrameFileWriter fileWriter = FrameFileWriter.open(Files.newByteChannel(
        file.toPath(),
        StandardOpenOption.WRITE
    ), null, ByteTracker.unboundedTracker());

    frames.forEach(frame -> {
      try {
        fileWriter.writeFrame(frame, FrameFileWriter.NO_PARTITION);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    fileWriter.abort();

    final IllegalStateException e = Assertions.assertThrows(IllegalStateException.class, () -> FrameFile.open(file, null));

    assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Corrupt or truncated file?"))
    );
  }
}
