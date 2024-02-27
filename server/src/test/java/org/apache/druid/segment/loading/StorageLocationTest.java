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

package org.apache.druid.segment.loading;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.AlertBuilder;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

/**
 */
public class StorageLocationTest
{
  @TempDir
  public File temporaryFolder;

  @Test
  @SuppressWarnings("GuardedBy")
  public void testStorageLocationFreePercent()
  {
    // free space ignored only maxSize matters
    StorageLocation locationPlain = fakeLocation(100_000, 5_000, 10_000, null);
    Assertions.assertTrue(locationPlain.canHandle(newSegmentId("2012/2013").toString(), 9_000));
    Assertions.assertFalse(locationPlain.canHandle(newSegmentId("2012/2013").toString(), 11_000));

    // enough space available maxSize is the limit
    StorageLocation locationFree = fakeLocation(100_000, 25_000, 10_000, 10.0);
    Assertions.assertTrue(locationFree.canHandle(newSegmentId("2012/2013").toString(), 9_000));
    Assertions.assertFalse(locationFree.canHandle(newSegmentId("2012/2013").toString(), 11_000));

    // disk almost full percentage is the limit
    StorageLocation locationFull = fakeLocation(100_000, 15_000, 10_000, 10.0);
    Assertions.assertTrue(locationFull.canHandle(newSegmentId("2012/2013").toString(), 4_000));
    Assertions.assertFalse(locationFull.canHandle(newSegmentId("2012/2013").toString(), 6_000));
  }

  @Test
  @SuppressWarnings("GuardedBy")
  public void testStorageLocationRealFileSystem() throws IOException
  {
    File file = newFolder(temporaryFolder, "junit");
    StorageLocation location = new StorageLocation(file, 10_000, 100.0d);
    Assertions.assertFalse(location.canHandle(newSegmentId("2012/2013").toString(), 5_000));

    location = new StorageLocation(file, 10_000, 0.0001d);
    Assertions.assertTrue(location.canHandle(newSegmentId("2012/2013").toString(), 1));
  }

  private StorageLocation fakeLocation(long total, long free, long max, Double percent)
  {
    File file = EasyMock.mock(File.class);
    EasyMock.expect(file.getTotalSpace()).andReturn(total).anyTimes();
    EasyMock.expect(file.getFreeSpace()).andReturn(free).anyTimes();
    EasyMock.replay(file);
    return new StorageLocation(file, max, percent);
  }

  @Test
  public void testStorageLocation() throws IOException
  {
    File dir = newFolder(temporaryFolder, "junit");
    long expectedAvail = 1000L;
    StorageLocation loc = new StorageLocation(dir, expectedAvail, null);

    verifyLoc(expectedAvail, loc);

    final DataSegment secondSegment = makeSegment("2012-01-02/2012-01-03", 23);

    loc.reserve("test1", makeSegment("2012-01-01/2012-01-02", 10));
    expectedAvail -= 10;
    verifyLoc(expectedAvail, loc);

    loc.reserve("test1", makeSegment("2012-01-01/2012-01-02", 10));
    verifyLoc(expectedAvail, loc);

    loc.reserve("test2", secondSegment);
    expectedAvail -= 23;
    verifyLoc(expectedAvail, loc);

    loc.removeSegmentDir(new File(dir, "test1"), makeSegment("2012-01-01/2012-01-02", 10));
    expectedAvail += 10;
    verifyLoc(expectedAvail, loc);

    loc.removeSegmentDir(new File(dir, "test1"), makeSegment("2012-01-01/2012-01-02", 10));
    verifyLoc(expectedAvail, loc);

    loc.removeSegmentDir(new File(dir, "test2"), secondSegment);
    expectedAvail += 23;
    verifyLoc(expectedAvail, loc);
  }

  @Test
  public void testMaybeReserve() throws IOException
  {
    ServiceEmitter emitter = Mockito.mock(ServiceEmitter.class);
    ArgumentCaptor<ServiceEventBuilder> argumentCaptor = ArgumentCaptor.forClass(ServiceEventBuilder.class);
    EmittingLogger.registerEmitter(emitter);

    File dir = newFolder(temporaryFolder, "junit");
    long expectedAvail = 1000L;
    StorageLocation loc = new StorageLocation(dir, expectedAvail, null);

    verifyLoc(expectedAvail, loc);

    final DataSegment secondSegment = makeSegment("2012-01-02/2012-01-03", 23);

    loc.maybeReserve("test1", makeSegment("2012-01-01/2012-01-02", 10));
    expectedAvail -= 10;
    verifyLoc(expectedAvail, loc);

    loc.maybeReserve("test1", makeSegment("2012-01-01/2012-01-02", 10));
    verifyLoc(expectedAvail, loc);

    loc.maybeReserve("test2", secondSegment);
    expectedAvail -= 23;
    verifyLoc(expectedAvail, loc);

    loc.removeSegmentDir(new File(dir, "test1"), makeSegment("2012-01-01/2012-01-02", 10));
    expectedAvail += 10;
    verifyLoc(expectedAvail, loc);

    loc.maybeReserve("test3", makeSegment("2012-01-01/2012-01-02", 999));
    expectedAvail -= 999;
    verifyLoc(expectedAvail, loc);

    Mockito.verify(emitter).emit(argumentCaptor.capture());
    AlertBuilder alertBuilder = (AlertBuilder) argumentCaptor.getValue();
    String description = alertBuilder.build(ImmutableMap.of()).getDescription();
    Assertions.assertNotNull(description);
    Assertions.assertTrue(description.contains("Please increase druid.segmentCache.locations maxSize param"), description);
  }

  @Test
  public void testReserveAndRelease() throws IOException
  {
    File dir = newFolder(temporaryFolder, "junit");
    StorageLocation loc = new StorageLocation(dir, 1000L, null);

    File reserved = loc.reserve("testPath", "segmentId", 100L);
    Assertions.assertNotNull(reserved);
    Assertions.assertEquals(new File(dir, "testPath"), reserved.getAbsoluteFile());
    Assertions.assertEquals(900L, loc.availableSizeBytes());
    Assertions.assertTrue(loc.contains("testPath"));

    Assertions.assertNull(loc.reserve("testPath", "segmentId", 100L));

    Assertions.assertTrue(loc.release("testPath", 100L));
    Assertions.assertEquals(1000L, loc.availableSizeBytes());
    Assertions.assertFalse(loc.contains("testPath"));

    Assertions.assertFalse(loc.release("testPath", 100L));
  }

  @SuppressWarnings("GuardedBy")
  private void verifyLoc(long maxSize, StorageLocation loc)
  {
    Assertions.assertEquals(maxSize, loc.availableSizeBytes());
    for (int i = 0; i <= maxSize; ++i) {
      Assertions.assertTrue(loc.canHandle(newSegmentId("2013/2014").toString(), i), String.valueOf(i));
    }
  }

  private DataSegment makeSegment(String intervalString, long size)
  {
    return new DataSegment(
        "test",
        Intervals.of(intervalString),
        "1",
        ImmutableMap.of(),
        Collections.singletonList("d"),
        Collections.singletonList("m"),
        null,
        null,
        size
    );
  }

  private SegmentId newSegmentId(String intervalString)
  {
    return SegmentId.of("test", Intervals.of(intervalString), "1", 0);
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
