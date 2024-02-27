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

package org.apache.druid.segment;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.timeline.SegmentId;
import org.easymock.EasyMock;
import org.joda.time.Days;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 */
public class ReferenceCountingSegmentTest
{
  private ReferenceCountingSegment segment;
  private ExecutorService exec;

  private final SegmentId segmentId = SegmentId.dummy("test_segment");
  private final Interval dataInterval = new Interval(DateTimes.nowUtc().minus(Days.days(1)), DateTimes.nowUtc());
  private QueryableIndex index;
  private StorageAdapter adapter;
  private IndexedTable indexedTable;
  private int underlyingSegmentClosedCount;

  @BeforeEach
  public void setUp()
  {
    underlyingSegmentClosedCount = 0;
    index = EasyMock.createNiceMock(QueryableIndex.class);
    adapter = EasyMock.createNiceMock(StorageAdapter.class);
    indexedTable = EasyMock.createNiceMock(IndexedTable.class);

    segment = ReferenceCountingSegment.wrapRootGenerationSegment(
        new Segment()
        {
          @Override
          public SegmentId getId()
          {
            return segmentId;
          }

          @Override
          public Interval getDataInterval()
          {
            return dataInterval;
          }

          @Override
          public QueryableIndex asQueryableIndex()
          {
            return index;
          }

          @Override
          public StorageAdapter asStorageAdapter()
          {
            return adapter;
          }

          @Override
          public <T> T as(Class<T> clazz)
          {
            if (clazz.equals(QueryableIndex.class)) {
              return (T) asQueryableIndex();
            } else if (clazz.equals(StorageAdapter.class)) {
              return (T) asStorageAdapter();
            } else if (clazz.equals(IndexedTable.class)) {
              return (T) indexedTable;
            }
            return null;
          }

          @Override
          public void close()
          {
            underlyingSegmentClosedCount++;
          }
        }
    );

    exec = Executors.newSingleThreadExecutor();
  }

  @Test
  public void testMultipleClose() throws Exception
  {
    Assertions.assertEquals(0, underlyingSegmentClosedCount);
    Assertions.assertFalse(segment.isClosed());
    Assertions.assertTrue(segment.increment());
    Assertions.assertEquals(1, segment.getNumReferences());

    Closeable closeable = segment.decrementOnceCloseable();
    Assertions.assertEquals(0, underlyingSegmentClosedCount);
    closeable.close();
    Assertions.assertEquals(0, underlyingSegmentClosedCount);
    closeable.close();
    Assertions.assertEquals(0, underlyingSegmentClosedCount);
    exec.submit(
        () -> {
          try {
            closeable.close();
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
    ).get();
    Assertions.assertEquals(0, segment.getNumReferences());
    Assertions.assertEquals(0, underlyingSegmentClosedCount);
    Assertions.assertFalse(segment.isClosed());

    // close for reals
    segment.close();
    Assertions.assertTrue(segment.isClosed());
    Assertions.assertEquals(1, underlyingSegmentClosedCount);
    // ... but make sure it only happens once
    segment.close();
    Assertions.assertEquals(1, underlyingSegmentClosedCount);
    exec.submit(
        () -> {
          try {
            segment.close();
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
    ).get();

    Assertions.assertEquals(0, segment.getNumReferences());
    Assertions.assertTrue(segment.isClosed());
    Assertions.assertEquals(1, underlyingSegmentClosedCount);

    segment.increment();
    segment.increment();
    segment.increment();
    Assertions.assertEquals(0, segment.getNumReferences());
    Assertions.assertEquals(1, underlyingSegmentClosedCount);
    segment.close();
    Assertions.assertEquals(0, segment.getNumReferences());
    Assertions.assertEquals(1, underlyingSegmentClosedCount);
  }

  @Test
  public void testExposesWrappedSegment()
  {
    Assertions.assertEquals(segmentId, segment.getId());
    Assertions.assertEquals(dataInterval, segment.getDataInterval());
    Assertions.assertEquals(index, segment.asQueryableIndex());
    Assertions.assertEquals(adapter, segment.asStorageAdapter());
  }

  @Test
  public void testSegmentAs()
  {
    Assertions.assertSame(index, segment.as(QueryableIndex.class));
    Assertions.assertSame(adapter, segment.as(StorageAdapter.class));
    Assertions.assertSame(indexedTable, segment.as(IndexedTable.class));
    Assertions.assertNull(segment.as(String.class));
  }

}
