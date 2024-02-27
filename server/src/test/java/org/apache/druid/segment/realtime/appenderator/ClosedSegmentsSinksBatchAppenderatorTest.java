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

package org.apache.druid.segment.realtime.appenderator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.chrono.ISOChronology;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClosedSegmentsSinksBatchAppenderatorTest extends InitializedNullHandlingTest
{
  private static final List<SegmentIdWithShardSpec> IDENTIFIERS = ImmutableList.of(
      createSegmentId("2000/2001", "A", 0), // should be in seg_0
      createSegmentId("2000/2001", "A", 1), // seg_1
      createSegmentId("2001/2002", "A", 0) // seg 2
  );

  @Test
  public void testSimpleIngestion() throws Exception
  {
    try (final ClosedSegmensSinksBatchAppenderatorTester tester = new ClosedSegmensSinksBatchAppenderatorTester(3, true)) {
      final Appenderator appenderator = tester.getAppenderator();

      // startJob
      Assertions.assertNull(appenderator.startJob());

      // getDataSource
      Assertions.assertEquals(ClosedSegmensSinksBatchAppenderatorTester.DATASOURCE, appenderator.getDataSource());

      // add #1
      Assertions.assertEquals(
          1,
          appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo", 1), null)
                      .getNumRowsInSegment()
      );

      // add #2
      Assertions.assertEquals(
          1,
          appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bar", 2), null)
                      .getNumRowsInSegment()
      );

      // getSegments
      Assertions.assertEquals(
          IDENTIFIERS.subList(0, 2),
          appenderator.getSegments().stream().sorted().collect(Collectors.toList())
      );

      // add #3, this hits max rows in memory:
      Assertions.assertEquals(
          2,
          appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "sux", 1), null)
                      .getNumRowsInSegment()
      );

      // since we just added three rows and the max rows in memory is three, all the segments (sinks etc.)
      // above should be cleared now
      Assertions.assertEquals(
          Collections.emptyList(),
          ((BatchAppenderator) appenderator).getInMemorySegments().stream().sorted().collect(Collectors.toList())
      );

      // add #4, this will add one more temporary segment:
      Assertions.assertEquals(
          1,
          appenderator.add(IDENTIFIERS.get(2), createInputRow("2001", "qux", 4), null)
                      .getNumRowsInSegment()
      );

      // push all
      final SegmentsAndCommitMetadata segmentsAndCommitMetadata = appenderator.push(
          appenderator.getSegments(),
          null,
          false
      ).get();
      Assertions.assertEquals(
          IDENTIFIERS.subList(0, 3),
          Lists.transform(
              segmentsAndCommitMetadata.getSegments(),
              SegmentIdWithShardSpec::fromDataSegment
          ).stream().sorted().collect(Collectors.toList())
      );
      Assertions.assertEquals(
          tester.getPushedSegments().stream().sorted().collect(Collectors.toList()),
          segmentsAndCommitMetadata.getSegments().stream().sorted().collect(Collectors.toList())
      );

      appenderator.close();
      Assertions.assertTrue(appenderator.getSegments().isEmpty());
    }
  }

  /**
   * Test the case when a segment identifier contains non UTC timestamps in its interval. This can happen
   * when a custom segment granularity for an interval with a non UTC Chronlogy is created by
   * {@link org.apache.druid.java.util.common.granularity.PeriodGranularity#bucketStart(DateTime)}
   */
  @Test
  public void testPeriodGranularityNonUTCIngestion() throws Exception
  {
    try (final ClosedSegmensSinksBatchAppenderatorTester tester = new ClosedSegmensSinksBatchAppenderatorTester(1, true)) {
      final Appenderator appenderator = tester.getAppenderator();

      // startJob
      Assertions.assertNull(appenderator.startJob());

      // getDataSource
      Assertions.assertEquals(ClosedSegmensSinksBatchAppenderatorTester.DATASOURCE, appenderator.getDataSource());

      // Create a segment identifier with a non-utc interval
      SegmentIdWithShardSpec segmentIdWithNonUTCTime =
          createNonUTCSegmentId("2021-06-27T00:00:00.000+09:00/2021-06-28T00:00:00.000+09:00",
                          "A", 0); // should be in seg_0

      Assertions.assertEquals(
          1,
          appenderator.add(segmentIdWithNonUTCTime, createInputRow("2021-06-27T00:01:11.080Z", "foo", 1), null)
                      .getNumRowsInSegment()
      );

      // getSegments
      Assertions.assertEquals(
          Collections.singletonList(segmentIdWithNonUTCTime),
          appenderator.getSegments().stream().sorted().collect(Collectors.toList())
      );


      // since we just added one row and the max rows in memory is one, all the segments (sinks etc)
      // above should be cleared now
      Assertions.assertEquals(
          Collections.emptyList(),
          ((BatchAppenderator) appenderator).getInMemorySegments().stream().sorted().collect(Collectors.toList())
      );


      // push all
      final SegmentsAndCommitMetadata segmentsAndCommitMetadata = appenderator.push(
          appenderator.getSegments(),
          null,
          false
      ).get();
      Assertions.assertEquals(
          Collections.singletonList(segmentIdWithNonUTCTime),
          Lists.transform(
              segmentsAndCommitMetadata.getSegments(),
              SegmentIdWithShardSpec::fromDataSegment
          ).stream().sorted().collect(Collectors.toList())
      );
      Assertions.assertEquals(
          tester.getPushedSegments().stream().sorted().collect(Collectors.toList()),
          segmentsAndCommitMetadata.getSegments().stream().sorted().collect(Collectors.toList())
      );

      appenderator.close();
      Assertions.assertTrue(appenderator.getSegments().isEmpty());
    }
  }

  @Test
  public void testMaxBytesInMemoryWithSkipBytesInMemoryOverheadCheckConfig() throws Exception
  {
    try (
        final ClosedSegmensSinksBatchAppenderatorTester tester = new ClosedSegmensSinksBatchAppenderatorTester(
            100,
            1024,
            null,
            true,
            new SimpleRowIngestionMeters(),
            true
        )
    ) {
      final Appenderator appenderator = tester.getAppenderator();

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo", 1), null);
      //expectedSizeInBytes =
      // 44(map overhead) + 28 (TimeAndDims overhead) + 56 (aggregator metrics) + 54 (dimsKeySize) =
      // 182 + 1 byte when null handling is enabled
      int nullHandlingOverhead = NullHandling.sqlCompatible() ? 1 : 0;
      Assertions.assertEquals(
          182 + nullHandlingOverhead,
          ((BatchAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bar", 1), null);
      Assertions.assertEquals(
          182 + nullHandlingOverhead,
          ((BatchAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(1))
      );
      appenderator.close();
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
    }
  }

  @Test
  public void testMaxBytesInMemoryInMultipleSinksWithSkipBytesInMemoryOverheadCheckConfig() throws Exception
  {
    try (
        final ClosedSegmensSinksBatchAppenderatorTester tester = new ClosedSegmensSinksBatchAppenderatorTester(
            100,
            1024,
            null,
            true,
            new SimpleRowIngestionMeters(),
            true
        )
    ) {
      final Appenderator appenderator = tester.getAppenderator();

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo", 1), null);
      //expectedSizeInBytes = 44(map overhead) + 28 (TimeAndDims overhead) + 56 (aggregator metrics) + 54 (dimsKeySize) = 182
      int nullHandlingOverhead = NullHandling.sqlCompatible() ? 1 : 0;
      Assertions.assertEquals(182 + nullHandlingOverhead, ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory());
      appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bar", 1), null);
      Assertions.assertEquals(
          364 + 2 * nullHandlingOverhead,
          ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory()
      );
      Assertions.assertEquals(2, appenderator.getSegments().size());
      appenderator.close();
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
    }
  }

  @Test
  public void testMaxBytesInMemory() throws Exception
  {
    try (final ClosedSegmensSinksBatchAppenderatorTester tester = new ClosedSegmensSinksBatchAppenderatorTester(100, 15000, true)) {
      final Appenderator appenderator = tester.getAppenderator();

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo", 1), null);
      // Still under maxSizeInBytes after the add. Hence, we do not persist yet
      //expectedSizeInBytes = 44(map overhead) + 28 (TimeAndDims overhead) + 56 (aggregator metrics) + 54 (dimsKeySize) = 182 + 1 byte when null handling is enabled
      int nullHandlingOverhead = NullHandling.sqlCompatible() ? 1 : 0;
      int currentInMemoryIndexSize = 182 + nullHandlingOverhead;
      int sinkSizeOverhead = BatchAppenderator.ROUGH_OVERHEAD_PER_SINK;
      // currHydrant in the sink still has > 0 bytesInMemory since we do not persist yet
      Assertions.assertEquals(
          currentInMemoryIndexSize,
          ((BatchAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      Assertions.assertEquals(
          currentInMemoryIndexSize + sinkSizeOverhead,
          ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory()
      );

      // We do multiple more adds to the same sink to cause persist.
      for (int i = 0; i < 53; i++) {
        appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "bar_" + i, 1), null);
      }

      // currHydrant size is 0 since we just persist all indexes to disk.
      currentInMemoryIndexSize = 0;
      // We are now over maxSizeInBytes after the add. Hence, we do a persist.
      // currHydrant in the sink has 0 bytesInMemory since we just did a persist
      Assertions.assertEquals(
          currentInMemoryIndexSize,
          ((BatchAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      // no sinks no hydrants after a persist, so we should have zero bytes currently in memory
      Assertions.assertEquals(
          currentInMemoryIndexSize,
          ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory()
      );

      // Add a single row after persisted
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "bob", 1), null);
      // currHydrant in the sink still has > 0 bytesInMemory since we do not persist yet
      currentInMemoryIndexSize = 182 + nullHandlingOverhead;
      Assertions.assertEquals(
          currentInMemoryIndexSize,
          ((BatchAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      Assertions.assertEquals(
          currentInMemoryIndexSize + sinkSizeOverhead,
          ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory()
      );

      // We do multiple more adds to the same sink to cause persist.
      for (int i = 0; i < 53; i++) {
        appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "bar_" + i, 1), null);
      }

      // currHydrant size is 0 since we just persist all indexes to disk.
      currentInMemoryIndexSize = 0;
      // We are now over maxSizeInBytes after the add. Hence, we do a persist.
      // so no sinks & hydrants should be in memory...
      Assertions.assertEquals(
          currentInMemoryIndexSize,
          ((BatchAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      Assertions.assertEquals(
          currentInMemoryIndexSize,
          ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory()
      );
      appenderator.persistAll(null).get();
      appenderator.close();
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory());
    }
  }

  @Test
  @Timeout(value = 5000L, unit = TimeUnit.MILLISECONDS)
  public void testTaskFailAsPersistCannotFreeAnyMoreMemory() throws Exception
  {
    assertThrows(RuntimeException.class, () -> {
      try (final ClosedSegmensSinksBatchAppenderatorTester tester =
          new ClosedSegmensSinksBatchAppenderatorTester(100, 5180, true)) {
        final Appenderator appenderator = tester.getAppenderator();
        appenderator.startJob();
        appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo", 1), null);
      }
    });
  }

  @Test
  public void testTaskDoesNotFailAsExceededMemoryWithSkipBytesInMemoryOverheadCheckConfig() throws Exception
  {
    try (
        final ClosedSegmensSinksBatchAppenderatorTester tester = new ClosedSegmensSinksBatchAppenderatorTester(
            100,
            10,
            null,
            true,
            new SimpleRowIngestionMeters(),
            true
        )
    ) {
      final Appenderator appenderator = tester.getAppenderator();

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo", 1), null);

      // Expected 0 since we persisted after the add
      Assertions.assertEquals(
          0,
          ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory()
      );
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo", 1), null);

      Assertions.assertEquals(
          0,
          ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory()
      );
    }
  }

  @Test
  public void testTaskCleanupInMemoryCounterAfterCloseWithRowInMemory() throws Exception
  {
    try (final ClosedSegmensSinksBatchAppenderatorTester tester =
             new ClosedSegmensSinksBatchAppenderatorTester(100, 10000, true)) {
      final Appenderator appenderator = tester.getAppenderator();

      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo", 1), null);

      // Still under maxSizeInBytes after the add. Hence, we do not persist yet
      int nullHandlingOverhead = NullHandling.sqlCompatible() ? 1 : 0;
      int currentInMemoryIndexSize = 182 + nullHandlingOverhead;
      int sinkSizeOverhead = BatchAppenderator.ROUGH_OVERHEAD_PER_SINK;
      Assertions.assertEquals(
          currentInMemoryIndexSize + sinkSizeOverhead,
          ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory()
      );

      // Close with row still in memory (no persist)
      appenderator.close();

      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory());
    }
  }

  @Test
  public void testMaxBytesInMemoryInMultipleSinks() throws Exception
  {
    try (final ClosedSegmensSinksBatchAppenderatorTester tester =
             new ClosedSegmensSinksBatchAppenderatorTester(1000, 28748, true)) {
      final Appenderator appenderator = tester.getAppenderator();

      appenderator.startJob();
      // next records are 182 bytes
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo", 1), null);
      appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bar", 1), null);

      // Still under maxSizeInBytes after the add. Hence, we do not persist yet
      //expectedSizeInBytes = 44(map overhead) + 28 (TimeAndDims overhead) + 56 (aggregator metrics) + 54 (dimsKeySize) = 182 + 1 byte when null handling is enabled
      int nullHandlingOverhead = NullHandling.sqlCompatible() ? 1 : 0;
      int currentInMemoryIndexSize = 182 + nullHandlingOverhead;
      int sinkSizeOverhead = 2 * BatchAppenderator.ROUGH_OVERHEAD_PER_SINK;
      // currHydrant in the sink still has > 0 bytesInMemory since we do not persist yet
      Assertions.assertEquals(
          currentInMemoryIndexSize,
          ((BatchAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      Assertions.assertEquals(
          currentInMemoryIndexSize,
          ((BatchAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(1))
      );
      Assertions.assertEquals(
          (2 * currentInMemoryIndexSize) + sinkSizeOverhead,
          ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory()
      );

      // We do multiple more adds to the same sink to cause persist.
      for (int i = 0; i < 49; i++) {
        // these records are 186 bytes
        appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "bar_" + i, 1), null);
        appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bar_" + i, 1), null);
      }

      // sinks + currHydrant size is 0 since we just persist all indexes to disk.
      currentInMemoryIndexSize = 0;
      // We are now over maxSizeInBytes after the add. Hence, we do a persist.
      // currHydrant and the sink has 0 bytesInMemory since we just did a persist
      Assertions.assertEquals(
          currentInMemoryIndexSize,
          ((BatchAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      Assertions.assertEquals(
          currentInMemoryIndexSize,
          ((BatchAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(1))
      );
      Assertions.assertEquals(
          currentInMemoryIndexSize,
          ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory()
      );

      // Add a single row after persisted to sink 0
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "bob", 1), null);
      // currHydrant in the sink still has > 0 bytesInMemory since we do not persist yet
      currentInMemoryIndexSize = 182 + nullHandlingOverhead;
      Assertions.assertEquals(
          currentInMemoryIndexSize,
          ((BatchAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      Assertions.assertEquals(
          0,
          ((BatchAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(1))
      );
      // only one sink so far:
      sinkSizeOverhead = BatchAppenderator.ROUGH_OVERHEAD_PER_SINK;
      Assertions.assertEquals(
          currentInMemoryIndexSize + sinkSizeOverhead,
          ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory()
      );
      // Now add a single row to sink 1
      appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bob", 1), null);
      Assertions.assertEquals(
          currentInMemoryIndexSize,
          ((BatchAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      Assertions.assertEquals(
          currentInMemoryIndexSize,
          ((BatchAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(1))
      );
      sinkSizeOverhead += BatchAppenderator.ROUGH_OVERHEAD_PER_SINK;
      Assertions.assertEquals(
          (2 * currentInMemoryIndexSize) + sinkSizeOverhead,
          ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory()
      );

      // We do multiple more adds to the both sink to cause persist.
      for (int i = 0; i < 49; i++) {
        // 186 bytes
        appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "bar_" + i, 1), null);
        appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bar_" + i, 1), null);
      }

      // currHydrant size is 0 since we just persist all indexes to disk.
      currentInMemoryIndexSize = 0;
      // We are now over maxSizeInBytes after the add. Hence, we do a persist.
      // currHydrant in the sink has 0 bytesInMemory since we just did a persist
      Assertions.assertEquals(
          currentInMemoryIndexSize,
          ((BatchAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      Assertions.assertEquals(
          currentInMemoryIndexSize,
          ((BatchAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(1))
      );
      // Mapped index size is the memory still needed after we persisted indexes. Note that the segments have
      // 1 dimension columns, 2 metric column, 1 time column. However, we have two indexes now from the two pervious
      // persists.
      Assertions.assertEquals(
          currentInMemoryIndexSize,
          ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory()
      );
      appenderator.close();
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory());
    }
  }

  @Test
  public void testIgnoreMaxBytesInMemory() throws Exception
  {
    try (final ClosedSegmensSinksBatchAppenderatorTester tester =
             new ClosedSegmensSinksBatchAppenderatorTester(100, -1, true)) {
      final Appenderator appenderator = tester.getAppenderator();

      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
      appenderator.startJob();
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo", 1), null);
      //we still calculate the size even when ignoring it to make persist decision
      int nullHandlingOverhead = NullHandling.sqlCompatible() ? 1 : 0;
      Assertions.assertEquals(
          182 + nullHandlingOverhead,
          ((BatchAppenderator) appenderator).getBytesInMemory(IDENTIFIERS.get(0))
      );
      Assertions.assertEquals(1, ((BatchAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bar", 1), null);

      // we added two rows only, and we told that maxSizeInBytes should be ignored, so it should not have been
      // persisted:
      int sinkSizeOverhead = 2 * BatchAppenderator.ROUGH_OVERHEAD_PER_SINK;
      Assertions.assertEquals(
          (364 + 2 * nullHandlingOverhead) + sinkSizeOverhead,
          ((BatchAppenderator) appenderator).getBytesCurrentlyInMemory()
      );
      Assertions.assertEquals(2, ((BatchAppenderator) appenderator).getRowsInMemory());
      appenderator.close();
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
    }
  }

  @Test
  public void testMaxRowsInMemory() throws Exception
  {
    try (final ClosedSegmensSinksBatchAppenderatorTester tester = new ClosedSegmensSinksBatchAppenderatorTester(3, true)) {
      final Appenderator appenderator = tester.getAppenderator();

      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
      appenderator.startJob();
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo", 1), null);
      Assertions.assertEquals(1, ((BatchAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bar", 1), null);
      Assertions.assertEquals(2, ((BatchAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bar", 1), null);
      Assertions.assertEquals(2, ((BatchAppenderator) appenderator).getRowsInMemory());
      // no persist since last add was for a dup record
      appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bat", 1), null);
      // persist expected ^ (3) rows added
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());

      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "baz", 1), null);
      Assertions.assertEquals(1, ((BatchAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "qux", 1), null);
      Assertions.assertEquals(2, ((BatchAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "bob", 1), null);
      // persist expected ^ (3) rows added
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
      appenderator.close();
    }
  }

  @Test
  public void testAllHydrantsAreRecovered() throws Exception
  {
    try (final ClosedSegmensSinksBatchAppenderatorTester tester = new ClosedSegmensSinksBatchAppenderatorTester(1, false)) {
      final Appenderator appenderator = tester.getAppenderator();

      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
      appenderator.startJob();
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo", 1), null);
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo2", 1), null);
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo3", 1), null);

      // Since maxRowsInMemory is one there ought to be three hydrants stored and recovered
      // just before push, internally the code has a sanity check to make sure that this works. If it does not it throws
      // an exception
      final SegmentsAndCommitMetadata segmentsAndCommitMetadata = appenderator.push(
          appenderator.getSegments(),
          null,
          false
      ).get();
      Assertions.assertEquals(
          IDENTIFIERS.subList(0, 1),
          Lists.transform(
              segmentsAndCommitMetadata.getSegments(),
              SegmentIdWithShardSpec::fromDataSegment
          ).stream().sorted().collect(Collectors.toList())
      );
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
      appenderator.close();
    }
  }

  @Test
  public void testTotalRowsPerSegment() throws Exception
  {
    try (final ClosedSegmensSinksBatchAppenderatorTester tester = new ClosedSegmensSinksBatchAppenderatorTester(3, true)) {
      final Appenderator appenderator = tester.getAppenderator();

      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
      appenderator.startJob();
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
      Appenderator.AppenderatorAddResult addResult0 =
          appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo", 1), null);
      Assertions.assertEquals(1, ((BatchAppenderator) appenderator).getRowsInMemory());
      Assertions.assertEquals(1, addResult0.getNumRowsInSegment());

      Appenderator.AppenderatorAddResult addResult1 =
          appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bar", 1), null);
      Assertions.assertEquals(2, ((BatchAppenderator) appenderator).getRowsInMemory());
      Assertions.assertEquals(1, addResult1.getNumRowsInSegment());

      addResult1 = // dup!
          appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bar", 1), null);
      Assertions.assertEquals(2, ((BatchAppenderator) appenderator).getRowsInMemory());
      Assertions.assertEquals(1, addResult1.getNumRowsInSegment()); // dup record does not count
      // no persist since last add was for a dup record

      addResult1 =
          appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bat", 1), null);
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
      Assertions.assertEquals(2, addResult1.getNumRowsInSegment());
      // persist expected ^ (3) rows added

      // total rows per segment ought to be preserved even when sinks are removed from memory:
      addResult1 =
          appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bat", 1), null);
      Assertions.assertEquals(1, ((BatchAppenderator) appenderator).getRowsInMemory());
      Assertions.assertEquals(3, addResult1.getNumRowsInSegment());

      addResult0 =
          appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "baz", 1), null);
      Assertions.assertEquals(2, ((BatchAppenderator) appenderator).getRowsInMemory());
      Assertions.assertEquals(2, addResult0.getNumRowsInSegment());

      addResult1 =
          appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "qux", 1), null);
      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
      Assertions.assertEquals(4, addResult1.getNumRowsInSegment());
      // persist expected ^ (3) rows added

      addResult0 =
          appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "bob", 1), null);
      Assertions.assertEquals(1, ((BatchAppenderator) appenderator).getRowsInMemory());
      Assertions.assertEquals(3, addResult0.getNumRowsInSegment());

      appenderator.close();

      Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
    }
  }


  @Test
  public void testRestoreFromDisk() throws Exception
  {
    final ClosedSegmensSinksBatchAppenderatorTester tester = new ClosedSegmensSinksBatchAppenderatorTester(2, true);
    final Appenderator appenderator = tester.getAppenderator();

    appenderator.startJob();

    appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo", 1), null);
    appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "bar", 2), null);

    Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());

    appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "baz", 3), null);
    appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "qux", 4), null);

    Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());

    appenderator.add(IDENTIFIERS.get(2), createInputRow("2001", "bob", 5), null);
    Assertions.assertEquals(1, ((BatchAppenderator) appenderator).getRowsInMemory());
    appenderator.persistAll(null).get();

    Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());

    List<File> segmentPaths = ((BatchAppenderator) appenderator).getPersistedidentifierPaths();
    Assertions.assertNotNull(segmentPaths);
    Assertions.assertEquals(3, segmentPaths.size());


    appenderator.push(IDENTIFIERS, null, false).get();

    segmentPaths = ((BatchAppenderator) appenderator).getPersistedidentifierPaths();
    Assertions.assertNotNull(segmentPaths);
    Assertions.assertEquals(0, segmentPaths.size());

    appenderator.close();

  }

  @Test
  public void testCleanupFromDiskAfterClose() throws Exception
  {
    final ClosedSegmensSinksBatchAppenderatorTester tester = new ClosedSegmensSinksBatchAppenderatorTester(2, true);
    final Appenderator appenderator = tester.getAppenderator();

    appenderator.startJob();

    appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo", 1), null);
    appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "bar", 2), null);
    Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
    Assertions.assertEquals(2, appenderator.getTotalRowCount());

    appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "baz", 3), null);
    appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "qux", 4), null);
    Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
    Assertions.assertEquals(4, appenderator.getTotalRowCount());

    appenderator.add(IDENTIFIERS.get(2), createInputRow("2001", "bob", 5), null);
    Assertions.assertEquals(1, ((BatchAppenderator) appenderator).getRowsInMemory());
    appenderator.persistAll(null).get();
    Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
    Assertions.assertEquals(5, appenderator.getTotalRowCount());

    List<File> segmentPaths = ((BatchAppenderator) appenderator).getPersistedidentifierPaths();
    Assertions.assertNotNull(segmentPaths);
    Assertions.assertEquals(3, segmentPaths.size());

    appenderator.close();

    segmentPaths = ((BatchAppenderator) appenderator).getPersistedidentifierPaths();
    Assertions.assertNotNull(segmentPaths);
    Assertions.assertEquals(0, segmentPaths.size());

    Assertions.assertEquals(0, ((BatchAppenderator) appenderator).getRowsInMemory());
    Assertions.assertEquals(0, appenderator.getTotalRowCount());

  }


  @Test
  @Timeout(value = 5000L, unit = TimeUnit.MILLISECONDS)
  public void testTotalRowCount() throws Exception
  {
    try (final ClosedSegmensSinksBatchAppenderatorTester tester = new ClosedSegmensSinksBatchAppenderatorTester(3, true)) {
      final Appenderator appenderator = tester.getAppenderator();

      Assertions.assertEquals(0, appenderator.getTotalRowCount());
      appenderator.startJob();
      Assertions.assertEquals(0, appenderator.getTotalRowCount());
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo", 1), null);
      Assertions.assertEquals(1, appenderator.getTotalRowCount());
      appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bar", 1), null);
      Assertions.assertEquals(2, appenderator.getTotalRowCount());

      appenderator.persistAll(null).get();
      Assertions.assertEquals(2, appenderator.getTotalRowCount());
      appenderator.drop(IDENTIFIERS.get(0)).get();
      Assertions.assertEquals(1, appenderator.getTotalRowCount());
      appenderator.drop(IDENTIFIERS.get(1)).get();
      Assertions.assertEquals(0, appenderator.getTotalRowCount());

      appenderator.add(IDENTIFIERS.get(2), createInputRow("2001", "bar", 1), null);
      Assertions.assertEquals(1, appenderator.getTotalRowCount());
      appenderator.add(IDENTIFIERS.get(2), createInputRow("2001", "baz", 1), null);
      Assertions.assertEquals(2, appenderator.getTotalRowCount());
      appenderator.add(IDENTIFIERS.get(2), createInputRow("2001", "qux", 1), null);
      Assertions.assertEquals(3, appenderator.getTotalRowCount());
      appenderator.add(IDENTIFIERS.get(2), createInputRow("2001", "bob", 1), null);
      Assertions.assertEquals(4, appenderator.getTotalRowCount());

      appenderator.persistAll(null).get();
      Assertions.assertEquals(4, appenderator.getTotalRowCount());
      appenderator.drop(IDENTIFIERS.get(2)).get();
      Assertions.assertEquals(0, appenderator.getTotalRowCount());

      appenderator.close();
      Assertions.assertEquals(0, appenderator.getTotalRowCount());
    }
  }

  @Test
  public void testVerifyRowIngestionMetrics() throws Exception
  {
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    try (final ClosedSegmensSinksBatchAppenderatorTester tester =
             new ClosedSegmensSinksBatchAppenderatorTester(5,
                                                           10000L,
                                                           null, false, rowIngestionMeters
             )) {
      final Appenderator appenderator = tester.getAppenderator();
      appenderator.startJob();
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000",
                                                          "foo", "invalid_met"
      ), null);
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "foo", 1), null);

      Assertions.assertEquals(1, rowIngestionMeters.getProcessed());
      Assertions.assertEquals(1, rowIngestionMeters.getProcessedWithError());
      Assertions.assertEquals(0, rowIngestionMeters.getUnparseable());
      Assertions.assertEquals(0, rowIngestionMeters.getThrownAway());
    }
  }

  @Test
  @Timeout(value = 10000L, unit = TimeUnit.MILLISECONDS)
  public void testPushContract() throws Exception
  {
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    try (final ClosedSegmensSinksBatchAppenderatorTester tester =
             new ClosedSegmensSinksBatchAppenderatorTester(1,
                                                           50000L,
                                                           null, false, rowIngestionMeters
             )) {
      final Appenderator appenderator = tester.getAppenderator();
      appenderator.startJob();

      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "bar", 1), null);
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "bar2", 1), null);
      appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bar3", 1), null);

      // push only a single segment
      final SegmentsAndCommitMetadata segmentsAndCommitMetadata = appenderator.push(
          Collections.singletonList(IDENTIFIERS.get(0)),
          null,
          false
      ).get();

      // only one segment must have been pushed:
      Assertions.assertEquals(
          Collections.singletonList(IDENTIFIERS.get(0)),
          Lists.transform(
              segmentsAndCommitMetadata.getSegments(),
              SegmentIdWithShardSpec::fromDataSegment
          ).stream().sorted().collect(Collectors.toList())
      );

      Assertions.assertEquals(
          tester.getPushedSegments().stream().sorted().collect(Collectors.toList()),
          segmentsAndCommitMetadata.getSegments().stream().sorted().collect(Collectors.toList())
      );
      // the responsability for dropping is in the BatchAppenderatorDriver, drop manually:
      appenderator.drop(IDENTIFIERS.get(0));

      // and the segment that was not pushed should still be active
      Assertions.assertEquals(
          Collections.singletonList(IDENTIFIERS.get(1)),
          appenderator.getSegments()
      );


    }
  }

  @Test
  @Timeout(value = 5000L, unit = TimeUnit.MILLISECONDS)
  public void testCloseContract() throws Exception
  {
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    try (final ClosedSegmensSinksBatchAppenderatorTester tester =
             new ClosedSegmensSinksBatchAppenderatorTester(1,
                                                           50000L,
                                                           null, false, rowIngestionMeters
             )) {
      final Appenderator appenderator = tester.getAppenderator();
      appenderator.startJob();

      // each one of these adds will trigger a persist since maxRowsInMemory is set to one above
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "bar", 1), null);
      appenderator.add(IDENTIFIERS.get(0), createInputRow("2000", "bar2", 1), null);

      // push only a single segment
      ListenableFuture<SegmentsAndCommitMetadata> firstFuture = appenderator.push(
          Collections.singletonList(IDENTIFIERS.get(0)),
          null,
          false
      );

      // push remaining segments:
      appenderator.add(IDENTIFIERS.get(1), createInputRow("2000", "bar3", 1), null);
      ListenableFuture<SegmentsAndCommitMetadata> secondFuture = appenderator.push(
          Collections.singletonList(IDENTIFIERS.get(1)),
          null,
          false
      );

      // close should wait for all pushes and persists to end:
      appenderator.close();

      Assertions.assertTrue(!firstFuture.isCancelled());
      Assertions.assertTrue(!secondFuture.isCancelled());

      Assertions.assertTrue(firstFuture.isDone());
      Assertions.assertTrue(secondFuture.isDone());

      final SegmentsAndCommitMetadata segmentsAndCommitMetadataForFirstFuture = firstFuture.get();
      final SegmentsAndCommitMetadata segmentsAndCommitMetadataForSecondFuture = secondFuture.get();

      // all segments must have been pushed:
      Assertions.assertEquals(segmentsAndCommitMetadataForFirstFuture.getSegments().size(), 1);
      Assertions.assertEquals(segmentsAndCommitMetadataForSecondFuture.getSegments().size(), 1);

    }
  }



  private static SegmentIdWithShardSpec createNonUTCSegmentId(String interval, String version, int partitionNum)
  {
    return new SegmentIdWithShardSpec(
        ClosedSegmensSinksBatchAppenderatorTester.DATASOURCE,
        new Interval(interval, ISOChronology.getInstance(DateTimes.inferTzFromString("Asia/Seoul"))),
        version,
        new LinearShardSpec(partitionNum)

    );
  }

  private static SegmentIdWithShardSpec createSegmentId(String interval, String version, int partitionNum)
  {
    return new SegmentIdWithShardSpec(
        ClosedSegmensSinksBatchAppenderatorTester.DATASOURCE,
        Intervals.of(interval),
        version,
        new LinearShardSpec(partitionNum)

    );
  }

  static InputRow createInputRow(String ts, String dim, Object met)
  {
    return new MapBasedInputRow(
        DateTimes.of(ts).getMillis(),
        ImmutableList.of("dim"),
        ImmutableMap.of(
            "dim",
            dim,
            "met",
            met
        )
    );
  }

}

