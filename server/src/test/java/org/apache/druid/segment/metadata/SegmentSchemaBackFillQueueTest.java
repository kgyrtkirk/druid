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

package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.firstlast.first.LongFirstAggregatorFactory;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SchemaPayloadPlus;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class SegmentSchemaBackFillQueueTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbyConnector.DerbyConnectorRule(CentralizedDatasourceSchemaConfig.enabled(true));

  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  @Test
  public void testPublishSchema() throws InterruptedException
  {
    TestDerbyConnector derbyConnector = derbyConnectorRule.getConnector();
    derbyConnector.createSegmentSchemasTable();
    derbyConnector.createSegmentTable();

    SegmentSchemaManager segmentSchemaManager = new SegmentSchemaManager(
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        mapper,
        derbyConnector
    );

    SegmentSchemaTestUtils segmentSchemaTestUtils =
        new SegmentSchemaTestUtils(derbyConnectorRule, derbyConnector, mapper);
    SegmentSchemaCache segmentSchemaCache = new SegmentSchemaCache();
    CentralizedDatasourceSchemaConfig config = new CentralizedDatasourceSchemaConfig(true, true, 1L, null);

    CountDownLatch latch = new CountDownLatch(1);

    StubServiceEmitter emitter = new StubServiceEmitter("coordinator", "host");

    SegmentSchemaBackFillQueue segmentSchemaBackFillQueue =
        new SegmentSchemaBackFillQueue(
            segmentSchemaManager,
            ScheduledExecutors::fixed,
            segmentSchemaCache,
            new FingerprintGenerator(mapper),
            emitter,
            config
        ) {
          @Override
          public void processBatchesDue()
          {
            super.processBatchesDue();
            latch.countDown();
          }
        };

    final DataSegment segment1 = new DataSegment(
        "foo",
        Intervals.of("2023-01-01/2023-01-02"),
        "2023-01-01",
        ImmutableMap.of("path", "a-1"),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new LinearShardSpec(0),
        9,
        100
    );

    final DataSegment segment2 = new DataSegment(
        "foo",
        Intervals.of("2023-01-02/2023-01-03"),
        "2023-02-01",
        ImmutableMap.of("path", "a-1"),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new LinearShardSpec(0),
        9,
        100
    );

    final DataSegment segment3 = new DataSegment(
        "foo1",
        Intervals.of("2023-01-01/2023-01-02"),
        "2023-01-01",
        ImmutableMap.of("path", "a-1"),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new LinearShardSpec(0),
        9,
        100
    );
    Set<DataSegment> segments = new HashSet<>();
    segments.add(segment1);
    segments.add(segment2);
    segments.add(segment3);
    segmentSchemaTestUtils.insertUsedSegments(segments, Collections.emptyMap());

    final Map<SegmentId, SchemaPayloadPlus> segmentIdSchemaMap = new HashMap<>();
    RowSignature rowSignature = RowSignature.builder().add("cx", ColumnType.FLOAT).build();
    Map<String, AggregatorFactory> aggregatorFactoryMap = new HashMap<>();
    aggregatorFactoryMap.put("longFirst", new LongFirstAggregatorFactory("longFirst", "long-col", null));

    final SchemaPayloadPlus schema = new SchemaPayloadPlus(new SchemaPayload(rowSignature, aggregatorFactoryMap), 20L);
    segmentIdSchemaMap.put(segment1.getId(), schema);
    segmentIdSchemaMap.put(segment2.getId(), schema);
    segmentIdSchemaMap.put(segment3.getId(), schema);

    segmentSchemaBackFillQueue.add(segment1.getId(), schema);
    segmentSchemaBackFillQueue.add(segment2.getId(), schema);
    segmentSchemaBackFillQueue.add(segment3.getId(), schema);
    segmentSchemaBackFillQueue.onLeaderStart();
    latch.await();
    segmentSchemaTestUtils.verifySegmentSchema(segmentIdSchemaMap);
    emitter.verifyValue(Metric.SCHEMAS_BACKFILLED, Map.of(DruidMetrics.DATASOURCE, "foo"), 2);
    emitter.verifyValue(Metric.SCHEMAS_BACKFILLED, Map.of(DruidMetrics.DATASOURCE, "foo1"), 1);
  }
}
