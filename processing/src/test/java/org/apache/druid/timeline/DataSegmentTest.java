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

package org.apache.druid.timeline;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.RangeSet;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.PartitionChunk;
import org.apache.druid.timeline.partition.ShardSpec;
import org.apache.druid.timeline.partition.ShardSpecLookup;
import org.apache.druid.timeline.partition.TombstoneShardSpec;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
public class DataSegmentTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private static final int TEST_VERSION = 0x9;

  private static ShardSpec getShardSpec(final int partitionNum)
  {
    return new ShardSpec()
    {
      @Override
      public <T> PartitionChunk<T> createChunk(T obj)
      {
        return null;
      }

      @Override
      public int getPartitionNum()
      {
        return partitionNum;
      }

      @Override
      public int getNumCorePartitions()
      {
        return 0;
      }

      @Override
      public ShardSpecLookup getLookup(List<? extends ShardSpec> shardSpecs)
      {
        return null;
      }

      @Override
      public List<String> getDomainDimensions()
      {
        return ImmutableList.of();
      }

      @Override
      public boolean possibleInDomain(Map<String, RangeSet<String>> domain)
      {
        return true;
      }

    };
  }

  @BeforeEach
  public void setUp()
  {
    InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT);
    MAPPER.setInjectableValues(injectableValues);
  }

  @Test
  public void testV1Serialization() throws Exception
  {

    final Interval interval = Intervals.of("2011-10-01/2011-10-02");
    final ImmutableMap<String, Object> loadSpec = ImmutableMap.of("something", "or_other");

    DataSegment segment = new DataSegment(
        "something",
        interval,
        "1",
        loadSpec,
        Arrays.asList("dim1", "dim2"),
        Arrays.asList("met1", "met2"),
        new NumberedShardSpec(3, 0),
        new CompactionState(
            new HashedPartitionsSpec(100000, null, ImmutableList.of("dim1")),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "bar", "foo"))
            ),
            ImmutableList.of(ImmutableMap.of("type", "count", "name", "count")),
            ImmutableMap.of("filter", ImmutableMap.of("type", "selector", "dimension", "dim1", "value", "foo")),
            ImmutableMap.of(),
            ImmutableMap.of()
        ),
        TEST_VERSION,
        1
    );

    final Map<String, Object> objectMap = MAPPER.readValue(
        MAPPER.writeValueAsString(segment),
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    Assertions.assertEquals(11, objectMap.size());
    Assertions.assertEquals("something", objectMap.get("dataSource"));
    Assertions.assertEquals(interval.toString(), objectMap.get("interval"));
    Assertions.assertEquals("1", objectMap.get("version"));
    Assertions.assertEquals(loadSpec, objectMap.get("loadSpec"));
    Assertions.assertEquals("dim1,dim2", objectMap.get("dimensions"));
    Assertions.assertEquals("met1,met2", objectMap.get("metrics"));
    Assertions.assertEquals(ImmutableMap.of("type", "numbered", "partitionNum", 3, "partitions", 0), objectMap.get("shardSpec"));
    Assertions.assertEquals(TEST_VERSION, objectMap.get("binaryVersion"));
    Assertions.assertEquals(1, objectMap.get("size"));
    Assertions.assertEquals(6, ((Map) objectMap.get("lastCompactionState")).size());

    DataSegment deserializedSegment = MAPPER.readValue(MAPPER.writeValueAsString(segment), DataSegment.class);

    Assertions.assertEquals(segment.getDataSource(), deserializedSegment.getDataSource());
    Assertions.assertEquals(segment.getInterval(), deserializedSegment.getInterval());
    Assertions.assertEquals(segment.getVersion(), deserializedSegment.getVersion());
    Assertions.assertEquals(segment.getLoadSpec(), deserializedSegment.getLoadSpec());
    Assertions.assertEquals(segment.getDimensions(), deserializedSegment.getDimensions());
    Assertions.assertEquals(segment.getMetrics(), deserializedSegment.getMetrics());
    Assertions.assertEquals(segment.getShardSpec(), deserializedSegment.getShardSpec());
    Assertions.assertEquals(segment.getSize(), deserializedSegment.getSize());
    Assertions.assertEquals(segment.getId(), deserializedSegment.getId());
    Assertions.assertEquals(segment.getLastCompactionState(), deserializedSegment.getLastCompactionState());

    deserializedSegment = MAPPER.readValue(MAPPER.writeValueAsString(segment), DataSegment.class);
    Assertions.assertEquals(0, segment.compareTo(deserializedSegment));

    deserializedSegment = MAPPER.readValue(MAPPER.writeValueAsString(segment), DataSegment.class);
    Assertions.assertEquals(0, deserializedSegment.compareTo(segment));

    deserializedSegment = MAPPER.readValue(MAPPER.writeValueAsString(segment), DataSegment.class);
    Assertions.assertEquals(segment.hashCode(), deserializedSegment.hashCode());
  }

  @Test
  public void testDeserializationDataSegmentLastCompactionStateWithNullSpecs() throws Exception
  {
    final Interval interval = Intervals.of("2011-10-01/2011-10-02");
    final ImmutableMap<String, Object> loadSpec = ImmutableMap.of("something", "or_other");
    DataSegment segment = new DataSegment(
        "something",
        interval,
        "1",
        loadSpec,
        Arrays.asList("dim1", "dim2"),
        Arrays.asList("met1", "met2"),
        new NumberedShardSpec(3, 0),
        new CompactionState(
            new HashedPartitionsSpec(100000, null, ImmutableList.of("dim1")),
            null,
            null,
            null,
            ImmutableMap.of(),
            ImmutableMap.of()
        ),
        TEST_VERSION,
        1
    );
    // lastCompactionState has null specs for dimensionSpec and transformSpec
    String lastCompactionStateWithNullSpecs = "{"
                                              + "\"dataSource\": \"something\","
                                              + "\"interval\": \"2011-10-01T00:00:00.000Z/2011-10-02T00:00:00.000Z\","
                                              + "\"version\": \"1\","
                                              + "\"loadSpec\": {"
                                              + " \"something\": \"or_other\""
                                              + "},"
                                              + "\"dimensions\": \"dim1,dim2\","
                                              + "\"metrics\": \"met1,met2\","
                                              + "\"shardSpec\": {"
                                              + " \"type\": \"numbered\","
                                              + " \"partitionNum\": 3,"
                                              + " \"partitions\": 0"
                                              + "},"
                                              + "\"lastCompactionState\": {"
                                              + " \"partitionsSpec\": {"
                                              + "  \"type\": \"hashed\","
                                              + "  \"numShards\": null,"
                                              + "  \"partitionDimensions\": [\"dim1\"],"
                                              + "  \"partitionFunction\": \"murmur3_32_abs\","
                                              + "  \"maxRowsPerSegment\": 100000"
                                              + " },"
                                              + " \"indexSpec\": {},"
                                              + " \"granularitySpec\": {}"
                                              + "},"
                                              + "\"binaryVersion\": 9,"
                                              + "\"size\": 1,"
                                              + "\"identifier\": \"something_2011-10-01T00:00:00.000Z_2011-10-02T00:00:00.000Z_1_3\""
                                              + "}";

    final Map<String, Object> objectMap = MAPPER.readValue(
        lastCompactionStateWithNullSpecs,
        JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );
    Assertions.assertEquals(11, objectMap.size());
    Assertions.assertEquals("something", objectMap.get("dataSource"));
    Assertions.assertEquals(interval.toString(), objectMap.get("interval"));
    Assertions.assertEquals("1", objectMap.get("version"));
    Assertions.assertEquals(loadSpec, objectMap.get("loadSpec"));
    Assertions.assertEquals("dim1,dim2", objectMap.get("dimensions"));
    Assertions.assertEquals("met1,met2", objectMap.get("metrics"));
    Assertions.assertEquals(ImmutableMap.of("type", "numbered", "partitionNum", 3, "partitions", 0), objectMap.get("shardSpec"));
    Assertions.assertEquals(TEST_VERSION, objectMap.get("binaryVersion"));
    Assertions.assertEquals(1, objectMap.get("size"));
    Assertions.assertEquals(3, ((Map) objectMap.get("lastCompactionState")).size());

    DataSegment deserializedSegment = MAPPER.readValue(lastCompactionStateWithNullSpecs, DataSegment.class);
    Assertions.assertEquals(segment.getDataSource(), deserializedSegment.getDataSource());
    Assertions.assertEquals(segment.getInterval(), deserializedSegment.getInterval());
    Assertions.assertEquals(segment.getVersion(), deserializedSegment.getVersion());
    Assertions.assertEquals(segment.getLoadSpec(), deserializedSegment.getLoadSpec());
    Assertions.assertEquals(segment.getDimensions(), deserializedSegment.getDimensions());
    Assertions.assertEquals(segment.getMetrics(), deserializedSegment.getMetrics());
    Assertions.assertEquals(segment.getShardSpec(), deserializedSegment.getShardSpec());
    Assertions.assertEquals(segment.getSize(), deserializedSegment.getSize());
    Assertions.assertEquals(segment.getId(), deserializedSegment.getId());
    Assertions.assertEquals(segment.getLastCompactionState(), deserializedSegment.getLastCompactionState());
    Assertions.assertNotNull(segment.getLastCompactionState());
    Assertions.assertNull(segment.getLastCompactionState().getDimensionsSpec());
    Assertions.assertNull(segment.getLastCompactionState().getTransformSpec());
    Assertions.assertNull(segment.getLastCompactionState().getMetricsSpec());
    Assertions.assertNotNull(deserializedSegment.getLastCompactionState());
    Assertions.assertNull(deserializedSegment.getLastCompactionState().getDimensionsSpec());

    deserializedSegment = MAPPER.readValue(lastCompactionStateWithNullSpecs, DataSegment.class);
    Assertions.assertEquals(0, segment.compareTo(deserializedSegment));

    deserializedSegment = MAPPER.readValue(lastCompactionStateWithNullSpecs, DataSegment.class);
    Assertions.assertEquals(0, deserializedSegment.compareTo(segment));

    deserializedSegment = MAPPER.readValue(lastCompactionStateWithNullSpecs, DataSegment.class);
    Assertions.assertEquals(segment.hashCode(), deserializedSegment.hashCode());
  }

  @Test
  public void testIdentifier()
  {
    final DataSegment segment = DataSegment.builder()
                                           .dataSource("foo")
                                           .interval(Intervals.of("2012-01-01/2012-01-02"))
                                           .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                           .shardSpec(NoneShardSpec.instance())
                                           .size(0)
                                           .build();

    Assertions.assertEquals(
        "foo_2012-01-01T00:00:00.000Z_2012-01-02T00:00:00.000Z_2012-01-01T11:22:33.444Z",
        segment.getId().toString()
    );
  }

  @Test
  public void testIdentifierWithZeroPartition()
  {
    final DataSegment segment = DataSegment.builder()
                                           .dataSource("foo")
                                           .interval(Intervals.of("2012-01-01/2012-01-02"))
                                           .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                           .shardSpec(getShardSpec(0))
                                           .size(0)
                                           .build();

    Assertions.assertEquals(
        "foo_2012-01-01T00:00:00.000Z_2012-01-02T00:00:00.000Z_2012-01-01T11:22:33.444Z",
        segment.getId().toString()
    );
  }

  @Test
  public void testIdentifierWithNonzeroPartition()
  {
    final DataSegment segment = DataSegment.builder()
                                           .dataSource("foo")
                                           .interval(Intervals.of("2012-01-01/2012-01-02"))
                                           .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                           .shardSpec(getShardSpec(7))
                                           .size(0)
                                           .build();

    Assertions.assertEquals(
        "foo_2012-01-01T00:00:00.000Z_2012-01-02T00:00:00.000Z_2012-01-01T11:22:33.444Z_7",
        segment.getId().toString()
    );
  }

  @Test
  public void testV1SerializationNullMetrics() throws Exception
  {
    final DataSegment segment =
        makeDataSegment("foo", "2012-01-01/2012-01-02", DateTimes.of("2012-01-01T11:22:33.444Z").toString());

    final DataSegment segment2 = MAPPER.readValue(MAPPER.writeValueAsString(segment), DataSegment.class);
    Assertions.assertEquals(ImmutableList.of(), segment2.getDimensions(), "empty dimensions");
    Assertions.assertEquals(ImmutableList.of(), segment2.getMetrics(), "empty metrics");
  }

  @Test
  public void testWithLastCompactionState()
  {
    final CompactionState compactionState = new CompactionState(
        new DynamicPartitionsSpec(null, null),
        new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
        ImmutableList.of(ImmutableMap.of("type", "count", "name", "count")),
        ImmutableMap.of("filter", ImmutableMap.of("type", "selector", "dimension", "dim1", "value", "foo")),
        Collections.singletonMap("test", "map"),
        Collections.singletonMap("test2", "map2")
    );
    final DataSegment segment1 = DataSegment.builder()
                                            .dataSource("foo")
                                            .interval(Intervals.of("2012-01-01/2012-01-02"))
                                            .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                            .shardSpec(getShardSpec(7))
                                            .size(0)
                                            .lastCompactionState(compactionState)
                                            .build();
    final DataSegment segment2 = DataSegment.builder()
                                           .dataSource("foo")
                                           .interval(Intervals.of("2012-01-01/2012-01-02"))
                                           .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                           .shardSpec(getShardSpec(7))
                                           .size(0)
                                           .build();
    Assertions.assertEquals(segment1, segment2.withLastCompactionState(compactionState));
  }

  @Test
  public void testTombstoneType()
  {

    final DataSegment segment1 = DataSegment.builder()
                                            .dataSource("foo")
                                            .interval(Intervals.of("2012-01-01/2012-01-02"))
                                            .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                            .shardSpec(new TombstoneShardSpec())
                                            .loadSpec(Collections.singletonMap(
                                                "type",
                                                DataSegment.TOMBSTONE_LOADSPEC_TYPE
                                            ))
                                            .size(0)
                                            .build();
    Assertions.assertTrue(segment1.isTombstone());
    Assertions.assertFalse(segment1.hasData());

    final DataSegment segment2 = DataSegment.builder()
                                            .dataSource("foo")
                                            .interval(Intervals.of("2012-01-01/2012-01-02"))
                                            .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                            .shardSpec(getShardSpec(7))
                                            .loadSpec(Collections.singletonMap(
                                                "type",
                                                "foo"
                                            ))
                                            .size(0)
                                            .build();

    Assertions.assertFalse(segment2.isTombstone());
    Assertions.assertTrue(segment2.hasData());

    final DataSegment segment3 = DataSegment.builder()
                                            .dataSource("foo")
                                            .interval(Intervals.of("2012-01-01/2012-01-02"))
                                            .version(DateTimes.of("2012-01-01T11:22:33.444Z").toString())
                                            .shardSpec(getShardSpec(7))
                                            .size(0)
                                            .build();

    Assertions.assertFalse(segment3.isTombstone());
    Assertions.assertTrue(segment3.hasData());

  }

  private DataSegment makeDataSegment(String dataSource, String interval, String version)
  {
    return DataSegment.builder()
                      .dataSource(dataSource)
                      .interval(Intervals.of(interval))
                      .version(version)
                      .size(1)
                      .build();
  }
}
