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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.druid.jackson.CommaListJoinDeserializer;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public class SegmentChangeRequestLoad implements DataSegmentChangeRequest
{
  private static class Serialized extends DataSegment {
    @JsonProperty
    int age;
    @JsonCreator
    public Serialized(
        @JsonProperty("dataSource") String dataSource,
        @JsonProperty("interval") Interval interval,
        @JsonProperty("version") String version,
        // use `Map` *NOT* `LoadSpec` because we want to do lazy materialization
        // to prevent dependency pollution
        @JsonProperty("loadSpec") @Nullable Map<String, Object> loadSpec,
        @JsonProperty("dimensions") @JsonDeserialize(using = CommaListJoinDeserializer.class) @Nullable List<String> dimensions,
        @JsonProperty("metrics") @JsonDeserialize(using = CommaListJoinDeserializer.class) @Nullable List<String> metrics,
        @JsonProperty("shardSpec") @Nullable ShardSpec shardSpec,
        @JsonProperty("lastCompactionState") @Nullable CompactionState lastCompactionState,
        @JsonProperty("binaryVersion") Integer binaryVersion,
        @JsonProperty("size") long size,
    @JsonProperty("age") int age)

    {
      super(
          dataSource,
          interval,
          version,
          loadSpec,
          dimensions,
          metrics,
          shardSpec,
          lastCompactionState,
          binaryVersion,
          size,
          PruneSpecsHolder.DEFAULT
      );
      this.age=age;
    }
    public Serialized(DataSegment segment, int something)
    {
      this(segment.getDataSource(),
      segment.getInterval(),
      segment.getVersion(),
      segment.getLoadSpec(),
      segment.getDimensions(),
      segment.getMetrics(),
      segment.getShardSpec(),
      segment.getLastCompactionState(),
      segment.getBinaryVersion(),
      segment.getSize(),
      something);
    }
  }

  final public int something;

  private final DataSegment segment;

  /**
   * To avoid pruning of the loadSpec on the broker, needed when the broker is loading broadcast segments,
   * we deserialize into an {@link LoadableDataSegment}, which never removes the loadSpec.
   */
  @JsonCreator
  public SegmentChangeRequestLoad(
      @JsonUnwrapped Serialized s
  )
  {
    this.segment = DataSegment.builder(s).build();
    this.something = s.age;
  }

  @JsonUnwrapped
  private Serialized buildSer() {

return    new Serialized(segment,something);

  }

  public SegmentChangeRequestLoad(
      DataSegment segment
  )
  {
    this.segment = segment;
    this.something = 33;
  }


  @Override
  public void go(DataSegmentChangeHandler handler, @Nullable DataSegmentChangeCallback callback)
  {
    handler.addSegment(segment, callback);
  }

  public DataSegment getSegment()
  {
    return segment;
  }

  @Override
  public String asString()
  {
    return StringUtils.format("LOAD: %s", segment.getId());
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentChangeRequestLoad that = (SegmentChangeRequestLoad) o;
    return Objects.equals(segment, that.segment);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segment);
  }

  @Override
  public String toString()
  {
    return "SegmentChangeRequestLoad{" +
           "segment=" + segment +
           '}';
  }
}
