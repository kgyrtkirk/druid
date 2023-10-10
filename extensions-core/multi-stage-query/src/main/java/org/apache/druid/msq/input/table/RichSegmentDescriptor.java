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

package org.apache.druid.msq.input.table;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 * Like {@link SegmentDescriptor}, but provides both the full interval and the clipped interval for a segment
 * (SegmentDescriptor only provides the clipped interval.), as well as the metadata of the servers it is loaded on.
 * <br>
 * To keep the serialized form lightweight, the full interval is only serialized if it is different from the
 * clipped interval.
 * <br>
 * It is possible to deserialize this class as {@link SegmentDescriptor}. However, going the other direction is
 * not a good idea, because the {@link #fullInterval} and {@link #servers} will not end up being set correctly.
 */
public class RichSegmentDescriptor extends SegmentDescriptor
{
  @Nullable
  private final Interval fullInterval;
  private final Set<DruidServerMetadata> servers;

  public RichSegmentDescriptor(
      final Interval fullInterval,
      final Interval interval,
      final String version,
      final int partitionNumber,
      final Set<DruidServerMetadata> servers
  )
  {
    super(interval, version, partitionNumber);
    this.fullInterval = interval.equals(Preconditions.checkNotNull(fullInterval, "fullInterval")) ? null : fullInterval;
    this.servers = servers;
  }

  public RichSegmentDescriptor(
      SegmentDescriptor segmentDescriptor,
      @Nullable Interval fullInterval,
      Set<DruidServerMetadata> servers
  )
  {
    super(segmentDescriptor.getInterval(), segmentDescriptor.getVersion(), segmentDescriptor.getPartitionNumber());
    this.fullInterval = fullInterval;
    this.servers = servers;
  }

  @JsonCreator
  static RichSegmentDescriptor fromJson(
      @JsonProperty("fi") @Nullable final Interval fullInterval,
      @JsonProperty("itvl") final Interval interval,
      @JsonProperty("ver") final String version,
      @JsonProperty("part") final int partitionNumber,
      @JsonProperty("servers") @Nullable final Set<DruidServerMetadata> servers
  )
  {
    return new RichSegmentDescriptor(
        fullInterval != null ? fullInterval : interval,
        interval,
        version,
        partitionNumber,
        servers == null ? ImmutableSet.of() : servers
    );
  }

  /**
   * Returns true if the location the segment is loaded is available, and false if it is not.
   */
  public boolean isLoadedOnServer()
  {
    return !CollectionUtils.isNullOrEmpty(getServers());
  }

  @JsonProperty("servers")
  public Set<DruidServerMetadata> getServers()
  {
    return servers;
  }

  public Interval getFullInterval()
  {
    return fullInterval == null ? getInterval() : fullInterval;
  }

  @JsonProperty("fi")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public Interval getFullIntervalForJson()
  {
    return fullInterval;
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
    if (!super.equals(o)) {
      return false;
    }
    RichSegmentDescriptor that = (RichSegmentDescriptor) o;
    return Objects.equals(fullInterval, that.fullInterval) && Objects.equals(servers, that.servers);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), fullInterval, servers);
  }

  @Override
  public String toString()
  {
    return "RichSegmentDescriptor{" +
           "fullInterval=" + (fullInterval == null ? getInterval() : fullInterval) +
           ", servers=" + getServers() +
           ", interval=" + getInterval() +
           ", version='" + getVersion() + '\'' +
           ", partitionNumber=" + getPartitionNumber() +
           '}';
  }
}
