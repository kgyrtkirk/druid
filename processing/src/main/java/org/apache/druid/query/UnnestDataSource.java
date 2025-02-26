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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.UnnestSegment;
import org.apache.druid.segment.VirtualColumn;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.function.Function;

/**
 * The data source for representing an unnest operation.
 * An unnest data source has the following:
 * a base data source which is to be unnested
 * the column name of the MVD which will be unnested
 * the name of the column that will hold the unnested values
 * and an allowlist serving as a filter of which values in the MVD will be unnested.
 */
public class UnnestDataSource extends ChainedDataSource<DataSource>
{
  private final VirtualColumn virtualColumn;

  @Nullable
  private final DimFilter unnestFilter;

  private UnnestDataSource(
      DataSource dataSource,
      VirtualColumn virtualColumn,
      DimFilter unnestFilter
  )
  {
    super(dataSource);
    this.virtualColumn = virtualColumn;
    this.unnestFilter = unnestFilter;
  }

  @JsonCreator
  public static UnnestDataSource create(
      @JsonProperty("base") DataSource base,
      @JsonProperty("virtualColumn") VirtualColumn virtualColumn,
      @Nullable @JsonProperty("unnestFilter") DimFilter unnestFilter

  )
  {
    return new UnnestDataSource(base, virtualColumn, unnestFilter);
  }

  @JsonProperty("virtualColumn")
  public VirtualColumn getVirtualColumn()
  {
    return virtualColumn;
  }

  @JsonProperty("unnestFilter")
  public DimFilter getUnnestFilter()
  {
    return unnestFilter;
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    return false;
  }

  @Override
  public boolean isGlobal()
  {
    return base.isGlobal();
  }

  @Override
  public boolean isConcrete()
  {
    return base.isConcrete();
  }

  @Override
  public Function<SegmentReference, SegmentReference> createSegmentMapFunction(Query query)
  {
    final Function<SegmentReference, SegmentReference> segmentMapFn = base.createSegmentMapFunction(query);
    return baseSegment -> new UnnestSegment(segmentMapFn.apply(baseSegment), virtualColumn, unnestFilter);
  }

  @Override
  public DataSource withUpdatedDataSource(DataSource newSource)
  {
    return new UnnestDataSource(newSource, virtualColumn, unnestFilter);
  }

  @Override
  public byte[] getCacheKey()
  {
    // The column being unnested would need to be part of the cache key
    // as the results are dependent on what column is being unnested.
    // Currently, it is not cacheable.
    // Future development should use the table name and column came to
    // create an appropriate cac
    return null;
  }

  @Override
  public String toString()
  {
    return "UnnestDataSource{" +
           "base=" + base +
           ", column='" + virtualColumn + '\'' +
           ", unnestFilter='" + unnestFilter + '\'' +
           '}';
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
    UnnestDataSource that = (UnnestDataSource) o;
    return base.equals(that.base) && virtualColumn.equals(that.virtualColumn) && Objects.equals(
        unnestFilter,
        that.unnestFilter
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(base, virtualColumn, unnestFilter);
  }
}


