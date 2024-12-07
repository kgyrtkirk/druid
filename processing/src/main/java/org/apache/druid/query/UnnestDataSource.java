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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.UnnestSegment;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * The data source for representing an unnest operation.
 * An unnest data source has the following:
 * a base data source which is to be unnested
 * the column name of the MVD which will be unnested
 * the name of the column that will hold the unnested values
 * and an allowlist serving as a filter of which values in the MVD will be unnested.
 */
public class UnnestDataSource implements DataSource
{
  private final DataSource base;
  private final VirtualColumn virtualColumn;
  @Nullable
  private final String outputName;

  @Nullable
  private final DimFilter unnestFilter;

  private UnnestDataSource(
      DataSource dataSource,
      VirtualColumn virtualColumn,
      DimFilter unnestFilter,
      @Nullable String outputName)
  {
    this.base = dataSource;
    this.virtualColumn = virtualColumn;
    this.unnestFilter = unnestFilter;
    this.outputName = outputName;
  }

  @JsonCreator
  public static UnnestDataSource create(
      @JsonProperty("base") DataSource base,
      @JsonProperty("virtualColumn") VirtualColumn virtualColumn,
      @Nullable @JsonProperty("unnestFilter") DimFilter unnestFilter,
      @Nullable @JsonProperty("outputName") String outputName

  )
  {
    return new UnnestDataSource(base, virtualColumn, unnestFilter, outputName);
  }

  // FIXME
  @Deprecated
  public static UnnestDataSource create(
      @JsonProperty("base") DataSource base,
      @JsonProperty("virtualColumn") VirtualColumn virtualColumn,
      @Nullable @JsonProperty("unnestFilter") DimFilter unnestFilter

  )
  {
    return create(base, virtualColumn, unnestFilter, null);
  }

  @JsonProperty("base")
  public DataSource getBase()
  {
    return base;
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

  @JsonInclude(Include.NON_NULL)
  @JsonProperty("outputName")
  public String getOutputName()
  {
    return outputName;
  }


  @Override
  public Set<String> getTableNames()
  {
    return base.getTableNames();
  }

  @Override
  public List<DataSource> getChildren()
  {
    return ImmutableList.of(base);
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    if (children.size() != 1) {
      throw new IAE("Expected [1] child, got [%d]", children.size());
    }

    return UnnestDataSource.create(children.get(0), virtualColumn, unnestFilter, outputName);
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
  public Function<SegmentReference, SegmentReference> createSegmentMapFunction(
      Query query,
      AtomicLong cpuTimeAccumulator
  )
  {
    final Function<SegmentReference, SegmentReference> segmentMapFn = base.createSegmentMapFunction(
        query,
        cpuTimeAccumulator
    );
    return JvmUtils.safeAccumulateThreadCpuTime(
        cpuTimeAccumulator,
        () -> baseSegment -> new UnnestSegment(segmentMapFn.apply(baseSegment), virtualColumn, unnestFilter, outputName)
    );
  }

  @Override
  public DataSource withUpdatedDataSource(DataSource newSource)
  {
    return new UnnestDataSource(newSource, virtualColumn, unnestFilter, outputName);
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
  public DataSourceAnalysis getAnalysis()
  {
    final DataSource current = this.getBase();
    return current.getAnalysis();
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


