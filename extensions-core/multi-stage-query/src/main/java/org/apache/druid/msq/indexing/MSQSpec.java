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

package org.apache.druid.msq.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.indexing.destination.TaskReportMSQDestination;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.sql.calcite.planner.ColumnMappings;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Legacy MSQSpec with a native query in it.
 *
 * Should only be used on task submission API to help ensure correct behaviour during upgrade.
 */
public class MSQSpec extends MSQSpec0
{
  private final Query<?> query;

  // jackson defaults
  public MSQSpec()
  {
    super();
    this.query = null;
  }

  public MSQSpec(
      @JsonProperty("query") Query<?> query,
      @JsonProperty("columnMappings") ColumnMappings columnMappings,
      @JsonProperty("destination") MSQDestination destination,
      @JsonProperty("assignmentStrategy") WorkerAssignmentStrategy assignmentStrategy,
      @JsonProperty("tuningConfig") MSQTuningConfig tuningConfig,
      @JsonProperty("compactionMetricSpec") List<AggregatorFactory> compactionMetricSpec1,
      @JsonProperty("queryContext") QueryContext queryContext)
  {
    this(
        query, columnMappings, destination, assignmentStrategy, tuningConfig, compactionMetricSpec1, queryContext, null
    );
  }

  @JsonCreator
  public MSQSpec(
      @JsonProperty("query") Query<?> query,
      @JsonProperty("columnMappings") ColumnMappings columnMappings,
      @JsonProperty("destination") MSQDestination destination,
      @JsonProperty("assignmentStrategy") WorkerAssignmentStrategy assignmentStrategy,
      @JsonProperty("tuningConfig") MSQTuningConfig tuningConfig,
      @JsonProperty("compactionMetricSpec") List<AggregatorFactory> compactionMetricSpec1,
      @JsonProperty("queryContext") QueryContext queryContext,
      @JsonProperty("queryDef") QueryDefinition queryDef
  )
  {
    super(columnMappings, destination, assignmentStrategy, tuningConfig, compactionMetricSpec1, queryContext, queryDef);
    this.query = Preconditions.checkNotNull(query, "query");
  }

  public static Builder builder()
  {
    return new Builder();
  }

  @JsonProperty
  public Query<?> getQuery()
  {
    return query;
  }

  public String getId()
  {
    return getContext().getString(BaseQuery.QUERY_ID);
  }

  public MSQSpec withOverriddenContext(Map<String, Object> contextOverride)
  {
    if (contextOverride == null || contextOverride.isEmpty()) {
      return this;
    } else {
      return new MSQSpec(
          query.withOverriddenContext(contextOverride),
          columnMappings,
          destination,
          assignmentStrategy,
          tuningConfig,
          compactionMetricSpec,
          queryContext.override(contextOverride)
      );
    }
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
    MSQSpec that = (MSQSpec) o;
    return Objects.equals(query, that.query)
           && Objects.equals(columnMappings, that.columnMappings)
           && Objects.equals(destination, that.destination)
           && Objects.equals(assignmentStrategy, that.assignmentStrategy)
           && Objects.equals(tuningConfig, that.tuningConfig);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(query, columnMappings, destination, assignmentStrategy, tuningConfig);
  }

  public static class Builder
  {
    private Query<?> query;
    private ColumnMappings columnMappings;
    private MSQDestination destination = TaskReportMSQDestination.instance();
    private WorkerAssignmentStrategy assignmentStrategy = WorkerAssignmentStrategy.MAX;
    private MSQTuningConfig tuningConfig;
    private List<AggregatorFactory> compactionMetrics = Collections.emptyList();
    private QueryContext queryContext;
    private QueryDefinition queryDef;

    @Deprecated
    public Builder query(Query<?> query)
    {
      this.query = query;
      return this;
    }

    public Builder queryDef(QueryDefinition queryDef)
    {
      this.queryDef = queryDef;
      return this;
    }

    public Builder columnMappings(final ColumnMappings columnMappings)
    {
      this.columnMappings = columnMappings;
      return this;
    }

    public Builder destination(final MSQDestination destination)
    {
      this.destination = destination;
      return this;
    }

    public Builder assignmentStrategy(final WorkerAssignmentStrategy assignmentStrategy)
    {
      this.assignmentStrategy = assignmentStrategy;
      return this;
    }

    public Builder tuningConfig(final MSQTuningConfig tuningConfig)
    {
      this.tuningConfig = tuningConfig;
      return this;
    }

    public Builder compactionMetrics(List<AggregatorFactory> compactionMetrics)
    {
      this.compactionMetrics=compactionMetrics;
      return this;
    }

    public MSQSpec build()
    {
      if (destination == null) {
        destination = TaskReportMSQDestination.instance();
      }
      return new MSQSpec(
          query, columnMappings, destination, assignmentStrategy, tuningConfig, compactionMetrics,
          query.context().override(queryContext),
          queryDef
      );
    }

    public Builder queryContext(QueryContext queryContext)
    {
      this.queryContext = queryContext;
      return this;
    }
  }

  public MSQSpec withQueryDef(QueryDefinition newQueryDef)
  {
    return new MSQSpec(
        query, columnMappings, destination, assignmentStrategy, tuningConfig, compactionMetricSpec, queryContext,
        newQueryDef
    );
  }
}
