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

package org.apache.druid;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.aggregation.SerializablePairLongStringComplexMetricSerde;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.joda.time.Interval;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import static org.junit.Assert.assertFalse;

public class KTest
{
  public static QuerySegmentSpec querySegmentSpec(final Interval... intervals)
  {
    return new MultipleIntervalSegmentSpec(Arrays.asList(intervals));
  }

  // A
  // B
  // C
  // E
  // G
  // H
  // K
  @Test
  public void asd0()
  {
    QueryDataSource queryDataSource = extracted();

    Set<String> t = queryDataSource.getTableNames();
    assertFalse(t.isEmpty());
  }

  @Test
  public void asd()
  {
    QueryDataSource queryDataSource = extracted();

    Set<String> t = queryDataSource.getTableNames2K();
    assertFalse(t.isEmpty());
  }

  @Test
  public void asd2()
  {
    QueryDataSource queryDataSource = extracted();

    Set<String> t = queryDataSource.getTableNames2X("K");
    assertFalse(t.isEmpty());
  }

  private QueryDataSource extracted()
  {
    QueryDataSource queryDataSource = new QueryDataSource(
        GroupByQuery.builder()
            .setDataSource("wikipedia_first_last")
            .setInterval(querySegmentSpec(Intervals.ETERNITY))
            .setGranularity(Granularities.ALL)
            .setDimensions(
                new DefaultDimensionSpec(
                    "string_first_added",
                    "d0",
                    ColumnType.ofComplex(SerializablePairLongStringComplexMetricSerde.TYPE_NAME)
                )
            )
            .setContext(new HashMap<>())
            .build()
    );
    return queryDataSource;
  }

}
