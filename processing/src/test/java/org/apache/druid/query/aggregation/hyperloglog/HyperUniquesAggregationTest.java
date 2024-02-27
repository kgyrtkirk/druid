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

package org.apache.druid.query.aggregation.hyperloglog;

import org.apache.druid.jackson.AggregatorsModule;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class HyperUniquesAggregationTest
{
  @TempDir
  public File tempFolder;

  private GroupByQueryConfig config;

  public void initHyperUniquesAggregationTest(GroupByQueryConfig config)
  {
    this.config = config;
  }

  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[]{config});
    }
    return constructors;
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testIngestAndQuery(GroupByQueryConfig config) throws Exception
  {
    initHyperUniquesAggregationTest(config);
    try (
        final AggregationTestHelper helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
            Collections.singletonList(new AggregatorsModule()),
            config,
            tempFolder
        )
    ) {

      String metricSpec = "[{"
                          + "\"type\": \"hyperUnique\","
                          + "\"name\": \"index_hll\","
                          + "\"fieldName\": \"market\""
                          + "}]";

      String parseSpec = "{"
                         + "\"type\" : \"string\","
                         + "\"parseSpec\" : {"
                         + "    \"format\" : \"tsv\","
                         + "    \"timestampSpec\" : {"
                         + "        \"column\" : \"timestamp\","
                         + "        \"format\" : \"auto\""
                         + "},"
                         + "    \"dimensionsSpec\" : {"
                         + "        \"dimensions\": [],"
                         + "        \"dimensionExclusions\" : [],"
                         + "        \"spatialDimensions\" : []"
                         + "    },"
                         + "    \"columns\": [\"timestamp\", \"market\", \"quality\", \"placement\", \"placementish\", \"index\"]"
                         + "  }"
                         + "}";

      String query = "{"
                     + "\"queryType\": \"groupBy\","
                     + "\"dataSource\": \"test_datasource\","
                     + "\"granularity\": \"ALL\","
                     + "\"dimensions\": [],"
                     + "\"aggregations\": ["
                     + "  { \"type\": \"hyperUnique\", \"name\": \"index_hll\", \"fieldName\": \"index_hll\" }"
                     + "],"
                     + "\"postAggregations\": ["
                     + "  { \"type\": \"hyperUniqueCardinality\", \"name\": \"index_unique_count\", \"fieldName\": \"index_hll\" }"
                     + "],"
                     + "\"intervals\": [ \"1970/2050\" ]"
                     + "}";

      Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
          new File(this.getClass().getClassLoader().getResource("druid.sample.tsv").getFile()),
          parseSpec,
          metricSpec,
          0,
          Granularities.NONE,
          50000,
          query
      );

      final ResultRow resultRow = seq.toList().get(0);
      Assertions.assertEquals(3.0, ((Number) resultRow.get(0)).floatValue(), 0.1, "index_hll");
      Assertions.assertEquals(3.0, ((Number) resultRow.get(1)).floatValue(), 0.1, "index_unique_count");
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testIngestAndQueryPrecomputedHll(GroupByQueryConfig config) throws Exception
  {
    initHyperUniquesAggregationTest(config);
    try (
        final AggregationTestHelper helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
            Collections.singletonList(new AggregatorsModule()),
            config,
            tempFolder
        )
    ) {

      String metricSpec = "[{"
                          + "\"type\": \"hyperUnique\","
                          + "\"name\": \"index_hll\","
                          + "\"fieldName\": \"preComputedHll\","
                          + "\"isInputHyperUnique\": true"
                          + "}]";

      String parseSpec = "{"
                         + "\"type\" : \"string\","
                         + "\"parseSpec\" : {"
                         + "    \"format\" : \"tsv\","
                         + "    \"timestampSpec\" : {"
                         + "        \"column\" : \"timestamp\","
                         + "        \"format\" : \"auto\""
                         + "},"
                         + "    \"dimensionsSpec\" : {"
                         + "        \"dimensions\": [],"
                         + "        \"dimensionExclusions\" : [],"
                         + "        \"spatialDimensions\" : []"
                         + "    },"
                         + "    \"columns\": [\"timestamp\", \"market\", \"preComputedHll\"]"
                         + "  }"
                         + "}";

      String query = "{"
                     + "\"queryType\": \"groupBy\","
                     + "\"dataSource\": \"test_datasource\","
                     + "\"granularity\": \"ALL\","
                     + "\"dimensions\": [],"
                     + "\"aggregations\": ["
                     + "  { \"type\": \"hyperUnique\", \"name\": \"index_hll\", \"fieldName\": \"index_hll\" }"
                     + "],"
                     + "\"postAggregations\": ["
                     + "  { \"type\": \"hyperUniqueCardinality\", \"name\": \"index_unique_count\", \"fieldName\": \"index_hll\" }"
                     + "],"
                     + "\"intervals\": [ \"1970/2050\" ]"
                     + "}";

      Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
          new File(this.getClass().getClassLoader().getResource("druid.hll.sample.tsv").getFile()),
          parseSpec,
          metricSpec,
          0,
          Granularities.DAY,
          50000,
          query
      );

      final ResultRow resultRow = seq.toList().get(0);
      Assertions.assertEquals(4.0, ((Number) resultRow.get(0)).floatValue(), 0.1, "index_hll");
      Assertions.assertEquals(4.0, ((Number) resultRow.get(1)).floatValue(), 0.1, "index_unique_count");
    }
  }
}
