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

package org.apache.druid.testing.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.AbstractQueryResourceTestClient;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class AbstractTestQueryHelper<QueryResultType extends AbstractQueryWithResults>
{
  public static final Logger LOG = new Logger(TestQueryHelper.class);

  protected final AbstractQueryResourceTestClient queryClient;
  protected final ObjectMapper jsonMapper;
  protected final String broker;
  protected final String brokerTLS;
  protected final String router;
  protected final String routerTLS;

  @Inject
  AbstractTestQueryHelper(
      ObjectMapper jsonMapper,
      AbstractQueryResourceTestClient<?> queryClient,
      IntegrationTestingConfig config
  )
  {
    this(
        jsonMapper,
        queryClient,
        config.getBrokerUrl(),
        config.getBrokerTLSUrl(),
        config.getRouterUrl(),
        config.getRouterTLSUrl()
    );
  }

  AbstractTestQueryHelper(
      ObjectMapper jsonMapper,
      AbstractQueryResourceTestClient<?> queryClient,
      String broker,
      String brokerTLS,
      String router,
      String routerTLS
  )
  {
    this.jsonMapper = jsonMapper;
    this.queryClient = queryClient;
    this.broker = broker;
    this.brokerTLS = brokerTLS;
    this.router = router;
    this.routerTLS = routerTLS;
  }

  public abstract String getQueryURL(String schemeAndHost);

  public String getCancelUrl(String schemaAndHost, String idToCancel)
  {
    return StringUtils.format("%s/%s", getQueryURL(schemaAndHost), idToCancel);
  }

  public void testQueriesFromFile(String filePath) throws Exception
  {
    testQueriesFromFile(getQueryURL(broker), filePath);
    testQueriesFromFile(getQueryURL(brokerTLS), filePath);
    testQueriesFromFile(getQueryURL(router), filePath);
    testQueriesFromFile(getQueryURL(routerTLS), filePath);
  }

  public void testQueriesFromString(String str) throws Exception
  {
    testQueriesFromString(getQueryURL(broker), str);
    if (!broker.equals(brokerTLS)) {
      testQueriesFromString(getQueryURL(brokerTLS), str);
    }
    testQueriesFromString(getQueryURL(router), str);
    if (!router.equals(routerTLS)) {
      testQueriesFromString(getQueryURL(routerTLS), str);
    }
  }

  public void testQueriesFromFile(String url, String filePath) throws Exception
  {
    LOG.info("Starting query tests for [%s]", filePath);
    List<QueryResultType> queries =
        jsonMapper.readValue(
            TestQueryHelper.class.getResourceAsStream(filePath),
            new TypeReference<>() {}
        );

    testQueries(url, queries);
  }

  public void testQueriesFromString(String url, String str) throws Exception
  {
    List<QueryResultType> queries =
        jsonMapper.readValue(
            str,
            new TypeReference<>() {}
        );
    testQueries(url, queries);
  }

  private void testQueries(String url, List<QueryResultType> queries) throws Exception
  {
    LOG.info("Testing [%d] queries from url[%s]", queries.size(), url);

    int queryIndex = 0;
    for (QueryResultType queryWithResult : queries) {
      List<Map<String, Object>> result =
          queryClient.query(url, queryWithResult.getQuery(), queryWithResult.getDescription());
      QueryResultVerifier.ResultVerificationObject resultsComparison = QueryResultVerifier.compareResults(
          result,
          queryWithResult.getExpectedResults(),
          queryWithResult.getFieldsToTest()
      );
      if (!resultsComparison.isSuccess()) {
        LOG.error(
            "Failed while executing query %s \n expectedResults: %s \n actualResults : %s",
            queryWithResult.getQuery(),
            jsonMapper.writeValueAsString(queryWithResult.getExpectedResults()),
            jsonMapper.writeValueAsString(result)
        );
        throw new ISE(
            "Results mismatch while executing the query %s.\n"
            + "Mismatch error: %s\n",
            queryWithResult.getQuery(),
            resultsComparison.getErrorMessage()
        );
      } else {
        LOG.info("Results Verified for Query[%d: %s]", queryIndex++, queryWithResult.getDescription());
      }
    }
  }

  @SuppressWarnings("unchecked")
  public long countRows(String dataSource, Interval interval, Function<String, AggregatorFactory> countAggregator)
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .aggregators(ImmutableList.of(countAggregator.apply("rows")))
                                  .granularity(Granularities.ALL)
                                  .intervals(Collections.singletonList(interval))
                                  .build();

    List<Map<String, Object>> results = queryClient.query(getQueryURL(broker), query, "Get row count");
    if (results.isEmpty()) {
      return 0;
    } else {
      Map<String, Object> map = (Map<String, Object>) results.get(0).get("result");

      Integer rowCount = (Integer) map.get("rows");
      return rowCount == null ? 0 : rowCount;
    }
  }
}
