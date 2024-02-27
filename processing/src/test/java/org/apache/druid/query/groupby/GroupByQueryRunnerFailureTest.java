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

package org.apache.druid.query.groupby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.collections.CloseableDefaultBlockingPool;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class GroupByQueryRunnerFailureTest
{
  private static final DruidProcessingConfig DEFAULT_PROCESSING_CONFIG = new DruidProcessingConfig()
  {

    @Override
    public String getFormatString()
    {
      return null;
    }

    @Override
    public int intermediateComputeSizeBytes()
    {
      return 10 * 1024 * 1024;
    }

    @Override
    public int getNumMergeBuffers()
    {
      return 1;
    }

    @Override
    public int getNumThreads()
    {
      return 2;
    }
  };

  private static GroupByQueryRunnerFactory makeQueryRunnerFactory(
      final ObjectMapper mapper,
      final GroupByQueryConfig config
  )
  {
    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final GroupingEngine groupingEngine = new GroupingEngine(
        DEFAULT_PROCESSING_CONFIG,
        configSupplier,
        BUFFER_POOL,
        MERGE_BUFFER_POOL,
        TestHelper.makeJsonMapper(),
        mapper,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(groupingEngine);
    return new GroupByQueryRunnerFactory(groupingEngine, toolChest);
  }

  private static final CloseableStupidPool<ByteBuffer> BUFFER_POOL = new CloseableStupidPool<>(
      "GroupByQueryEngine-bufferPool",
      () -> ByteBuffer.allocate(DEFAULT_PROCESSING_CONFIG.intermediateComputeSizeBytes())
  );
  private static final CloseableDefaultBlockingPool<ByteBuffer> MERGE_BUFFER_POOL = new CloseableDefaultBlockingPool<>(
      () -> ByteBuffer.allocate(DEFAULT_PROCESSING_CONFIG.intermediateComputeSizeBytes()),
      DEFAULT_PROCESSING_CONFIG.getNumMergeBuffers()
  );

  private static final GroupByQueryRunnerFactory FACTORY = makeQueryRunnerFactory(
      GroupByQueryRunnerTest.DEFAULT_MAPPER,
      new GroupByQueryConfig()
      {
      }
  );

  private QueryRunner<ResultRow> runner;

  @AfterAll
  public static void teardownClass()
  {
    BUFFER_POOL.close();
    MERGE_BUFFER_POOL.close();
  }

  public static Collection<Object[]> constructorFeeder()
  {
    final List<Object[]> args = new ArrayList<>();
    for (QueryRunner<ResultRow> runner : QueryRunnerTestHelper.makeQueryRunners(FACTORY)) {
      args.add(new Object[]{runner});
    }
    return args;
  }

  public void initGroupByQueryRunnerFailureTest(QueryRunner<ResultRow> runner)
  {
    this.runner = FACTORY.mergeRunners(Execs.directExecutor(), ImmutableList.of(runner));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testNotEnoughMergeBuffersOnQueryable(QueryRunner<ResultRow> runner)
  {
    Throwable exception = assertThrows(QueryTimeoutException.class, () -> {
      initGroupByQueryRunnerFailureTest(runner);

      final GroupByQuery query = GroupByQuery
          .builder()
          .setDataSource(
              new QueryDataSource(
                  GroupByQuery.builder()
                      .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
                      .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
                      .setGranularity(Granularities.ALL)
                      .setDimensions(new DefaultDimensionSpec("quality", "alias"))
                      .setAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.ROWS_COUNT))
                      .build()
              )
          )
          .setGranularity(Granularities.ALL)
          .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
          .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
          .setContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 500))
          .build();

      GroupByQueryRunnerTestHelper.runQuery(FACTORY, runner, query);
    });
    assertTrue(exception.getMessage().contains("Cannot acquire enough merge buffers"));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testResourceLimitExceededOnBroker(QueryRunner<ResultRow> runner)
  {
    assertThrows(ResourceLimitExceededException.class, () -> {
      initGroupByQueryRunnerFailureTest(runner);

      final GroupByQuery query = GroupByQuery
          .builder()
          .setDataSource(
              new QueryDataSource(
                  GroupByQuery.builder()
                      .setDataSource(
                          GroupByQuery.builder()
                              .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
                              .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
                              .setGranularity(Granularities.ALL)
                              .setDimensions(
                                  new DefaultDimensionSpec("quality", "alias"),
                                  new DefaultDimensionSpec("market", null)
                              )
                              .setAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.ROWS_COUNT))
                              .build()
                      )
                      .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
                      .setGranularity(Granularities.ALL)
                      .setDimensions(new DefaultDimensionSpec("quality", "alias"))
                      .setAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.ROWS_COUNT))
                      .build()
              )
          )
          .setGranularity(Granularities.ALL)
          .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
          .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
          .setContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 500))
          .build();

      GroupByQueryRunnerTestHelper.runQuery(FACTORY, runner, query);
    });
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testInsufficientResourcesOnBroker(QueryRunner<ResultRow> runner)
  {
    Throwable exception = assertThrows(QueryCapacityExceededException.class, () -> {
      initGroupByQueryRunnerFailureTest(runner);
      final GroupByQuery query = GroupByQuery
          .builder()
          .setDataSource(
              new QueryDataSource(
                  GroupByQuery.builder()
                      .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
                      .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
                      .setGranularity(Granularities.ALL)
                      .setDimensions(new DefaultDimensionSpec("quality", "alias"))
                      .setAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.ROWS_COUNT))
                      .build()
              )
          )
          .setGranularity(Granularities.ALL)
          .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
          .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
          .setContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 500))
          .build();

      List<ReferenceCountingResourceHolder<ByteBuffer>> holder = null;
      try {
        holder = MERGE_BUFFER_POOL.takeBatch(1, 10);
        expectedException.expect(QueryCapacityExceededException.class);
        expectedException.expectMessage("Cannot acquire 1 merge buffers. Try again after current running queries are finished.");
        GroupByQueryRunnerTestHelper.runQuery(FACTORY, runner, query);
      }
      finally {
        if (holder != null) {
          holder.forEach(ReferenceCountingResourceHolder::close);
        }
      }
    });
    assertTrue(exception.getMessage().contains("Cannot acquire 1 merge buffers. Try again after current running queries are finished."));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testTimeoutExceptionOnQueryable(QueryRunner<ResultRow> runner)
  {
    assertThrows(QueryTimeoutException.class, () -> {
      initGroupByQueryRunnerFailureTest(runner);

      final GroupByQuery query = GroupByQuery
          .builder()
          .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
          .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
          .setDimensions(new DefaultDimensionSpec("quality", "alias"))
          .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"))
          .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
          .overrideContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 1))
          .build();

      GroupByQueryRunnerFactory factory = makeQueryRunnerFactory(
          GroupByQueryRunnerTest.DEFAULT_MAPPER,
          new GroupByQueryConfig()
          {

            @Override
            public boolean isSingleThreaded()
            {
              return true;
            }
          }
      );
      QueryRunner<ResultRow> mockRunner = (queryPlus, responseContext) -> {
        try {
          Thread.sleep(100);
        }
        catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return Sequences.empty();
      };

      QueryRunner<ResultRow> mergeRunners = factory.mergeRunners(Execs.directExecutor(), ImmutableList.of(runner, mockRunner));
      GroupByQueryRunnerTestHelper.runQuery(factory, mergeRunners, query);
    });
  }
}
