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

package org.apache.druid.benchmark.query;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(time = 1, iterations = 2)
@Measurement(time = 1, iterations = 5)
public class TopNVsGroupByBenchmark
{
  @Param({"200000"})
  private int rowsPerSegment;

  private static final Logger log = new Logger(TopNVsGroupByBenchmark.class);

  static {
    NullHandling.initializeForTests();
  }

  private SpecificSegmentsQuerySegmentWalker walker;
  private SqlEngine engine;
  private PlannerFactory plannerFactory;
  private Closer closer;

  @Setup(Level.Trial)
  public void setup()
  {
    this.closer = Closer.create();

    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();

    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());
    log.info("Starting benchmark setup using tmpDir[%s], rows[%,d].", segmentGenerator.getCacheDir(), rowsPerSegment);

    final QueryableIndex index = segmentGenerator.generate(dataSegment, schemaInfo, Granularities.NONE, rowsPerSegment);
    final QueryRunnerFactoryConglomerate conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(closer);
    final PlannerConfig plannerConfig = new PlannerConfig();

    this.walker = closer.register(SpecificSegmentsQuerySegmentWalker.createWalker(conglomerate).add(dataSegment, index));
    final DruidSchemaCatalog rootSchema =
        CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);
    engine = CalciteTests.createMockSqlEngine(walker, conglomerate);
    plannerFactory = new PlannerFactory(
        rootSchema,
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of()),
        CalciteTests.createJoinableFactoryWrapper(),
        CatalogResolver.NULL_RESOLVER,
        new AuthConfig()
    );
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    closer.close();
  }


  String groupCol = "dimSequential";

  private DimFilter filter = new AndDimFilter(
      new OrDimFilter(
          new SelectorDimFilter("dimMultivalEnumerated", "Foo", null)
      )
  );

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void groupByQuery(Blackhole blackhole)
  {
    GroupByQuery groupByQuery = GroupByQuery
        .builder()
        .setDataSource("foo")
        .setDimFilter(
            filter
        )
        .setInterval(Intervals.ETERNITY)
        .setDimensions(new DefaultDimensionSpec("dimSequential", "d1"))
        .setAggregatorSpecs(
            new CountAggregatorFactory("c"),
            new LongSumAggregatorFactory("l0", "metLongSequential"),
            new LongSumAggregatorFactory("l1", "metLongUniform")
            )
        .setGranularity(Granularities.ALL)
        .setLimit(100)
        .queryId("gby")
        .build();

    runQuery(blackhole, groupByQuery);
  }

  public static QuerySegmentSpec querySegmentSpec(final Interval... intervals)
  {
    return new MultipleIntervalSegmentSpec(Arrays.asList(intervals));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void topNQuery(Blackhole blackhole)
  {
    TopNQuery groupByQuery = new TopNQueryBuilder()
        .dataSource("foo")
        .intervals(querySegmentSpec(Filtration.eternity()))
        .metric("l0")
        .aggregators(
            new CountAggregatorFactory("c"),
            new LongSumAggregatorFactory("l0", "metLongSequential"),
            new LongSumAggregatorFactory("l1", "metLongUniform")
            )
        .dimension(new DefaultDimensionSpec("dimSequential", "d1"))
        .threshold(100)
        .granularity(Granularities.ALL)
        .queryId("topN")
        .build();

    runQuery(blackhole, groupByQuery);
  }

  private <T> void runQuery(Blackhole blackhole, BaseQuery<T> groupByQuery)
  {
    final Sequence<T> resultSequence = QueryPlus.wrap(groupByQuery).run(walker, ResponseContext.createEmpty());
    final T lastRow = resultSequence.accumulate(null, (accumulated, in) -> in);
    blackhole.consume(lastRow);
  }

}
