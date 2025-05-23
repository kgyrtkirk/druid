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

package org.apache.druid.server.coordinator.duty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.indexing.IndexingTotalWorkerCapacityInfo;
import org.apache.druid.client.indexing.IndexingWorker;
import org.apache.druid.client.indexing.IndexingWorkerInfo;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.metadata.SqlSegmentsMetadataManagerTestBase;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.rpc.indexing.NoopOverlordClient;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.config.KillUnusedSegmentsConfig;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KillUnusedSegmentsTest
{
  private static final DateTime NOW = DateTimes.nowUtc();
  private static final Interval YEAR_OLD = new Interval(Period.days(1), NOW.minusDays(365));
  private static final Interval MONTH_OLD = new Interval(Period.days(1), NOW.minusDays(30));
  private static final Interval FIFTEEN_DAY_OLD = new Interval(Period.days(1), NOW.minusDays(15));
  private static final Interval DAY_OLD = new Interval(Period.days(1), NOW.minusDays(1));
  private static final Interval HOUR_OLD = new Interval(Period.days(1), NOW.minusHours(1));
  private static final Interval NEXT_DAY = new Interval(Period.days(1), NOW.plusDays(1));
  private static final Interval NEXT_MONTH = new Interval(Period.days(1), NOW.plusDays(30));

  private static final String DS1 = "DS1";
  private static final String DS2 = "DS2";
  private static final String DS3 = "DS3";

  private static final RowKey DS1_STAT_KEY = RowKey.of(Dimension.DATASOURCE, DS1);
  private static final RowKey DS2_STAT_KEY = RowKey.of(Dimension.DATASOURCE, DS2);
  private static final RowKey DS3_STAT_KEY = RowKey.of(Dimension.DATASOURCE, DS3);
  
  private static final String VERSION = "v1";

  private CoordinatorDynamicConfig.Builder dynamicConfigBuilder;
  private TestOverlordClient overlordClient;
  private KillUnusedSegmentsConfig.Builder configBuilder;
  private DruidCoordinatorRuntimeParams.Builder paramsBuilder;

  private KillUnusedSegments killDuty;

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule
      = new TestDerbyConnector.DerbyConnectorRule();

  private IndexerMetadataStorageCoordinator storageCoordinator;
  private SQLMetadataConnector connector;
  private MetadataStorageTablesConfig config;

  @Before
  public void setup()
  {
    connector = derbyConnectorRule.getConnector();
    storageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        null,
        TestHelper.JSON_MAPPER,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        connector,
        null,
        CentralizedDatasourceSchemaConfig.create()
    );

    this.config = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createSegmentTable();

    overlordClient = new TestOverlordClient();
    configBuilder = KillUnusedSegmentsConfig.builder()
        .withCleanupPeriod(Duration.standardSeconds(0))
        .withDurationToRetain(Duration.standardHours(36))
        .withMaxSegmentsToKill(10)
        .withMaxIntervalToKill(Period.ZERO)
        .withBufferPeriod(Duration.standardSeconds(1));
    dynamicConfigBuilder = CoordinatorDynamicConfig.builder()
        .withKillTaskSlotRatio(1.0);
    paramsBuilder = DruidCoordinatorRuntimeParams.builder()
                                                 .withUsedSegments(Collections.emptySet());
  }

  @Test
  public void testKillWithDefaultCoordinatorConfig()
  {
    configBuilder = KillUnusedSegmentsConfig.builder();
    dynamicConfigBuilder = CoordinatorDynamicConfig.builder();

    final DateTime sixtyDaysAgo = NOW.minusDays(60);

    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, DAY_OLD, VERSION, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, HOUR_OLD, VERSION, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, NEXT_DAY, VERSION, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, NEXT_MONTH, VERSION, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, Intervals.ETERNITY, VERSION, sixtyDaysAgo);

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(1, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(1, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, Intervals.ETERNITY);
  }

  @Test
  public void testKillWithDefaultCoordinatorConfigPlusZeroMaxIntervalToKill()
  {
    configBuilder = KillUnusedSegmentsConfig.builder().withMaxIntervalToKill(Period.ZERO);
    dynamicConfigBuilder = CoordinatorDynamicConfig.builder();

    final DateTime sixtyDaysAgo = NOW.minusDays(60);

    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, DAY_OLD, VERSION, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, HOUR_OLD, VERSION, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, NEXT_DAY, VERSION, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, NEXT_MONTH, VERSION, sixtyDaysAgo);
    createAndAddUnusedSegment(DS1, Intervals.ETERNITY, VERSION, sixtyDaysAgo);

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(1, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(1, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, Intervals.ETERNITY);
  }

  @Test
  public void testKillWithNoDatasources()
  {
    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(0, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
  }

  /**
   * Set up multiple datasources {@link #DS1} and {@link #DS2} with unused segments.
   * Then run the kill duty multiple times until all the segments are killed.
   */
  @Test
  public void testKillWithMultipleDatasources()
  {
    configBuilder.withIgnoreDurationToRetain(true).withMaxSegmentsToKill(2);

    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, DAY_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, NEXT_DAY, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, NEXT_MONTH, VERSION, NOW.minusDays(1));

    createAndAddUnusedSegment(DS2, YEAR_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS2, DAY_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS2, NEXT_DAY, VERSION, NOW.minusDays(1));

    initDuty();
    CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));
    Assert.assertEquals(2, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS2_STAT_KEY));

    validateLastKillStateAndReset(DS1, new Interval(YEAR_OLD.getStart(), MONTH_OLD.getEnd()));
    validateLastKillStateAndReset(DS2, new Interval(YEAR_OLD.getStart(), DAY_OLD.getEnd()));

    stats = runDutyAndGetStats();

    Assert.assertEquals(20, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(4, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(20, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(4, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));
    Assert.assertEquals(3, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS2_STAT_KEY));

    validateLastKillStateAndReset(DS1, new Interval(DAY_OLD.getStart(), NEXT_DAY.getEnd()));
    validateLastKillStateAndReset(DS2, NEXT_DAY);

    stats = runDutyAndGetStats();

    Assert.assertEquals(30, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(5, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(30, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(5, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));
    Assert.assertEquals(3, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS2_STAT_KEY));

    validateLastKillStateAndReset(DS1, NEXT_MONTH);
    validateLastKillStateAndReset(DS2, null);
  }

  /**
   * Set up multiple datasources {@link #DS1}, {@link #DS2} and {@link #DS3} with unused segments with 2 kill task
   * slots. Running the kill duty each time should pick at least one unique datasource in a round-robin manner.
   */
  @Test
  public void testRoundRobinKillMultipleDatasources()
  {
    configBuilder.withIgnoreDurationToRetain(true)
                 .withMaxSegmentsToKill(2);
    dynamicConfigBuilder.withMaxKillTaskSlots(2);

    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, DAY_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, NEXT_DAY, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, NEXT_MONTH, VERSION, NOW.minusDays(1));

    createAndAddUnusedSegment(DS2, YEAR_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS2, DAY_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS2, NEXT_DAY, VERSION, NOW.minusDays(1));

    createAndAddUnusedSegment(DS3, YEAR_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS3, DAY_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS3, NEXT_DAY, VERSION, NOW.minusDays(1));

    initDuty();
    CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(2, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(2, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));
    Assert.assertEquals(2, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS2_STAT_KEY));

    stats = runDutyAndGetStats();

    Assert.assertEquals(4, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(4, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(4, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS3_STAT_KEY));
    Assert.assertEquals(4, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    stats = runDutyAndGetStats();

    Assert.assertEquals(6, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(6, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(6, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(3, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS2_STAT_KEY));
    Assert.assertEquals(3, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS3_STAT_KEY));

    stats = runDutyAndGetStats();

    Assert.assertEquals(8, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(7, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(8, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(5, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));
    Assert.assertEquals(3, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS2_STAT_KEY));
  }

  /**
   * The set of datasources to kill change in consecutive runs. The kill duty should avoid selecting two
   * consecutive datasources across runs as long as there are other datasources to kill.
   */
  @Test
  public void testRoundRobinKillWhenDatasourcesChange()
  {
    configBuilder.withIgnoreDurationToRetain(true)
                 .withMaxSegmentsToKill(2);
    dynamicConfigBuilder.withMaxKillTaskSlots(1);

    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, DAY_OLD, VERSION, NOW.minusDays(1));

    initDuty();
    CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(1, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(1, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, new Interval(YEAR_OLD.getStart(), MONTH_OLD.getEnd()));

    createAndAddUnusedSegment(DS2, YEAR_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS2, DAY_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS2, NEXT_DAY, VERSION, NOW.minusDays(1));

    stats = runDutyAndGetStats();

    Assert.assertEquals(2, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(2, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS2_STAT_KEY));

    stats = runDutyAndGetStats();

    Assert.assertEquals(3, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(3, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(3, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(3, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    stats = runDutyAndGetStats();

    Assert.assertEquals(4, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(4, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(4, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(3, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS2_STAT_KEY));
  }

  @Test
  public void testKillSingleDatasourceMultipleRuns()
  {
    configBuilder.withIgnoreDurationToRetain(true)
                 .withMaxSegmentsToKill(2);
    dynamicConfigBuilder.withMaxKillTaskSlots(2);

    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, DAY_OLD, VERSION, NOW.minusDays(1));

    initDuty();
    CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(2, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(2, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    stats = runDutyAndGetStats();

    Assert.assertEquals(4, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(4, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(3, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    stats = runDutyAndGetStats();

    Assert.assertEquals(6, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(6, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(3, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

  }

  /**
   * The {@code DAY_OLD} and {@code HOUR_OLD} segments are "more recent" in terms of last updated time.
   * Even though they fall within the umbrella kill interval computed by the duty, the kill task will narrow down to
   * exclude them and clean up only segments that strictly honor the max last updated time.
   * See {@code KillUnusedSegmentsTaskTest#testKillMultipleUnusedSegmentsWithNullMaxUsedStatusLastUpdatedTime}
   * for example.
   */
  @Test
  public void testKillWithDifferentLastUpdatedTimesInWideInterval()
  {
    configBuilder.withIgnoreDurationToRetain(true)
                 .withBufferPeriod(Duration.standardDays(3));

    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, DAY_OLD, VERSION, NOW.minusDays(2));
    createAndAddUnusedSegment(DS1, HOUR_OLD, VERSION, NOW.minusDays(2));
    createAndAddUnusedSegment(DS1, NEXT_DAY, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, NEXT_MONTH, VERSION, NOW.minusDays(10));

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(4, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, new Interval(YEAR_OLD.getStart(), NEXT_MONTH.getEnd()));
  }

  /**
   * Add two older unused segments with {@code YEAR_OLD} and {@code MONTH_OLD} intervals after removing the initial set
   * of unused segments. The duty will clean up the older segments eventually, but only after it deletes rest of
   * the initial set of segments.
   */
  @Test
  public void testAddOlderSegmentsAfterInitialRun()
  {
    configBuilder.withIgnoreDurationToRetain(true)
                 .withMaxSegmentsToKill(2);

    createAndAddUnusedSegment(DS1, DAY_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, NEXT_DAY, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, NEXT_MONTH, VERSION, NOW.minusDays(1));

    initDuty();
    CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, new Interval(DAY_OLD.getStart(), NEXT_DAY.getEnd()));

    // Add two old unused segments now. These only get killed much later on when the kill
    // duty eventually round robins its way through until the latest time.
    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, NOW.minusDays(1));

    stats = runDutyAndGetStats();

    Assert.assertEquals(20, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(20, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(3, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, NEXT_MONTH);

    stats = runDutyAndGetStats();

    Assert.assertEquals(30, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(30, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(3, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, null);

    stats = runDutyAndGetStats();

    Assert.assertEquals(40, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(3, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(40, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(5, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, new Interval(YEAR_OLD.getStart(), MONTH_OLD.getEnd()));
  }

  @Test
  public void testDatasoucesAllowList()
  {
    dynamicConfigBuilder.withSpecificDataSourcesToKillUnusedSegmentsIn(ImmutableSet.of(DS2, DS3));
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS2, YEAR_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS3, MONTH_OLD, VERSION, NOW.minusDays(1));

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(0, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));
    Assert.assertEquals(1, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS2_STAT_KEY));
    Assert.assertEquals(1, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS3_STAT_KEY));

    validateLastKillStateAndReset(DS1, null);
    validateLastKillStateAndReset(DS2, YEAR_OLD);
    validateLastKillStateAndReset(DS3, MONTH_OLD);
  }

  @Test
  public void testNegativeDurationToRetain()
  {
    configBuilder.withDurationToRetain(Duration.standardHours(36).negated());

    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, DAY_OLD, VERSION, NOW.minusDays(2));
    createAndAddUnusedSegment(DS1, HOUR_OLD, VERSION, NOW.minusDays(2));
    createAndAddUnusedSegment(DS1, NEXT_DAY, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, NEXT_MONTH, VERSION, NOW.minusDays(10));

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(5, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, new Interval(YEAR_OLD.getStart(), NEXT_DAY.getEnd())
    );
  }

  @Test
  public void testIgnoreDurationToRetain()
  {
    configBuilder.withIgnoreDurationToRetain(true);

    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, DAY_OLD, VERSION, NOW.minusDays(2));
    createAndAddUnusedSegment(DS1, HOUR_OLD, VERSION, NOW.minusDays(2));
    createAndAddUnusedSegment(DS1, NEXT_DAY, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, NEXT_MONTH, VERSION, NOW.minusDays(10));

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(6, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    // All past and future unused segments should be killed
    validateLastKillStateAndReset(DS1, new Interval(YEAR_OLD.getStart(), NEXT_MONTH.getEnd()));
  }

  @Test
  public void testLowerMaxSegmentsToKill()
  {
    configBuilder.withMaxSegmentsToKill(1);

    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, DAY_OLD, VERSION, NOW.minusDays(2));

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, YEAR_OLD);
  }

  @Test
  public void testLowerMaxIntervalToKill()
  {
    configBuilder.withMaxIntervalToKill(Period.hours(1));

    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, DAY_OLD, VERSION, NOW.minusDays(2));

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, YEAR_OLD);
  }

  @Test
  public void testMaxIntervalToKillOverridesDurationToRetain()
  {
    configBuilder.withDurationToRetain(Period.hours(6).toStandardDuration())
            .withMaxIntervalToKill(Period.days(20));

    initDuty();

    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, NOW.minusDays(29));
    CoordinatorRunStats newDatasourceStats = runDutyAndGetStats();

    // For a new datasource, the duration to retain is used to determine kill interval
    Assert.assertEquals(1, newDatasourceStats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));
    validateLastKillStateAndReset(DS1, MONTH_OLD);

    // For a datasource where kill has already happened, maxIntervalToKill is used
    // if it leads to a smaller kill interval than durationToRetain
    createAndAddUnusedSegment(DS1, FIFTEEN_DAY_OLD, VERSION, NOW.minusDays(14));
    createAndAddUnusedSegment(DS1, DAY_OLD, VERSION, NOW.minusHours(2));
    CoordinatorRunStats oldDatasourceStats = runDutyAndGetStats();

    Assert.assertEquals(2, oldDatasourceStats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));
    validateLastKillStateAndReset(DS1, FIFTEEN_DAY_OLD);
  }

  @Test
  public void testDurationToRetainOverridesMaxIntervalToKill()
  {
    configBuilder.withDurationToRetain(Period.days(20).toStandardDuration())
            .withMaxIntervalToKill(Period.days(350));

    initDuty();

    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, NOW.minusDays(29));
    CoordinatorRunStats newDatasourceStats = runDutyAndGetStats();

    Assert.assertEquals(1, newDatasourceStats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));
    validateLastKillStateAndReset(DS1, YEAR_OLD);

    // For a datasource where (now - durationToRetain) < (lastKillTime(year old segment) + maxInterval)
    // Fifteen day old segment will be rejected
    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, NOW.minusDays(29));
    createAndAddUnusedSegment(DS1, FIFTEEN_DAY_OLD, VERSION, NOW.minusDays(14));
    CoordinatorRunStats oldDatasourceStats = runDutyAndGetStats();

    Assert.assertEquals(2, oldDatasourceStats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));
    validateLastKillStateAndReset(DS1, MONTH_OLD);
  }

  @Test
  public void testHigherMaxIntervalToKill()
  {
    configBuilder.withMaxIntervalToKill(Period.days(360));

    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, DAY_OLD, VERSION, NOW.minusDays(2));

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, JodaUtils.umbrellaInterval(Arrays.asList(YEAR_OLD, MONTH_OLD)));
  }

  @Test
  public void testKillDatasourceWithNoUnusedSegmentsInInitialRun()
  {
    configBuilder.withMaxSegmentsToKill(1);

    // create a datasource but no unused segments yet.
    createAndAddUsedSegment(DS1, YEAR_OLD, VERSION);

    initDuty();
    CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(0, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(0, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, DAY_OLD, VERSION, NOW.minusDays(2));

    stats = runDutyAndGetStats();

    Assert.assertEquals(20, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(20, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, YEAR_OLD);
  }

  /**
   * The kill period is honored after the first indexing run.
   */
  @Test
  public void testLargeKillPeriod()
  {
    configBuilder.withCleanupPeriod(Duration.standardHours(1));

    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, DAY_OLD, VERSION, NOW.minusDays(2));
    createAndAddUnusedSegment(DS1, HOUR_OLD, VERSION, NOW.minusDays(2));
    createAndAddUnusedSegment(DS1, NEXT_DAY, VERSION, NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, NEXT_MONTH, VERSION, NOW.minusDays(10));

    initDuty();
    CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, new Interval(YEAR_OLD.getStart(), MONTH_OLD.getEnd()));

    stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, null);
  }

  /**
   * There are multiple datasources to be killed, but the kill task slot is at capacity,
   * so the duty has to wait until the slots free up in the next cycle or so.
   */
  @Test
  public void testKillTaskSlotAtCapacity()
  {
    dynamicConfigBuilder.withKillTaskSlotRatio(0.3);
    dynamicConfigBuilder.withMaxKillTaskSlots(2);
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    createAndAddUnusedSegment(DS1, YEAR_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, MONTH_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS1, DAY_OLD, VERSION, NOW.minusDays(1));

    createAndAddUnusedSegment(DS2, YEAR_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS2, DAY_OLD, VERSION, NOW.minusDays(1));
    createAndAddUnusedSegment(DS2, NEXT_MONTH, VERSION, NOW.minusDays(1));

    createAndAddUnusedSegment(DS3, YEAR_OLD, VERSION, NOW.minusDays(1));

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(2, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(2, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));
    Assert.assertEquals(1, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS2_STAT_KEY));

    validateLastKillStateAndReset(DS1, new Interval(YEAR_OLD.getStart(), MONTH_OLD.getEnd()));
    validateLastKillStateAndReset(DS2, YEAR_OLD);
    validateLastKillStateAndReset(DS3, null);
  }

  @Test
  public void testKillWithOverlordTaskSlotsFull()
  {
    dynamicConfigBuilder.withKillTaskSlotRatio(0.10);
    dynamicConfigBuilder.withMaxKillTaskSlots(10);
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    overlordClient = new TestOverlordClient(1, 5);

    overlordClient.addTask(DS1);

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(0, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(0, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(0, stats.get(Stats.Kill.MAX_SLOTS));
  }

  @Test
  public void testKillWithOverlordTaskSlotAvailable()
  {
    dynamicConfigBuilder.withKillTaskSlotRatio(1.0);
    dynamicConfigBuilder.withMaxKillTaskSlots(3);
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    overlordClient = new TestOverlordClient(3, 10);
    overlordClient.addTask(DS1);
    overlordClient.addTask(DS1);

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(1, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(0, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(3, stats.get(Stats.Kill.MAX_SLOTS));
  }

  @Test
  public void testDefaultKillTaskSlotStats()
  {
    dynamicConfigBuilder = CoordinatorDynamicConfig.builder();
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(1, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(0, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(1, stats.get(Stats.Kill.MAX_SLOTS));
  }

  @Test
  public void testKillTaskSlotStats1()
  {
    dynamicConfigBuilder.withKillTaskSlotRatio(1.0);
    dynamicConfigBuilder.withMaxKillTaskSlots(Integer.MAX_VALUE);
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(0, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
  }

  @Test
  public void testKillTaskSlotStats2()
  {
    dynamicConfigBuilder.withKillTaskSlotRatio(0.0);
    dynamicConfigBuilder.withMaxKillTaskSlots(Integer.MAX_VALUE);
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(0, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(0, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(0, stats.get(Stats.Kill.MAX_SLOTS));
  }

  @Test
  public void testKillTaskSlotStats3()
  {
    dynamicConfigBuilder.withKillTaskSlotRatio(1.0);
    dynamicConfigBuilder.withMaxKillTaskSlots(0);
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(0, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(0, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(0, stats.get(Stats.Kill.MAX_SLOTS));
  }

  @Test
  public void testKillTaskSlotStats4()
  {
    dynamicConfigBuilder.withKillTaskSlotRatio(0.1);
    dynamicConfigBuilder.withMaxKillTaskSlots(3);
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(1, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(0, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(1, stats.get(Stats.Kill.MAX_SLOTS));
  }

  @Test
  public void testKillTaskSlotStats5()
  {
    dynamicConfigBuilder.withKillTaskSlotRatio(0.3);
    dynamicConfigBuilder.withMaxKillTaskSlots(2);
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(2, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(0, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(2, stats.get(Stats.Kill.MAX_SLOTS));
  }

  @Test
  public void testKillFirstHalfEternitySegment()
  {
    configBuilder.withIgnoreDurationToRetain(true);

    final Interval firstHalfEternity = new Interval(DateTimes.MIN, DateTimes.of("2024"));
    createAndAddUnusedSegment(DS1, firstHalfEternity, VERSION, NOW.minusDays(60));

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, firstHalfEternity);
  }

  @Test
  public void testKillEternitySegment()
  {
    configBuilder.withIgnoreDurationToRetain(true);

    createAndAddUnusedSegment(DS1, Intervals.ETERNITY, VERSION, NOW.minusDays(60));

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, Intervals.ETERNITY);
  }

  @Test
  public void testKillSecondHalfEternitySegment()
  {
    configBuilder.withIgnoreDurationToRetain(true);
    final Interval secondHalfEternity = new Interval(DateTimes.of("1970"), DateTimes.MAX);

    createAndAddUnusedSegment(DS1, secondHalfEternity, VERSION, NOW.minusDays(60));

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, secondHalfEternity);
  }

  @Test
  public void testKillLargeIntervalSegments()
  {
    final Interval largeTimeRange1 = Intervals.of("1990-01-01T00Z/19940-01-01T00Z");
    final Interval largeTimeRange2 = Intervals.of("-19940-01-01T00Z/1970-01-01T00Z");

    createAndAddUnusedSegment(DS1, largeTimeRange1, VERSION, NOW.minusDays(60));
    createAndAddUnusedSegment(DS1, largeTimeRange2, VERSION, NOW.minusDays(60));

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(2, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, new Interval(largeTimeRange2.getStart(), largeTimeRange1.getEnd()));
  }

  @Test
  public void testKillMultipleSegmentsInSameInterval()
  {
    configBuilder.withIgnoreDurationToRetain(true);

    createAndAddUnusedSegment(DS1, YEAR_OLD, "v1", NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, YEAR_OLD, "v2", NOW.minusDays(10));
    createAndAddUnusedSegment(DS1, YEAR_OLD, "v3", NOW.minusDays(10));

    initDuty();
    final CoordinatorRunStats stats = runDutyAndGetStats();

    Assert.assertEquals(10, stats.get(Stats.Kill.AVAILABLE_SLOTS));
    Assert.assertEquals(1, stats.get(Stats.Kill.SUBMITTED_TASKS));
    Assert.assertEquals(10, stats.get(Stats.Kill.MAX_SLOTS));
    Assert.assertEquals(3, stats.get(Stats.Kill.ELIGIBLE_UNUSED_SEGMENTS, DS1_STAT_KEY));

    validateLastKillStateAndReset(DS1, YEAR_OLD);
  }

  @Test
  public void testLimitToPeriod_empty()
  {
    Assert.assertEquals(
        Collections.emptyList(),
        KillUnusedSegments.limitToPeriod(Collections.emptyList(), Period.ZERO)
    );
  }

  @Test
  public void testLimitToPeriod_zeroPeriod()
  {
    Assert.assertEquals(
        ImmutableList.of(DAY_OLD, YEAR_OLD, MONTH_OLD),
        KillUnusedSegments.limitToPeriod(ImmutableList.of(DAY_OLD, YEAR_OLD, MONTH_OLD), Period.ZERO)
    );
  }

  @Test
  public void testLimitToPeriod_oneSecondPeriod()
  {
    Assert.assertEquals(
        ImmutableList.of(YEAR_OLD),
        KillUnusedSegments.limitToPeriod(ImmutableList.of(DAY_OLD, YEAR_OLD, MONTH_OLD), Period.seconds(1))
    );
  }

  @Test
  public void testLimitToPeriod_360DayPeriod()
  {
    Assert.assertEquals(
        ImmutableList.of(YEAR_OLD, MONTH_OLD),
        KillUnusedSegments.limitToPeriod(ImmutableList.of(DAY_OLD, YEAR_OLD, MONTH_OLD), Period.days(360))
    );
  }

  @Test
  public void testLimitToPeriod_1YearPeriod()
  {
    Assert.assertEquals(
        ImmutableList.of(DAY_OLD, YEAR_OLD, MONTH_OLD),
        KillUnusedSegments.limitToPeriod(ImmutableList.of(DAY_OLD, YEAR_OLD, MONTH_OLD), Period.years(1))
    );
  }

  private void validateLastKillStateAndReset(final String dataSource, @Nullable final Interval expectedKillInterval)
  {
    final Interval observedLastKillInterval = overlordClient.getLastKillInterval(dataSource);
    final String observedLastKillTaskId = overlordClient.getLastKillTaskId(dataSource);

    Assert.assertEquals(expectedKillInterval, observedLastKillInterval);

    String expectedKillTaskId = null;
    if (expectedKillInterval != null) {
      expectedKillTaskId = TestOverlordClient.getTaskId(
          KillUnusedSegments.TASK_ID_PREFIX,
          dataSource,
          expectedKillInterval
      );
    }

    Assert.assertEquals(expectedKillTaskId, observedLastKillTaskId);

    // Clear the state after validation
    overlordClient.deleteLastKillTaskId(dataSource);
    overlordClient.deleteLastKillInterval(dataSource);
  }

  private DataSegment createAndAddUsedSegment(final String dataSource, final Interval interval, final String version)
  {
    final DataSegment segment = createSegment(dataSource, interval, version);
    SqlSegmentsMetadataManagerTestBase.publishSegment(connector, config, TestHelper.makeJsonMapper(), segment);
    return segment;
  }

  private void createAndAddUnusedSegment(
      final String dataSource,
      final Interval interval,
      final String version,
      final DateTime lastUpdatedTime
  )
  {
    final DataSegment segment = createAndAddUsedSegment(dataSource, interval, version);
    int numUpdatedSegments = SqlSegmentsMetadataManagerTestBase.markSegmentsAsUnused(
        Set.of(segment.getId()),
        derbyConnectorRule.getConnector(),
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        TestHelper.JSON_MAPPER,
        lastUpdatedTime
    );
    Assert.assertEquals(1, numUpdatedSegments);
  }

  private DataSegment createSegment(final String dataSource, final Interval interval, final String version)
  {
    return new DataSegment(
        dataSource,
        interval,
        version,
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        1,
        0
    );
  }

  private void initDuty()
  {
    killDuty = new KillUnusedSegments(storageCoordinator, overlordClient, configBuilder.build());
  }

  private CoordinatorRunStats runDutyAndGetStats()
  {
    paramsBuilder.withDynamicConfigs(dynamicConfigBuilder.build());
    final DruidCoordinatorRuntimeParams params = killDuty.run(paramsBuilder.build());
    return params.getCoordinatorStats();
  }

  /**
   * Simulates an Overlord with a configurable list of kill tasks. It also helps verify the calls made by the kill duty.
   */
  private static class TestOverlordClient extends NoopOverlordClient
  {
    private final List<TaskStatusPlus> taskStatuses = new ArrayList<>();

    private final Map<String, Interval> observedDatasourceToLastKillInterval = new HashMap<>();
    private final Map<String, String> observedDatasourceToLastKillTaskId = new HashMap<>();
    private final IndexingTotalWorkerCapacityInfo capcityInfo;
    private int taskIdSuffix = 0;

    TestOverlordClient()
    {
      capcityInfo = new IndexingTotalWorkerCapacityInfo(5, 10);
    }

    TestOverlordClient(final int currentClusterCapacity, final int maxWorkerCapacity)
    {
      capcityInfo = new IndexingTotalWorkerCapacityInfo(currentClusterCapacity, maxWorkerCapacity);
    }

    static String getTaskId(final String idPrefix, final String dataSource, final Interval interval)
    {
      return idPrefix + "-" + dataSource + "-" + interval;
    }

    void addTask(final String datasource)
    {
      taskStatuses.add(
          new TaskStatusPlus(
              KillUnusedSegments.TASK_ID_PREFIX + "__" + datasource + "__" + taskIdSuffix++,
              null,
              KillUnusedSegments.KILL_TASK_TYPE,
              DateTimes.EPOCH,
              DateTimes.EPOCH,
              TaskState.RUNNING,
              RunnerTaskState.RUNNING,
              100L,
              TaskLocation.unknown(),
              datasource,
              null
          )
      );
    }

    @Override
    public ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
        @Nullable String state,
        @Nullable String dataSource,
        @Nullable Integer maxCompletedTasks
    )
    {
      return Futures.immediateFuture(
          CloseableIterators.wrap(taskStatuses.iterator(), null)
      );
    }

    @Override
    public ListenableFuture<String> runKillTask(
        String idPrefix,
        String dataSource,
        Interval interval,
        @Nullable List<String> versions,
        @Nullable Integer maxSegmentsToKill,
        @Nullable DateTime maxUsedStatusLastUpdatedTime
    )
    {
      final String taskId = getTaskId(idPrefix, dataSource, interval);
      observedDatasourceToLastKillInterval.put(dataSource, interval);
      observedDatasourceToLastKillTaskId.put(dataSource, taskId);
      return Futures.immediateFuture(taskId);
    }

    @Override
    public ListenableFuture<IndexingTotalWorkerCapacityInfo> getTotalWorkerCapacity()
    {
      return Futures.immediateFuture(capcityInfo);
    }

    @Override
    public ListenableFuture<List<IndexingWorkerInfo>> getWorkers()
    {
      return Futures.immediateFuture(
          ImmutableList.of(
              new IndexingWorkerInfo(
                  new IndexingWorker("http", "localhost", "1.2.3.4", 3, "2"),
                  0,
                  Collections.emptySet(),
                  Collections.emptyList(),
                  DateTimes.of("2000"),
                  null
              )
          )
      );
    }

    Interval getLastKillInterval(final String dataSource)
    {
      return observedDatasourceToLastKillInterval.get(dataSource);
    }

    void deleteLastKillInterval(final String dataSource)
    {
      observedDatasourceToLastKillInterval.remove(dataSource);
    }

    String getLastKillTaskId(final String dataSource)
    {
      final String lastKillTaskId = observedDatasourceToLastKillTaskId.get(dataSource);
      observedDatasourceToLastKillTaskId.remove(dataSource);
      return lastKillTaskId;
    }

    void deleteLastKillTaskId(final String dataSource)
    {
      observedDatasourceToLastKillTaskId.remove(dataSource);
    }
  }
}
