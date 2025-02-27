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

package org.apache.druid.query.planning;

import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.JoinAlgorithm;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.RestrictedDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.TrueDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.policy.NoRestrictionPolicy;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinType;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

public class DataSourceAnalysisTest
{
  private static final List<Interval> MILLENIUM_INTERVALS = ImmutableList.of(Intervals.of("2000/3000"));
  private static final TableDataSource TABLE_FOO = new TableDataSource("foo");
  private static final TableDataSource TABLE_BAR = new TableDataSource("bar");
  private static final RestrictedDataSource RESTRICTED_FOO = RestrictedDataSource.create(
      TABLE_FOO,
      NoRestrictionPolicy.instance()
  );
  private static final LookupDataSource LOOKUP_LOOKYLOO = new LookupDataSource("lookyloo");
  private static final InlineDataSource INLINE = InlineDataSource.fromIterable(
      ImmutableList.of(new Object[0]),
      RowSignature.builder().add("column", ColumnType.STRING).build()
  );

  @Test
  public void testTable()
  {
    ScanQuery makeScanQuery = makeScanQuery(TABLE_FOO);
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery);

    Assert.assertTrue(ev.isExecutable());
    Assert.assertTrue(ev.isTableBased());
    Assert.assertEquals(TABLE_FOO, ev.getBaseDataSource());
    Assert.assertEquals(TABLE_FOO, ev.getBaseTableDataSource());
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assert.assertFalse(ev.isJoin());
    Assert.assertTrue(ev.isBaseColumn("foo"));
  }

  @Test
  public void testRestricted()
  {
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(RESTRICTED_FOO));

    Assert.assertTrue(ev.isExecutable());
    Assert.assertTrue(ev.isTableBased());
    Assert.assertEquals(TABLE_FOO, ev.getBaseDataSource());
    Assert.assertEquals(TABLE_FOO, ev.getBaseTableDataSource());
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assert.assertFalse(ev.isJoin());
    Assert.assertTrue(ev.isBaseColumn("foo"));
  }

  @Test
  public void testRestrictedInJoin()
  {
    JoinDataSource ds = join(
        RESTRICTED_FOO,
        LOOKUP_LOOKYLOO,
        "1.",
        JoinType.INNER
    );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(ds));

    Assert.assertTrue(ev.isExecutable());
    Assert.assertTrue(ev.isTableBased());
    Assert.assertEquals(TABLE_FOO, ev.getBaseDataSource());
    Assert.assertEquals(TABLE_FOO, ev.getBaseTableDataSource());
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assert.assertTrue(ev.isJoin());
    Assert.assertTrue(ev.isBaseColumn("foo"));
  }

  @Test
  public void testUnion()
  {
    final UnionDataSource unionDataSource = new UnionDataSource(ImmutableList.of(TABLE_FOO, TABLE_BAR));
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(unionDataSource));

    Assert.assertTrue(ev.isExecutable());
    Assert.assertTrue(ev.isTableBased());
    Assert.assertEquals(unionDataSource, ev.getBaseDataSource());
    Assert.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assert.assertFalse(ev.isJoin());
    Assert.assertTrue(ev.isBaseColumn("foo"));
  }

  @Test
  public void testSubQueryOnTable()
  {
    final QueryDataSource queryDataSource = makeQueryDS(TABLE_FOO);
    final ExecutionVertex ev = ExecutionVertex.of(makeGroupByQuery(queryDataSource));

    Assert.assertTrue(ev.isTableBased());
    Assert.assertTrue(ev.isExecutable());
    Assert.assertEquals(TABLE_FOO, ev.getBaseDataSource());
    Assert.assertEquals(TABLE_FOO, ev.getBaseTableDataSource());
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assert.assertFalse(ev.isJoin());
    Assert.assertTrue(ev.isBaseColumn("foo"));
  }

  @Test
  public void testQueryOnUnion()
  {
    final UnionDataSource unionDataSource = new UnionDataSource(ImmutableList.of(TABLE_FOO, TABLE_BAR));
    final QueryDataSource queryDataSource = makeQueryDS(unionDataSource);
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(queryDataSource));//queryDataSource.getAnalysis();

    Assert.assertFalse(ev.isExecutable());
    Assert.assertFalse(ev.isTableBased());
    Assert.assertEquals(queryDataSource, ev.getBaseDataSource());
    Assert.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assert.assertFalse(ev.isJoin());
    Assert.assertTrue(ev.isBaseColumn("foo"));
  }

  @Test
  public void testLookup()
  {
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(LOOKUP_LOOKYLOO));

    Assert.assertTrue(ev.isExecutable());
    Assert.assertFalse(ev.isTableBased());
    Assert.assertEquals(LOOKUP_LOOKYLOO, ev.getBaseDataSource());
    Assert.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assert.assertFalse(ev.isJoin());
    Assert.assertTrue(ev.isBaseColumn("foo"));
  }

  @Test
  public void testQueryOnLookup()
  {
    final QueryDataSource queryDataSource = makeQueryDS(LOOKUP_LOOKYLOO);
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(queryDataSource));

    Assert.assertFalse(ev.isExecutable());
    Assert.assertFalse(ev.isTableBased());
    Assert.assertEquals(queryDataSource, ev.getBaseDataSource());
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assert.assertFalse(ev.isJoin());
    Assert.assertTrue(ev.isBaseColumn("foo"));
  }

  @Test
  public void testInline()
  {
    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(INLINE));

    Assert.assertTrue(ev.isExecutable());
    Assert.assertFalse(ev.isTableBased());
    Assert.assertEquals(INLINE, ev.getBaseDataSource());
    Assert.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assert.assertFalse(ev.isJoin());
    Assert.assertTrue(ev.isBaseColumn("foo"));
  }

  @Test
  public void testJoinSimpleLeftLeaning()
  {
    // Join of a table onto a variety of simple joinable objects (lookup, inline, subquery) with a left-leaning
    // structure (no right children are joins themselves).

    final JoinDataSource joinDataSource =
        join(
            join(
                join(
                    TABLE_FOO,
                    LOOKUP_LOOKYLOO,
                    "1.",
                    JoinType.INNER
                ),
                INLINE,
                "2.",
                JoinType.LEFT
            ),
            makeQueryDS(LOOKUP_LOOKYLOO),
            "3.",
            JoinType.FULL
        );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(joinDataSource));
    final JoinDataSourceAnalysis analysis = joinDataSource.getJoinAnalysisForDataSource();

    Assert.assertFalse(ev.isExecutable());
    Assert.assertTrue(ev.isTableBased());
    Assert.assertEquals(TABLE_FOO, ev.getBaseDataSource());
    Assert.assertEquals(TABLE_FOO, ev.getBaseTableDataSource());
    Assert.assertEquals(Optional.empty(), analysis.getJoinBaseTableFilter());
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assert.assertEquals(
        ImmutableList.of(
            new PreJoinableClause((JoinDataSource) ((JoinDataSource) joinDataSource.getLeft()).getLeft()),
            new PreJoinableClause((JoinDataSource) joinDataSource.getLeft()),
            new PreJoinableClause(joinDataSource)
        ),
        analysis.getPreJoinableClauses()
    );
//    Assert.assertEquals(joinDataSource.isGlobal(), ev.isGlobal());
    Assert.assertTrue(ev.isJoin());
    Assert.assertTrue(ev.isBaseColumn("foo"));
    Assert.assertFalse(ev.isBaseColumn("1.foo"));
    Assert.assertFalse(ev.isBaseColumn("2.foo"));
    Assert.assertFalse(ev.isBaseColumn("3.foo"));
  }

  @Test
  public void testJoinSimpleLeftLeaningWithLeftFilter()
  {
    final JoinDataSource joinDataSource =
        join(
            join(
                join(
                    TABLE_FOO,
                    LOOKUP_LOOKYLOO,
                    "1.",
                    JoinType.INNER,
                    TrueDimFilter.instance()
                ),
                INLINE,
                "2.",
                JoinType.LEFT
            ),
            makeQueryDS(LOOKUP_LOOKYLOO),
            "3.",
            JoinType.FULL
        );

    final ExecutionVertex analysis = ExecutionVertex.of(makeScanQuery(joinDataSource));
    final JoinDataSourceAnalysis analysis1 = joinDataSource.getJoinAnalysisForDataSource();

    Assert.assertFalse(analysis.isExecutable());
    Assert.assertTrue(analysis.isTableBased());
    Assert.assertEquals(TABLE_FOO, analysis.getBaseDataSource());
    Assert.assertEquals(TABLE_FOO, analysis.getBaseTableDataSource());
    Assert.assertEquals(null, analysis1.getJoinBaseTableFilter().orElse(null));
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), analysis.getEffectiveQuerySegmentSpec());
    Assert.assertEquals(
        ImmutableList.of(
            new PreJoinableClause((JoinDataSource) ((JoinDataSource) joinDataSource.getLeft()).getLeft()),
            new PreJoinableClause((JoinDataSource) joinDataSource.getLeft()),
            new PreJoinableClause(joinDataSource)
        ),
        analysis1.getPreJoinableClauses()
    );
    Assert.assertTrue(analysis.isJoin());
    Assert.assertTrue(analysis.isBaseColumn("foo"));
    Assert.assertFalse(analysis.isBaseColumn("1.foo"));
    Assert.assertFalse(analysis.isBaseColumn("2.foo"));
    Assert.assertFalse(analysis.isBaseColumn("3.foo"));
  }

  @Test
  public void testJoinSimpleRightLeaning()
  {
    // Join of a table onto a variety of simple joinable objects (lookup, inline, subquery) with a right-leaning
    // structure (no left children are joins themselves).
    //
    // Note that unlike the left-leaning stack, which is fully flattened, this one will not get flattened at all.

    final JoinDataSource rightLeaningJoinStack =
        join(
            LOOKUP_LOOKYLOO,
            join(
                INLINE,
                makeQueryDS(LOOKUP_LOOKYLOO),
                "1.",
                JoinType.LEFT
            ),
            "2.",
            JoinType.FULL
        );

    final JoinDataSource joinDataSource =
        join(
            TABLE_FOO,
            rightLeaningJoinStack,
            "3.",
            JoinType.RIGHT
        );

    final ExecutionVertex analysis = ExecutionVertex.of(makeScanQuery(joinDataSource));
    final JoinDataSourceAnalysis analysis1 = joinDataSource.getJoinAnalysisForDataSource();

    Assert.assertFalse(analysis.isExecutable());
    Assert.assertTrue(analysis.isTableBased());
    Assert.assertEquals(TABLE_FOO, analysis.getBaseDataSource());
    Assert.assertEquals(TABLE_FOO, analysis.getBaseTableDataSource());
    Assert.assertEquals(Optional.empty(), analysis1.getJoinBaseTableFilter());
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), analysis.getEffectiveQuerySegmentSpec());
    Assert.assertEquals(
        ImmutableList.of(
            new PreJoinableClause(joinDataSource)
        ),
        analysis1.getPreJoinableClauses()
    );
    Assert.assertTrue(analysis.isJoin());
    Assert.assertTrue(analysis.isBaseColumn("foo"));
    Assert.assertTrue(analysis.isBaseColumn("1.foo"));
    Assert.assertTrue(analysis.isBaseColumn("2.foo"));
    Assert.assertFalse(analysis.isBaseColumn("3.foo"));
  }

  @Test
  public void testJoinSimpleRightLeaningWithLeftFilter()
  {
    final JoinDataSource rightLeaningJoinStack =
        join(
            LOOKUP_LOOKYLOO,
            join(
                INLINE,
                makeQueryDS(LOOKUP_LOOKYLOO),
                "1.",
                JoinType.LEFT
            ),
            "2.",
            JoinType.FULL
        );

    final JoinDataSource joinDataSource =
        join(
            TABLE_FOO,
            rightLeaningJoinStack,
            "3.",
            JoinType.RIGHT,
            TrueDimFilter.instance()
        );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(joinDataSource));
    final JoinDataSourceAnalysis analysis = joinDataSource.getJoinAnalysisForDataSource();

    Assert.assertFalse(ev.isExecutable());
    Assert.assertTrue(ev.isTableBased());
    Assert.assertEquals(TABLE_FOO, ev.getBaseDataSource());
    Assert.assertEquals(TABLE_FOO, ev.getBaseTableDataSource());
    Assert.assertEquals(null, analysis.getJoinBaseTableFilter().orElse(null));
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assert.assertEquals(
        ImmutableList.of(
            new PreJoinableClause(joinDataSource)
        ),
        analysis.getPreJoinableClauses()
    );
    Assert.assertTrue(ev.isJoin());
    Assert.assertTrue(ev.isBaseColumn("foo"));
    Assert.assertTrue(ev.isBaseColumn("1.foo"));
    Assert.assertTrue(ev.isBaseColumn("2.foo"));
    Assert.assertFalse(ev.isBaseColumn("3.foo"));
  }

  @Test
  public void testJoinOverTableSubquery()
  {
    final JoinDataSource joinDataSource = join(
        TABLE_FOO,
        makeQueryDS(TABLE_FOO),
        "1.",
        JoinType.INNER,
        TrueDimFilter.instance()
    );

    final ExecutionVertex analysis = ExecutionVertex.of(makeScanQuery(joinDataSource));
    final JoinDataSourceAnalysis analysis1 = joinDataSource.getJoinAnalysisForDataSource();

    Assert.assertFalse(analysis.isExecutable());
    Assert.assertTrue(analysis.isTableBased());
    Assert.assertEquals(TABLE_FOO, analysis.getBaseDataSource());
    Assert.assertEquals(null, analysis1.getJoinBaseTableFilter().orElse(null));
    Assert.assertEquals(TABLE_FOO, analysis.getBaseTableDataSource());
    Assert.assertEquals(
        ImmutableList.of(
            new PreJoinableClause(joinDataSource)
        ),
        analysis1.getPreJoinableClauses()
    );
    Assert.assertTrue(analysis.isJoin());
    Assert.assertTrue(analysis.isBaseColumn("foo"));
    Assert.assertFalse(analysis.isBaseColumn("1.foo"));
  }

  @Test
  public void testJoinTableUnionToLookup()
  {
    final UnionDataSource unionDataSource = new UnionDataSource(ImmutableList.of(TABLE_FOO, TABLE_BAR));
    final JoinDataSource joinDataSource = join(
        unionDataSource,
        LOOKUP_LOOKYLOO,
        "1.",
        JoinType.INNER
    );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(joinDataSource));
    final JoinDataSourceAnalysis analysis1 = joinDataSource.getJoinAnalysisForDataSource();

    Assert.assertTrue(ev.isExecutable());
    Assert.assertTrue(ev.isTableBased());
    Assert.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assert.assertEquals(Optional.empty(), analysis1.getJoinBaseTableFilter());
    Assert.assertEquals(unionDataSource, ev.getBaseDataSource());
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assert.assertEquals(
        ImmutableList.of(
            new PreJoinableClause(joinDataSource)
        ),
        analysis1.getPreJoinableClauses()
    );
    Assert.assertTrue(ev.isJoin());
    Assert.assertTrue(ev.isBaseColumn("foo"));
    Assert.assertFalse(ev.isBaseColumn("1.foo"));
  }

  @Test
  public void testJoinUnderTopLevelSubqueries()
  {
    final QueryDataSource queryDataSource =
        makeQueryDS(
            makeQueryDS(
                join(
                    TABLE_FOO,
                    LOOKUP_LOOKYLOO,
                    "1.",
                    JoinType.INNER,
                    TrueDimFilter.instance()
                )
            )
        );

    final ExecutionVertex analysis = ExecutionVertex.of(makeScanQuery(queryDataSource));

    Assert.assertFalse(analysis.isExecutable());
    Assert.assertFalse(analysis.isTableBased());
    Assert.assertEquals(queryDataSource, analysis.getBaseDataSource());
    Assert.assertThrows(DruidException.class, () -> analysis.getBaseTableDataSource());
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), analysis.getEffectiveQuerySegmentSpec());
    Assert.assertFalse(analysis.isJoin());
    Assert.assertTrue(analysis.isBaseColumn("foo"));
    Assert.assertTrue(analysis.isBaseColumn("1.foo"));
  }

  @Test
  public void testSubqueriesAnalysis()
  {
    final JoinDataSource joinDataSource;
    GroupByQuery query = GroupByQuery.builder()
        .setDataSource(
            makeQueryDS(
                joinDataSource = join(
                    TABLE_FOO,
                    LOOKUP_LOOKYLOO,
                    "1.",
                    JoinType.INNER,
                    TrueDimFilter.instance()
                )
            )
        )
        .setInterval(Intervals.ONLY_ETERNITY)
        .setGranularity(Granularities.ALL)
        .build();

    final ExecutionVertex ev = ExecutionVertex.of(query);
    final JoinDataSourceAnalysis analysis = joinDataSource.getJoinAnalysisForDataSource();

    Assert.assertTrue(ev.isExecutable());
    Assert.assertTrue(ev.isTableBased());
    Assert.assertTrue(ev.isExecutable());
    Assert.assertEquals(TABLE_FOO, ev.getBaseDataSource());
    Assert.assertEquals(TABLE_FOO, ev.getBaseTableDataSource());
Assert.assertEquals(null, analysis.getJoinBaseTableFilter().orElse(null));
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assert.assertEquals(
        ImmutableList.of(
            new PreJoinableClause(joinDataSource)
        ),
        analysis.getPreJoinableClauses()
    );
    Assert.assertTrue(ev.isJoin());
    Assert.assertTrue(ev.isBaseColumn("foo"));
    Assert.assertFalse(ev.isBaseColumn("1.foo"));
  }

  @Test
  public void testSubQuery3()
  {
    final QueryDataSource queryDataSource =
        makeQueryDS(
            makeQueryDS(
                makeQueryDS(
                    TABLE_FOO
                )
            )
        );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(queryDataSource));

    Assert.assertFalse(ev.isExecutable());
    Assert.assertFalse(ev.isTableBased());
    Assert.assertEquals(queryDataSource, ev.getBaseDataSource());
    Assert.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assert.assertFalse(ev.isJoin());
    Assert.assertTrue(ev.isBaseColumn("foo"));
    Assert.assertTrue(ev.isBaseColumn("1.foo"));
  }

  @Test
  public void testJoinLookupToLookup()
  {
    final JoinDataSource joinDataSource = join(
        LOOKUP_LOOKYLOO,
        LOOKUP_LOOKYLOO,
        "1.",
        JoinType.INNER
    );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(joinDataSource));
    final JoinDataSourceAnalysis analysis1 = joinDataSource.getJoinAnalysisForDataSource();

    Assert.assertTrue(ev.isExecutable());
    Assert.assertFalse(ev.isTableBased());
    Assert.assertEquals(LOOKUP_LOOKYLOO, ev.getBaseDataSource());
    Assert.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assert.assertEquals(Optional.empty(), analysis1.getJoinBaseTableFilter());
    Assert.assertEquals(
        ImmutableList.of(
            new PreJoinableClause(joinDataSource)
        ),
        analysis1.getPreJoinableClauses()
    );
    Assert.assertTrue(ev.isJoin());
    Assert.assertTrue(ev.isBaseColumn("foo"));
    Assert.assertFalse(ev.isBaseColumn("1.foo"));
  }

  @Test
  public void testJoinLookupToTable()
  {
    final JoinDataSource joinDataSource = join(
        LOOKUP_LOOKYLOO,
        TABLE_FOO,
        "1.",
        JoinType.INNER
    );

    final ExecutionVertex ev = ExecutionVertex.of(makeScanQuery(joinDataSource));
    final JoinDataSourceAnalysis analysis1 = joinDataSource.getJoinAnalysisForDataSource();

    Assert.assertFalse(ev.isExecutable());
    Assert.assertFalse(ev.isTableBased());
    Assert.assertEquals(LOOKUP_LOOKYLOO, ev.getBaseDataSource());
    Assert.assertThrows(DruidException.class, () -> ev.getBaseTableDataSource());
    Assert.assertEquals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS), ev.getEffectiveQuerySegmentSpec());
    Assert.assertEquals(Optional.empty(), analysis1.getJoinBaseTableFilter());
    Assert.assertEquals(
        ImmutableList.of(
            new PreJoinableClause(joinDataSource)
        ),
        analysis1.getPreJoinableClauses()
    );
    Assert.assertTrue(ev.isJoin());
    Assert.assertTrue(ev.isBaseColumn("foo"));
    Assert.assertFalse(ev.isBaseColumn("1.foo"));
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(DataSourceAnalysis.class)
                  .usingGetClass()
                  .withNonnullFields("baseDataSource")
                  // These fields are not necessary, because they're wholly determined by "dataSource"
                  .withIgnoredFields("baseQuery", "preJoinableClauses", "joinBaseTableFilter")
                  .verify();
  }

  /**
   * Generate a datasource that joins on a column named "x" on both sides.
   */
  private static JoinDataSource join(
      final DataSource left,
      final DataSource right,
      final String rightPrefix,
      final JoinType joinType,
      final DimFilter dimFilter
  )
  {
    return JoinDataSource.create(
        left,
        right,
        rightPrefix,
        joinClause(rightPrefix).getOriginalExpression(),
        joinType,
        dimFilter,
        ExprMacroTable.nil(),
        null,
        JoinAlgorithm.BROADCAST
    );
  }

  private static JoinDataSource join(
      final DataSource left,
      final DataSource right,
      final String rightPrefix,
      final JoinType joinType
  )
  {
    return join(left, right, rightPrefix, joinType, null);
  }

  /**
   * Generate a join clause that joins on a column named "x" on both sides.
   */
  private static JoinConditionAnalysis joinClause(
      final String rightPrefix
  )
  {
    return JoinConditionAnalysis.forExpression(
        StringUtils.format("x == \"%sx\"", rightPrefix),
        rightPrefix,
        ExprMacroTable.nil()
    );
  }

  /**
   * Generate a datasource that does a subquery on another datasource. The specific kind of query doesn't matter
   * much for the purpose of this test class, so it's always the same.
   */
  private static QueryDataSource makeQueryDS(final DataSource dataSource)
  {
    return new QueryDataSource(makeGroupByQuery(dataSource));
  }

  private static GroupByQuery makeGroupByQuery(final DataSource dataSource)
  {
    return GroupByQuery.builder()
        .setDataSource(dataSource)
        .setInterval(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS))
        .setGranularity(Granularities.ALL)
        .build();
  }

  private static ScanQuery makeScanQuery(final DataSource dataSource)
  {
    return Druids.newScanQueryBuilder()
        .dataSource(dataSource)
        .intervals(new MultipleIntervalSegmentSpec(MILLENIUM_INTERVALS))
        .build();
  }
}
