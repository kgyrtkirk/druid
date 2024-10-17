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

package org.apache.druid.query.union;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.allocation.SingleMemoryAllocatorFactory;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FrameBasedInlineDataSource;
import org.apache.druid.query.FrameSignaturePair;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestTestHelper;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryQueryToolChestTest;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UnionQueryQueryToolChestTest
{
  @BeforeAll
  public static void setUpClass()
  {
    NullHandling.initializeForTests();
  }

  final UnionQueryQueryToolChest toolChest;

  public UnionQueryQueryToolChestTest()
  {
    toolChest = new UnionQueryQueryToolChest();
    QueryToolChestWarehouse warehouse = new MapQueryToolChestWarehouse(
        ImmutableMap.<Class<? extends Query>, QueryToolChest>builder()
            .put(ScanQuery.class, ScanQueryQueryToolChestTest.makeTestScanQueryToolChest())
            .build()
    );
    toolChest.setWarehouse(warehouse);
  }

  @Test
  public void testResultArraySignatureWithTimestampResultField()
  {
    RowSignature sig = RowSignature.builder()
        .add("a", ColumnType.STRING)
        .add("b", ColumnType.STRING)
        .build();

    TestScanQuery scan1 = new TestScanQuery("foo", sig)
        .appendRow("a", "a")
        .appendRow("a", "b");
    TestScanQuery scan2 = new TestScanQuery("bar", sig)
        .appendRow("x", "x")
        .appendRow("x", "y");

    List<Query<?>> queries = ImmutableList.of(
        scan1.query,
        scan2.query
    );

    UnionQuery query = new UnionQuery(queries);

    Assert.assertEquals(
        sig,
        toolChest.resultArraySignature(query)
    );
  }

  static class TestScanQuery
  {
    final ScanQuery query;
    final List<Object[]> results = new ArrayList<Object[]>();

    public TestScanQuery(String sourceName, RowSignature signature)
    {
      this.query = Druids.newScanQueryBuilder()
          .dataSource("bar")
          .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2000/3000"))))
          .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
          .columns(signature.getColumnNames())
          .columnTypes(signature.getColumnTypes())
          .build();
    }

    public TestScanQuery appendRow(Object... row)
    {
      results.add(row);
      return this;
    }

    public Sequence<ScanResultValue> makeResultSequence()
    {
      ScanResultValue result = new ScanResultValue(
          QueryRunnerTestHelper.SEGMENT_ID.toString(),
          query.getColumns(),
          convertResultsToListOfLists()
      );
      return Sequences.of(result);
    }

    private List<List<Object>> convertResultsToListOfLists()
    {
      List<List<Object>> resultsRows = new ArrayList<List<Object>>();
      for (Object[] objects : results) {
        resultsRows.add(Arrays.asList(objects));
      }
      return resultsRows;
    }
  }

  @Test
  public void testResultsAsArrays()
  {
    RowSignature sig = RowSignature.builder()
        .add("a", ColumnType.STRING)
        .add("b", ColumnType.STRING)
        .build();

    TestScanQuery scan1 = new TestScanQuery("foo", sig)
        .appendRow("a", "a")
        .appendRow("a", "b");
    TestScanQuery scan2 = new TestScanQuery("bar", sig)
        .appendRow("x", "x")
        .appendRow("x", "y");

    UnionQuery query = new UnionQuery(
        ImmutableList.of(
            scan1.query,
            scan2.query
        )
    );
    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.<Object[]>builder()
            .addAll(scan1.results)
            .addAll(scan2.results)
            .build(),
        toolChest.resultsAsArrays(
            query,
            Sequences.of(
                new UnionResult(scan1.makeResultSequence()),
                new UnionResult(scan2.makeResultSequence())
            )
        )
    );
  }

  @Test
  public void testResultsAsFrames()
  {
    RowSignature sig = RowSignature.builder()
        .add("a", ColumnType.STRING)
        .add("b", ColumnType.STRING)
        .build();

    TestScanQuery scan1 = new TestScanQuery("foo", sig)
        .appendRow("a", "a")
        .appendRow("a", "b");
    TestScanQuery scan2 = new TestScanQuery("bar", sig)
        .appendRow("x", "x")
        .appendRow("x", "y");

    UnionQuery query = new UnionQuery(
        ImmutableList.of(
            scan1.query,
            scan2.query
        )
    );
    List<FrameSignaturePair> frames = toolChest.resultsAsFrames(
        query,
        Sequences.of(
            new UnionResult(scan1.makeResultSequence()),
            new UnionResult(scan2.makeResultSequence())
        ),
        new SingleMemoryAllocatorFactory(HeapMemoryAllocator.unlimited()),
        true
    ).get().toList();

    Sequence<Object[]> rows = new FrameBasedInlineDataSource(frames, scan1.query.getRowSignature()).getRowsAsSequence();

    QueryToolChestTestHelper.assertArrayResultsEquals(
        ImmutableList.<Object[]>builder()
            .addAll(scan1.results)
            .addAll(scan2.results)
            .build(),
        rows
    );
  }
}
