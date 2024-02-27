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

package org.apache.druid.segment.join;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.CoreMatchers;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HashJoinSegmentTest extends InitializedNullHandlingTest
{
  @TempDir
  public File temporaryFolder;

  private QueryableIndexSegment baseSegment;
  private ReferenceCountingSegment referencedSegment;
  private HashJoinSegment hashJoinSegment;

  private int allReferencesAcquireCount;
  private int allReferencesCloseCount;
  private int referencedSegmentAcquireCount;
  private int referencedSegmentClosedCount;
  private int indexedTableJoinableReferenceAcquireCount;
  private int indexedTableJoinableReferenceCloseCount;
  private boolean j0Closed;
  private boolean j1Closed;

  @BeforeEach
  public void setUp() throws IOException
  {
    allReferencesAcquireCount = 0;
    allReferencesCloseCount = 0;
    referencedSegmentAcquireCount = 0;
    referencedSegmentClosedCount = 0;
    indexedTableJoinableReferenceAcquireCount = 0;
    indexedTableJoinableReferenceCloseCount = 0;
    j0Closed = false;
    j1Closed = false;

    baseSegment = new QueryableIndexSegment(
        JoinTestHelper.createFactIndexBuilder(newFolder(temporaryFolder, "junit")).buildMMappedIndex(),
        SegmentId.dummy("facts")
    );

    List<JoinableClause> joinableClauses = ImmutableList.of(
        new JoinableClause(
            "j0.",
            new IndexedTableJoinable(JoinTestHelper.createCountriesIndexedTable())
            {
              @Override
              public Optional<Closeable> acquireReferences()
              {
                if (!j0Closed) {
                  indexedTableJoinableReferenceAcquireCount++;
                  Closer closer = Closer.create();
                  closer.register(() -> indexedTableJoinableReferenceCloseCount++);
                  return Optional.of(closer);
                }
                return Optional.empty();
              }
            },
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression("1", "j0.", ExprMacroTable.nil())
        ),
        new JoinableClause(
            "j1.",
            new IndexedTableJoinable(JoinTestHelper.createRegionsIndexedTable())
            {
              @Override
              public Optional<Closeable> acquireReferences()
              {
                if (!j1Closed) {
                  indexedTableJoinableReferenceAcquireCount++;
                  Closer closer = Closer.create();
                  closer.register(() -> indexedTableJoinableReferenceCloseCount++);
                  return Optional.of(closer);
                }
                return Optional.empty();
              }
            },
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression("1", "j1.", ExprMacroTable.nil())
        )
    );

    referencedSegment = ReferenceCountingSegment.wrapRootGenerationSegment(baseSegment);
    SegmentReference testWrapper = new SegmentReference()
    {
      @Override
      public Optional<Closeable> acquireReferences()
      {
        Closer closer = Closer.create();
        return referencedSegment.acquireReferences().map(closeable -> {
          referencedSegmentAcquireCount++;
          closer.register(closeable);
          closer.register(() -> referencedSegmentClosedCount++);
          return closer;
        });
      }

      @Override
      public SegmentId getId()
      {
        return referencedSegment.getId();
      }

      @Override
      public Interval getDataInterval()
      {
        return referencedSegment.getDataInterval();
      }

      @Nullable
      @Override
      public QueryableIndex asQueryableIndex()
      {
        return referencedSegment.asQueryableIndex();
      }

      @Override
      public StorageAdapter asStorageAdapter()
      {
        return referencedSegment.asStorageAdapter();
      }

      @Override
      public void close()
      {
        referencedSegment.close();
      }
    };
    hashJoinSegment = new HashJoinSegment(
        testWrapper,
        null,
        joinableClauses,
        null
    )
    {
      @Override
      public Optional<Closeable> acquireReferences()
      {
        Closer closer = Closer.create();
        return super.acquireReferences().map(closeable -> {
          allReferencesAcquireCount++;
          closer.register(closeable);
          closer.register(() -> allReferencesCloseCount++);
          return closer;
        });
      }
    };
  }

  @Test
  public void test_constructor_noClauses()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {

      List<JoinableClause> joinableClauses = ImmutableList.of();

      final HashJoinSegment ignored = new HashJoinSegment(
          ReferenceCountingSegment.wrapRootGenerationSegment(baseSegment),
          null,
          joinableClauses,
          null
      );
    });
    assertTrue(exception.getMessage().contains("'clauses' and 'baseFilter' are both empty, no need to create HashJoinSegment"));
  }

  @Test
  public void test_getId()
  {
    Assertions.assertEquals(baseSegment.getId(), hashJoinSegment.getId());
  }

  @Test
  public void test_getDataInterval()
  {
    Assertions.assertEquals(baseSegment.getDataInterval(), hashJoinSegment.getDataInterval());
  }

  @Test
  public void test_asQueryableIndex()
  {
    Assertions.assertNull(hashJoinSegment.asQueryableIndex());
  }

  @Test
  public void test_asStorageAdapter()
  {
    assertThat(
        hashJoinSegment.asStorageAdapter(),
        CoreMatchers.instanceOf(HashJoinSegmentStorageAdapter.class)
    );
  }

  @Test
  public void testJoinableClausesAreClosedWhenReferencesUsed() throws IOException
  {
    Assertions.assertFalse(referencedSegment.isClosed());

    Optional<Closeable> maybeCloseable = hashJoinSegment.acquireReferences();
    Assertions.assertTrue(maybeCloseable.isPresent());

    Assertions.assertEquals(1, referencedSegmentAcquireCount);
    Assertions.assertEquals(2, indexedTableJoinableReferenceAcquireCount);
    Assertions.assertEquals(1, allReferencesAcquireCount);
    Assertions.assertEquals(0, referencedSegmentClosedCount);
    Assertions.assertEquals(0, indexedTableJoinableReferenceCloseCount);
    Assertions.assertEquals(0, allReferencesCloseCount);

    Closeable closer = maybeCloseable.get();
    closer.close();

    Assertions.assertFalse(referencedSegment.isClosed());
    Assertions.assertEquals(1, referencedSegmentClosedCount);
    Assertions.assertEquals(2, indexedTableJoinableReferenceCloseCount);
    Assertions.assertEquals(1, allReferencesCloseCount);

  }

  @Test
  public void testJoinableClausesClosedIfSegmentIsAlreadyClosed()
  {
    Assertions.assertFalse(referencedSegment.isClosed());

    referencedSegment.close();
    Assertions.assertTrue(referencedSegment.isClosed());

    Optional<Closeable> maybeCloseable = hashJoinSegment.acquireReferences();
    Assertions.assertFalse(maybeCloseable.isPresent());
    Assertions.assertEquals(0, referencedSegmentAcquireCount);
    Assertions.assertEquals(0, indexedTableJoinableReferenceAcquireCount);
    Assertions.assertEquals(0, allReferencesAcquireCount);
    Assertions.assertEquals(0, referencedSegmentClosedCount);
    Assertions.assertEquals(0, indexedTableJoinableReferenceCloseCount);
    Assertions.assertEquals(0, allReferencesCloseCount);
  }

  @Test
  public void testJoinableClausesClosedIfJoinableZeroIsAlreadyClosed()
  {
    Assertions.assertFalse(referencedSegment.isClosed());
    j0Closed = true;

    Optional<Closeable> maybeCloseable = hashJoinSegment.acquireReferences();
    Assertions.assertFalse(maybeCloseable.isPresent());
    Assertions.assertEquals(1, referencedSegmentAcquireCount);
    Assertions.assertEquals(0, indexedTableJoinableReferenceAcquireCount);
    Assertions.assertEquals(0, allReferencesAcquireCount);
    Assertions.assertEquals(1, referencedSegmentClosedCount);
    Assertions.assertEquals(0, indexedTableJoinableReferenceCloseCount);
    Assertions.assertEquals(0, allReferencesCloseCount);
  }

  @Test
  public void testJoinableClausesClosedIfJoinableOneIsAlreadyClosed()
  {
    Assertions.assertFalse(referencedSegment.isClosed());
    j1Closed = true;

    Optional<Closeable> maybeCloseable = hashJoinSegment.acquireReferences();
    Assertions.assertFalse(maybeCloseable.isPresent());
    Assertions.assertEquals(1, referencedSegmentAcquireCount);
    Assertions.assertEquals(1, indexedTableJoinableReferenceAcquireCount);
    Assertions.assertEquals(0, allReferencesAcquireCount);
    Assertions.assertEquals(1, referencedSegmentClosedCount);
    Assertions.assertEquals(1, indexedTableJoinableReferenceCloseCount);
    Assertions.assertEquals(0, allReferencesCloseCount);
  }

  private static File newFolder(File root, String... subDirs) throws IOException {
    String subFolder = String.join("/", subDirs);
    File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }
}
