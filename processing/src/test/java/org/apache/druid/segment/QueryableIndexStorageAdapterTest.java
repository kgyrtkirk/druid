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

package org.apache.druid.segment;

import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;



public class QueryableIndexStorageAdapterTest
{
  @Nested
  public class DimensionDictionarySelectorTest extends InitializedNullHandlingTest
  {
    private boolean vectorize;

    private DimensionDictionarySelector qualitySelector;
    private DimensionDictionarySelector placementishSelector;
    private DimensionDictionarySelector partialNullSelector;

    private Closer closer = Closer.create();

    public static Collection<?> constructorFeeder()
    {
      return Arrays.asList(new Object[]{false}, new Object[]{true});
    }

    public void initDimensionDictionarySelectorTest(boolean vectorize)
    {
      this.vectorize = vectorize;
    }

    @BeforeEach
    public void setUp()
    {
      final QueryableIndex index = TestIndex.getMMappedTestIndex();
      final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(index);

      if (vectorize) {
        final VectorCursor cursor = closer.register(
            adapter.makeVectorCursor(
                null,
                Intervals.ETERNITY,
                VirtualColumns.EMPTY,
                false,
                QueryableIndexStorageAdapter.DEFAULT_VECTOR_SIZE,
                null
            )
        );

        final VectorColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();

        qualitySelector =
            columnSelectorFactory.makeSingleValueDimensionSelector(DefaultDimensionSpec.of("quality"));
        placementishSelector =
            columnSelectorFactory.makeMultiValueDimensionSelector(DefaultDimensionSpec.of("placementish"));
        partialNullSelector =
            columnSelectorFactory.makeSingleValueDimensionSelector(DefaultDimensionSpec.of("partial_null_column"));
      } else {
        final Sequence<Cursor> cursors = adapter.makeCursors(
            null,
            Intervals.ETERNITY,
            VirtualColumns.EMPTY,
            Granularities.ALL,
            false,
            null
        );

        final Yielder<Cursor> yielder = closer.register(Yielders.each(cursors));
        final Cursor cursor = yielder.get();
        final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();

        qualitySelector =
            columnSelectorFactory.makeDimensionSelector(DefaultDimensionSpec.of("quality"));
        placementishSelector =
            columnSelectorFactory.makeDimensionSelector(DefaultDimensionSpec.of("placementish"));
        partialNullSelector =
            columnSelectorFactory.makeDimensionSelector(DefaultDimensionSpec.of("partial_null_column"));
      }
    }

    @AfterEach
    public void tearDown() throws IOException
    {
      closer.close();
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "vectorize = {0}")
    public void test_getCardinality_quality(boolean vectorize)
    {
      initDimensionDictionarySelectorTest(vectorize);
      Assertions.assertEquals(9, qualitySelector.getValueCardinality());
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "vectorize = {0}")
    public void test_getCardinality_placementish(boolean vectorize)
    {
      initDimensionDictionarySelectorTest(vectorize);
      Assertions.assertEquals(9, placementishSelector.getValueCardinality());
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "vectorize = {0}")
    public void test_getCardinality_partialNullColumn(boolean vectorize)
    {
      initDimensionDictionarySelectorTest(vectorize);
      Assertions.assertEquals(2, partialNullSelector.getValueCardinality());
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "vectorize = {0}")
    public void test_lookupName_quality(boolean vectorize)
    {
      initDimensionDictionarySelectorTest(vectorize);
      Assertions.assertEquals("automotive", qualitySelector.lookupName(0));
      Assertions.assertEquals("business", qualitySelector.lookupName(1));
      Assertions.assertEquals("entertainment", qualitySelector.lookupName(2));
      Assertions.assertEquals("health", qualitySelector.lookupName(3));
      Assertions.assertEquals("mezzanine", qualitySelector.lookupName(4));
      Assertions.assertEquals("news", qualitySelector.lookupName(5));
      Assertions.assertEquals("premium", qualitySelector.lookupName(6));
      Assertions.assertEquals("technology", qualitySelector.lookupName(7));
      Assertions.assertEquals("travel", qualitySelector.lookupName(8));
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "vectorize = {0}")
    public void test_lookupName_placementish(boolean vectorize)
    {
      initDimensionDictionarySelectorTest(vectorize);
      Assertions.assertEquals("a", placementishSelector.lookupName(0));
      Assertions.assertEquals("b", placementishSelector.lookupName(1));
      Assertions.assertEquals("e", placementishSelector.lookupName(2));
      Assertions.assertEquals("h", placementishSelector.lookupName(3));
      Assertions.assertEquals("m", placementishSelector.lookupName(4));
      Assertions.assertEquals("n", placementishSelector.lookupName(5));
      Assertions.assertEquals("p", placementishSelector.lookupName(6));
      Assertions.assertEquals("preferred", placementishSelector.lookupName(7));
      Assertions.assertEquals("t", placementishSelector.lookupName(8));
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "vectorize = {0}")
    public void test_lookupName_partialNull(boolean vectorize)
    {
      initDimensionDictionarySelectorTest(vectorize);
      Assertions.assertNull(partialNullSelector.lookupName(0));
      Assertions.assertEquals("value", partialNullSelector.lookupName(1));
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "vectorize = {0}")
    public void test_lookupNameUtf8_quality(boolean vectorize)
    {
      initDimensionDictionarySelectorTest(vectorize);
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("automotive")), qualitySelector.lookupNameUtf8(0));
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("business")), qualitySelector.lookupNameUtf8(1));
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("entertainment")), qualitySelector.lookupNameUtf8(2));
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("health")), qualitySelector.lookupNameUtf8(3));
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("mezzanine")), qualitySelector.lookupNameUtf8(4));
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("news")), qualitySelector.lookupNameUtf8(5));
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("premium")), qualitySelector.lookupNameUtf8(6));
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("technology")), qualitySelector.lookupNameUtf8(7));
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("travel")), qualitySelector.lookupNameUtf8(8));
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "vectorize = {0}")
    public void test_lookupNameUtf8_placementish(boolean vectorize)
    {
      initDimensionDictionarySelectorTest(vectorize);
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("a")), placementishSelector.lookupNameUtf8(0));
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("b")), placementishSelector.lookupNameUtf8(1));
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("e")), placementishSelector.lookupNameUtf8(2));
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("h")), placementishSelector.lookupNameUtf8(3));
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("m")), placementishSelector.lookupNameUtf8(4));
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("n")), placementishSelector.lookupNameUtf8(5));
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("p")), placementishSelector.lookupNameUtf8(6));
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("preferred")), placementishSelector.lookupNameUtf8(7));
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("t")), placementishSelector.lookupNameUtf8(8));
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "vectorize = {0}")
    public void test_lookupNameUtf8_partialNull(boolean vectorize)
    {
      initDimensionDictionarySelectorTest(vectorize);
      Assertions.assertNull(partialNullSelector.lookupNameUtf8(0));
      Assertions.assertEquals(ByteBuffer.wrap(StringUtils.toUtf8("value")), partialNullSelector.lookupNameUtf8(1));
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "vectorize = {0}")
    public void test_lookupNameUtf8_buffersAreNotShared(boolean vectorize)
    {
      initDimensionDictionarySelectorTest(vectorize);
      // Different buffer on different calls; enables callers to safely modify position, limit as promised in
      // the javadocs.
      Assertions.assertNotSame(qualitySelector.lookupNameUtf8(0), qualitySelector.lookupNameUtf8(0));
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "vectorize = {0}")
    public void test_supportsLookupNameUtf8_quality(boolean vectorize)
    {
      initDimensionDictionarySelectorTest(vectorize);
      Assertions.assertTrue(partialNullSelector.supportsLookupNameUtf8());
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "vectorize = {0}")
    public void test_supportsLookupNameUtf8_placementish(boolean vectorize)
    {
      initDimensionDictionarySelectorTest(vectorize);
      Assertions.assertTrue(partialNullSelector.supportsLookupNameUtf8());
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "vectorize = {0}")
    public void test_supportsLookupNameUtf8_partialNull(boolean vectorize)
    {
      initDimensionDictionarySelectorTest(vectorize);
      Assertions.assertTrue(partialNullSelector.supportsLookupNameUtf8());
    }
  }

  @Nested
  public class ManySelectorsOneColumnTest extends InitializedNullHandlingTest
  {
    private Cursor cursor;
    private ColumnSelectorFactory columnSelectorFactory;
    private final Closer closer = Closer.create();

    @BeforeEach
    public void setUp()
    {
      final QueryableIndex index = TestIndex.getMMappedTestIndex();
      final QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(index);
      final Sequence<Cursor> cursors = adapter.makeCursors(
          null,
          Intervals.ETERNITY,
          VirtualColumns.EMPTY,
          Granularities.ALL,
          false,
          null
      );
      final Yielder<Cursor> cursorYielder = Yielders.each(cursors);
      cursor = cursorYielder.get();
      columnSelectorFactory = cursor.getColumnSelectorFactory();
      closer.register(cursorYielder);
    }

    @AfterEach
    public void testDown() throws IOException
    {
      closer.close();
    }

    @Test
    public void testTwoSelectorsOneComplexColumn()
    {
      final ColumnValueSelector<?> valueSelector = columnSelectorFactory.makeColumnValueSelector("quality_uniques");
      assertThat(valueSelector.getObject(), CoreMatchers.instanceOf(HyperLogLogCollector.class));

      final DimensionSelector dimensionSelector =
          columnSelectorFactory.makeDimensionSelector(DefaultDimensionSpec.of("quality_uniques"));
      Assertions.assertNull(dimensionSelector.getObject());
    }

    @Test
    public void testTwoSelectorsOneNumericColumn()
    {
      final ColumnValueSelector<?> valueSelector = columnSelectorFactory.makeColumnValueSelector("index");
      assertThat(valueSelector.getObject(), CoreMatchers.instanceOf(Double.class));

      final DimensionSelector dimensionSelector =
          columnSelectorFactory.makeDimensionSelector(DefaultDimensionSpec.of("index"));
      Assertions.assertEquals("100.0", dimensionSelector.getObject());
    }

    @Test
    public void testTwoSelectorsOneStringColumn()
    {
      final ColumnValueSelector<?> valueSelector = columnSelectorFactory.makeColumnValueSelector("market");
      assertThat(valueSelector.getObject(), CoreMatchers.instanceOf(String.class));

      final DimensionSelector dimensionSelector =
          columnSelectorFactory.makeDimensionSelector(DefaultDimensionSpec.of("market"));
      assertThat(dimensionSelector.getObject(), CoreMatchers.instanceOf(String.class));
    }
  }
}
