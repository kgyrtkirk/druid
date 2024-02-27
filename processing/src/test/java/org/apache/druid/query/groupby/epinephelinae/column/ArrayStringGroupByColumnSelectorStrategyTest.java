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

package org.apache.druid.query.groupby.epinephelinae.column;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.data.ComparableIntArray;
import org.apache.druid.segment.data.ComparableStringArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.ByteBuffer;

@ExtendWith(MockitoExtension.class)
public class ArrayStringGroupByColumnSelectorStrategyTest
{
  private final BiMap<String, Integer> dictionaryInt = HashBiMap.create();

  // The dictionary has been constructed such that the values are not sorted lexicographically
  // so we can tell when the comparator uses a lexicographic comparison and when it uses the indexes.
  private final BiMap<ComparableIntArray, Integer> indexedIntArrays = HashBiMap.create();

  private final ByteBuffer buffer1 = ByteBuffer.allocate(4);
  private final ByteBuffer buffer2 = ByteBuffer.allocate(4);

  private ArrayStringGroupByColumnSelectorStrategy strategy;

  @BeforeEach
  public void setup()
  {
    strategy = new ArrayStringGroupByColumnSelectorStrategy(dictionaryInt, indexedIntArrays);

    dictionaryInt.put("a", 0);
    dictionaryInt.put("b", 1);
    dictionaryInt.put("bd", 2);
    dictionaryInt.put("d", 3);
    dictionaryInt.put("e", 4);

    indexedIntArrays.put(ComparableIntArray.of(0, 1), 0);
    indexedIntArrays.put(ComparableIntArray.of(2, 4), 1);
    indexedIntArrays.put(ComparableIntArray.of(0, 2), 2);
  }

  @Test
  public void testKeySize()
  {
    Assertions.assertEquals(Integer.BYTES, strategy.getGroupingKeySize());
  }

  @Test
  public void testWriteKey()
  {
    strategy.writeToKeyBuffer(0, 1, buffer1);
    Assertions.assertEquals(1, buffer1.getInt(0));
  }

  @Test
  public void testBufferComparatorCanCompareIntsAndNullStringComparatorShouldUseLexicographicComparator()
  {
    buffer1.putInt(1);
    buffer2.putInt(2);
    Grouper.BufferComparator comparator = strategy.bufferComparator(0, null);
    Assertions.assertTrue(comparator.compare(buffer1, buffer2, 0, 0) > 0);
    Assertions.assertTrue(comparator.compare(buffer2, buffer1, 0, 0) < 0);
  }

  @Test
  public void testBufferComparatorCanCompareIntsAndLexicographicStringComparatorShouldUseLexicographicComparator()
  {
    buffer1.putInt(1);
    buffer2.putInt(2);
    Grouper.BufferComparator comparator = strategy.bufferComparator(0, StringComparators.LEXICOGRAPHIC);
    Assertions.assertTrue(comparator.compare(buffer1, buffer2, 0, 0) > 0);
    Assertions.assertTrue(comparator.compare(buffer2, buffer1, 0, 0) < 0);
  }

  @Test
  public void testBufferComparatorCanCompareIntsAndStrLenStringComparatorShouldUseLexicographicComparator()
  {
    buffer1.putInt(1);
    buffer2.putInt(2);
    Grouper.BufferComparator comparator = strategy.bufferComparator(0, StringComparators.STRLEN);
    Assertions.assertTrue(comparator.compare(buffer1, buffer2, 0, 0) > 0);
    Assertions.assertTrue(comparator.compare(buffer2, buffer1, 0, 0) < 0);
  }

  @Test
  public void testSanity()
  {
    ColumnValueSelector columnValueSelector = Mockito.mock(ColumnValueSelector.class);
    Mockito.when(columnValueSelector.getObject()).thenReturn(ImmutableList.of("a", "b"));
    Assertions.assertEquals(0, strategy.computeDictionaryId(columnValueSelector));

    GroupByColumnSelectorPlus groupByColumnSelectorPlus = Mockito.mock(GroupByColumnSelectorPlus.class);
    Mockito.when(groupByColumnSelectorPlus.getResultRowPosition()).thenReturn(0);
    ResultRow row = ResultRow.create(1);

    buffer1.putInt(0);
    strategy.processValueFromGroupingKey(groupByColumnSelectorPlus, buffer1, row, 0);
    Assertions.assertEquals(ComparableStringArray.of("a", "b"), row.get(0));
  }


  @Test
  public void testAddingInDictionary()
  {
    ColumnValueSelector columnValueSelector = Mockito.mock(ColumnValueSelector.class);
    Mockito.when(columnValueSelector.getObject()).thenReturn(ImmutableList.of("f", "a"));
    Assertions.assertEquals(3, strategy.computeDictionaryId(columnValueSelector));

    GroupByColumnSelectorPlus groupByColumnSelectorPlus = Mockito.mock(GroupByColumnSelectorPlus.class);
    Mockito.when(groupByColumnSelectorPlus.getResultRowPosition()).thenReturn(0);
    ResultRow row = ResultRow.create(1);

    buffer1.putInt(3);
    strategy.processValueFromGroupingKey(groupByColumnSelectorPlus, buffer1, row, 0);
    Assertions.assertEquals(ComparableStringArray.of("f", "a"), row.get(0));
  }

  @Test
  public void testAddingInDictionaryWithObjects()
  {
    ColumnValueSelector columnValueSelector = Mockito.mock(ColumnValueSelector.class);
    Mockito.when(columnValueSelector.getObject()).thenReturn(new Object[]{"f", "a"});
    Assertions.assertEquals(3, strategy.computeDictionaryId(columnValueSelector));

    GroupByColumnSelectorPlus groupByColumnSelectorPlus = Mockito.mock(GroupByColumnSelectorPlus.class);
    Mockito.when(groupByColumnSelectorPlus.getResultRowPosition()).thenReturn(0);
    ResultRow row = ResultRow.create(1);

    buffer1.putInt(3);
    strategy.processValueFromGroupingKey(groupByColumnSelectorPlus, buffer1, row, 0);
    Assertions.assertEquals(ComparableStringArray.of("f", "a"), row.get(0));
  }

  @AfterEach
  public void tearDown()
  {
    buffer1.clear();
    buffer2.clear();
  }
}
