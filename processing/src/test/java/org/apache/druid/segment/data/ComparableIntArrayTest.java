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

package org.apache.druid.segment.data;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ComparableIntArrayTest
{
  private final int[] array = new int[]{1, 2, 3};
  private final ComparableIntArray comparableIntArray = ComparableIntArray.of(1, 2, 3);

  @Test
  public void testDelegate()
  {
    Assertions.assertArrayEquals(array, comparableIntArray.getDelegate());
    Assertions.assertEquals(0, ComparableIntArray.of(new int[0]).getDelegate().length);
    Assertions.assertEquals(0, ComparableIntArray.of().getDelegate().length);
  }

  @Test
  public void testHashCode()
  {
    Assertions.assertEquals(Arrays.hashCode(array), comparableIntArray.hashCode());
    Set<ComparableIntArray> set = new HashSet<>();
    set.add(comparableIntArray);
    set.add(ComparableIntArray.of(array));
    Assertions.assertEquals(1, set.size());
  }

  @Test
  public void testEquals()
  {
    Assertions.assertTrue(comparableIntArray.equals(ComparableIntArray.of(array)));
    Assertions.assertFalse(comparableIntArray.equals(ComparableIntArray.of(1, 2, 5)));
    Assertions.assertFalse(comparableIntArray.equals(ComparableIntArray.EMPTY_ARRAY));
    Assertions.assertFalse(comparableIntArray.equals(null));
  }

  @Test
  public void testCompareTo()
  {
    Assertions.assertEquals(0, comparableIntArray.compareTo(ComparableIntArray.of(array)));
    Assertions.assertEquals(1, comparableIntArray.compareTo(null));
    Assertions.assertEquals(1, comparableIntArray.compareTo(ComparableIntArray.of(1, 2)));
    Assertions.assertEquals(-1, comparableIntArray.compareTo(ComparableIntArray.of(1, 2, 3, 4)));
    Assertions.assertTrue(comparableIntArray.compareTo(ComparableIntArray.of(2)) < 0);
  }
}
