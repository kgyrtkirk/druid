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

public class ComparableStringArrayTest
{
  private final String[] array = new String[]{"a", "b", "c"};
  private final ComparableStringArray comparableStringArray = ComparableStringArray.of("a", "b", "c");

  @Test
  public void testDelegate()
  {
    Assertions.assertArrayEquals(array, comparableStringArray.getDelegate());
    Assertions.assertEquals(0, ComparableStringArray.of(new String[0]).getDelegate().length);
    Assertions.assertEquals(0, ComparableStringArray.of().getDelegate().length);
  }

  @Test
  public void testHashCode()
  {
    Assertions.assertEquals(Arrays.hashCode(array), comparableStringArray.hashCode());
    Set<ComparableStringArray> set = new HashSet<>();
    set.add(comparableStringArray);
    set.add(ComparableStringArray.of(array));
    Assertions.assertEquals(1, set.size());
  }

  @Test
  public void testEquals()
  {
    Assertions.assertTrue(comparableStringArray.equals(ComparableStringArray.of(array)));
    Assertions.assertFalse(comparableStringArray.equals(ComparableStringArray.of("a", "b", "C")));
    Assertions.assertFalse(comparableStringArray.equals(ComparableStringArray.EMPTY_ARRAY));
    Assertions.assertFalse(comparableStringArray.equals(null));
  }

  @Test
  public void testCompareTo()
  {
    Assertions.assertEquals(0, comparableStringArray.compareTo(ComparableStringArray.of(array)));
    Assertions.assertEquals(1, comparableStringArray.compareTo(null));
    Assertions.assertEquals(1, comparableStringArray.compareTo(ComparableStringArray.of("a", "b")));
    Assertions.assertEquals(-1, comparableStringArray.compareTo(ComparableStringArray.of("a", "b", "c", "d")));
    Assertions.assertTrue(comparableStringArray.compareTo(ComparableStringArray.of("b")) < 0);

    ComparableStringArray nullList = ComparableStringArray.of(null, "a");

    Assertions.assertTrue(comparableStringArray.compareTo(nullList) > 0);
    Assertions.assertTrue(nullList.compareTo(comparableStringArray) < 0);
    Assertions.assertTrue(nullList.compareTo(ComparableStringArray.of(null, "a")) == 0);
  }
}
