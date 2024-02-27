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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ComparableListTest
{
  private final List<Integer> integers = ImmutableList.of(1, 2, 3);
  private final ComparableList comparableList = new ComparableList(ImmutableList.of(1, 2, 3));

  @Test
  public void testDelegate()
  {
    Assertions.assertEquals(integers, comparableList.getDelegate());
    Assertions.assertEquals(0, new ComparableList(ImmutableList.of()).getDelegate().size());
  }

  @Test
  public void testHashCode()
  {
    Assertions.assertEquals(integers.hashCode(), comparableList.hashCode());
    Set<ComparableList> set = new HashSet<>();
    set.add(comparableList);
    set.add(new ComparableList(integers));
    Assertions.assertEquals(1, set.size());
  }

  @Test
  public void testEquals()
  {
    Assertions.assertTrue(comparableList.equals(new ComparableList(integers)));
    Assertions.assertFalse(comparableList.equals(new ComparableList(ImmutableList.of(1, 2, 5))));
    Assertions.assertFalse(comparableList.equals(null));
  }

  @Test
  public void testCompareTo()
  {
    Assertions.assertEquals(0, comparableList.compareTo(new ComparableList(integers)));
    Assertions.assertEquals(1, comparableList.compareTo(null));
    Assertions.assertEquals(1, comparableList.compareTo(new ComparableList(ImmutableList.of(1, 2))));
    Assertions.assertEquals(-1, comparableList.compareTo(new ComparableList(ImmutableList.of(1, 2, 3, 4))));
    Assertions.assertTrue(comparableList.compareTo(new ComparableList(ImmutableList.of(2))) < 0);
    ComparableList nullList = new ComparableList(Lists.newArrayList(null, 1));

    Assertions.assertTrue(comparableList.compareTo(nullList) > 0);
    Assertions.assertTrue(nullList.compareTo(comparableList) < 0);
    Assertions.assertTrue(nullList.compareTo(new ComparableList(Lists.newArrayList(null, 1))) == 0);
  }
}
