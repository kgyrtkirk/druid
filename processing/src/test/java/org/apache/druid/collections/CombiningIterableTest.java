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

package org.apache.druid.collections;

import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.Result;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 */
public class CombiningIterableTest
{
  @Test
  public void testCreateSplatted()
  {
    List<Integer> firstList = Arrays.asList(1, 2, 5, 7, 9, 10, 20);
    List<Integer> secondList = Arrays.asList(1, 2, 5, 8, 9);
    Set<Integer> mergedLists = new HashSet<>();
    mergedLists.addAll(firstList);
    mergedLists.addAll(secondList);
    ArrayList<Iterable<Integer>> iterators = new ArrayList<>();
    iterators.add(firstList);
    iterators.add(secondList);
    CombiningIterable<Integer> actualIterable = CombiningIterable.createSplatted(
        iterators,
        Ordering.natural()
    );
    Assertions.assertEquals(mergedLists.size(), Iterables.size(actualIterable));
    Set actualHashset = Sets.newHashSet(actualIterable);
    Assertions.assertEquals(actualHashset, mergedLists);
  }
  
  @Test
  public void testMerge()
  {
    List<Result<Object>> resultsBefore = Arrays.asList(
        new Result<>(DateTimes.of("2011-01-01"), 1L),
        new Result<>(DateTimes.of("2011-01-01"), 2L)
    );

    Iterable<Result<Object>> expectedResults = Collections.singletonList(
        new Result<>(DateTimes.of("2011-01-01"), 3L)
    );

    Iterable<Result<Object>> resultsAfter = CombiningIterable.create(
        resultsBefore,
        Comparator.comparing(Result::getTimestamp),
        (arg1, arg2) -> {
          if (arg1 == null) {
            return arg2;
          }

          if (arg2 == null) {
            return arg1;
          }

          return new Result<>(
              arg1.getTimestamp(),
              ((Long) arg1.getValue()).longValue() + ((Long) arg2.getValue()).longValue()
          );
        }
    );

    Iterator<Result<Object>> it1 = expectedResults.iterator();
    Iterator<Result<Object>> it2 = resultsAfter.iterator();

    while (it1.hasNext() && it2.hasNext()) {
      Result r1 = it1.next();
      Result r2 = it2.next();

      Assertions.assertEquals(r1.getTimestamp(), r2.getTimestamp());
      Assertions.assertEquals(r1.getValue(), r2.getValue());
    }
  }
}
