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

package org.apache.druid.query;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;

/**
 */
public class ResultGranularTimestampComparatorTest
{
  public static Iterable<Object[]> constructorFeeder()
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(Arrays.asList(false, true));
  }

  private boolean descending;

  public void initResultGranularTimestampComparatorTest(boolean descending)
  {
    this.descending = descending;
  }

  private final DateTime time = DateTimes.of("2011-11-11");

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "descending={0}")
  public void testCompareAll(boolean descending)
  {
    initResultGranularTimestampComparatorTest(descending);
    Result<Object> r1 = new Result<Object>(time, null);
    Result<Object> r2 = new Result<Object>(time.plusYears(5), null);

    Assertions.assertEquals(ResultGranularTimestampComparator.create(Granularities.ALL, descending).compare(r1, r2), 0);
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "descending={0}")
  public void testCompareDay(boolean descending)
  {
    initResultGranularTimestampComparatorTest(descending);
    Result<Object> res = new Result<Object>(time, null);
    Result<Object> same = new Result<Object>(time.plusHours(12), null);
    Result<Object> greater = new Result<Object>(time.plusHours(25), null);
    Result<Object> less = new Result<Object>(time.minusHours(1), null);

    Granularity day = Granularities.DAY;
    Assertions.assertEquals(ResultGranularTimestampComparator.create(day, descending).compare(res, same), 0);
    Assertions.assertEquals(ResultGranularTimestampComparator.create(day, descending).compare(res, greater), descending ? 1 : -1);
    Assertions.assertEquals(ResultGranularTimestampComparator.create(day, descending).compare(res, less), descending ? -1 : 1);
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "descending={0}")
  public void testCompareHour(boolean descending)
  {
    initResultGranularTimestampComparatorTest(descending);
    Result<Object> res = new Result<Object>(time, null);
    Result<Object> same = new Result<Object>(time.plusMinutes(55), null);
    Result<Object> greater = new Result<Object>(time.plusHours(1), null);
    Result<Object> less = new Result<Object>(time.minusHours(1), null);

    Granularity hour = Granularities.HOUR;
    Assertions.assertEquals(ResultGranularTimestampComparator.create(hour, descending).compare(res, same), 0);
    Assertions.assertEquals(ResultGranularTimestampComparator.create(hour, descending).compare(res, greater), descending ? 1 : -1);
    Assertions.assertEquals(ResultGranularTimestampComparator.create(hour, descending).compare(res, less), descending ? -1 : 1);
  }
}
