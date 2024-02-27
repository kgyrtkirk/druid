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

package org.apache.druid.query.aggregation;

import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.TestLongColumnSelector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;

public class MetricManipulatorFnsTest
{
  private static final String NAME = "name";
  private static final String FIELD = "field";

  public static Iterable<Object[]> constructorFeeder()
  {
    final ArrayList<Object[]> constructorArrays = new ArrayList<>();
    final long longVal = 13789;
    LongMinAggregator longMinAggregator = new LongMinAggregator(
        new TestLongColumnSelector()
        {
          @Override
          public long getLong()
          {
            return longVal;
          }

          @Override
          public boolean isNull()
          {
            return false;
          }
        }
    );
    LongMinAggregatorFactory longMinAggregatorFactory = new LongMinAggregatorFactory(NAME, FIELD);
    constructorArrays.add(
        new Object[]{
            longMinAggregatorFactory,
            longMinAggregator,
            longMinAggregator,
            longMinAggregator,
            longVal,
            longVal
        }
    );

    HyperUniquesAggregatorFactory hyperUniquesAggregatorFactory = new HyperUniquesAggregatorFactory(NAME, FIELD);
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    collector.add((short) 1, (byte) 5);

    constructorArrays.add(
        new Object[]{
            hyperUniquesAggregatorFactory,
            collector,
            collector,
            collector.estimateCardinality(),
            collector.toByteArray(),
            collector
        }
    );


    LongSumAggregatorFactory longSumAggregatorFactory = new LongSumAggregatorFactory(NAME, FIELD);
    LongSumAggregator longSumAggregator = new LongSumAggregator(
        new TestLongColumnSelector()
        {
          @Override
          public long getLong()
          {
            return longVal;
          }

          @Override
          public boolean isNull()
          {
            return false;
          }
        }
    );
    constructorArrays.add(
        new Object[]{
            longSumAggregatorFactory,
            longSumAggregator,
            longSumAggregator,
            longSumAggregator,
            longVal,
            longVal
        }
    );


    for (Object[] argList : constructorArrays) {
      Assertions.assertEquals(
          6, argList.length, StringUtils.format(
              "Arglist %s is too short. Expected 6 found %d",
              Arrays.toString(argList),
              argList.length
          )
      );
    }
    return constructorArrays;
  }

  private AggregatorFactory aggregatorFactory;
  private Object agg;
  private Object identity;
  private Object finalize;
  private Object serialForm;
  private Object deserForm;

  public void initMetricManipulatorFnsTest(
      AggregatorFactory aggregatorFactory,
      Object agg,
      Object identity,
      Object finalize,
      Object serialForm,
      Object deserForm
  )
  {
    this.aggregatorFactory = aggregatorFactory;
    this.agg = agg;
    this.identity = identity;
    this.finalize = finalize;
    this.serialForm = serialForm;
    this.deserForm = deserForm;
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testIdentity(AggregatorFactory aggregatorFactory, Object agg, Object identity, Object finalize, Object serialForm, Object deserForm)
  {
    initMetricManipulatorFnsTest(aggregatorFactory, agg, identity, finalize, serialForm, deserForm);
    Assertions.assertEquals(identity, agg);
    Assertions.assertEquals(identity, MetricManipulatorFns.identity().manipulate(aggregatorFactory, agg));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testFinalize(AggregatorFactory aggregatorFactory, Object agg, Object identity, Object finalize, Object serialForm, Object deserForm)
  {
    initMetricManipulatorFnsTest(aggregatorFactory, agg, identity, finalize, serialForm, deserForm);
    Assertions.assertEquals(identity, agg);
    Assertions.assertEquals(finalize, MetricManipulatorFns.finalizing().manipulate(aggregatorFactory, agg));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testDeserialize(AggregatorFactory aggregatorFactory, Object agg, Object identity, Object finalize, Object serialForm, Object deserForm)
  {
    initMetricManipulatorFnsTest(aggregatorFactory, agg, identity, finalize, serialForm, deserForm);
    Assertions.assertEquals(identity, agg);
    Assertions.assertEquals(deserForm, MetricManipulatorFns.deserializing().manipulate(aggregatorFactory, serialForm));
  }
}
