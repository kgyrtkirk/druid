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

package org.apache.druid.benchmark.query;

import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceWrapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class WrappingYielderBenchmark
{
  public WrappingYielderBenchmark() {
    setup();
  }

  private SequenceWrapper wrapper;
  private Sequence<Integer> baseSeq;
  private YieldingAccumulator<Integer, Integer> accumlator = new YieldingAccumulator<Integer, Integer>()
  {
    @Override
    public Integer accumulate(Integer accumulated, Integer in)
    {
      return accumulated + in;
    }
  };

  @Setup(Level.Trial)
  public void setup()
  {
    List<Integer> li = new ArrayList<>();
    for (int i = 0; i < 1_000_000; i++) {
      li.add(i);

    }
    baseSeq = Sequences.simple(li);

    wrapper = new SequenceWrapper()
    {
      int wraps;

      @Override
      public <RetType> RetType wrap(Supplier<RetType> sequenceProcessing)
      {
        wraps++;
        return super.wrap(sequenceProcessing);
      }

      @Override
      public String toString()
      {
        return "wraps: " + wraps;
      }
    };
  }

  private Accumulator accumator0=new Accumulator<Integer, Integer>()
  {
    @Override
    public Integer accumulate(Integer accumulated, Integer in)
    {
      return in+accumulated;
    }
  };

  @Test
  public void runAccumlator()
  {
    runAccumlator(new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous."));
  }

  @Test
  public void runYielder()
  {
    runYielder(new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous."));
  }


  @Benchmark
  public void runAccumlator(Blackhole blackhole)
  {
    Sequence<Integer> wrapped = Sequences.wrap(baseSeq, wrapper);
    Integer init = 0;
    Integer res = wrapped.accumulate(init, accumator0);
    blackhole.consume(res);
  }


  @Benchmark
  public void runYielder(Blackhole blackhole)
  {
    Sequence<Integer> wrapped = Sequences.wrap(baseSeq, wrapper);
    Integer init = 0;
    Yielder<Integer> y = Yielders.each(wrapped);
//    Yielder<Integer> y = wrapped.toYielder(init, accumlator);

    while (!y.isDone()) {
      y = y.next(init);
    }
    blackhole.consume(y);
//    init=y.get();
//    System.out.println(init);
    System.out.println(wrapper);
  }

}

/**

Benchmark                            Mode  Cnt  Score   Error  Units
WrappingYielderBenchmark.runYielder  avgt    5  5.494 ± 0.202  ms/op

*/