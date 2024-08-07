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

package org.apache.druid.java.util.common.guava;

import com.google.common.base.Supplier;

import java.io.IOException;

/**
 */
public class LazySequence<T> implements Sequence<T>
{
  private final Supplier<Sequence<T>> provider;

  public LazySequence(
      Supplier<Sequence<T>> provider
  )
  {
    this.provider = provider;
  }

  @Override
  public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
  {
    return provider.get().accumulate(initValue, accumulator);
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    return new LazyYielder<OutType, T>(provider,initValue, accumulator);
  }

  static class LazyYielder<OutType,T> implements Yielder<OutType> {

    private Supplier<Sequence<T>> provider;
    private Yielder<OutType> realYielder=null;
    private OutType initValue;
    private YieldingAccumulator<OutType, T> accumulator;

    public LazyYielder(Supplier<Sequence<T>> provider, OutType initValue,
        YieldingAccumulator<OutType, T> accumulator)
    {
      this.provider = provider;
      this.initValue = initValue;
      this.accumulator = accumulator;
    }


    @Override
    public void close() throws IOException
    {
      getSeq().close();
    }

    private Yielder<OutType> getSeq()
    {
      if (realYielder== null) {
        realYielder= provider.get().toYielder(initValue, accumulator);
      }
      return realYielder;
    }


    @Override
    public OutType get()
    {
      return getSeq().get();
    }

    @Override
    public Yielder<OutType> next(OutType initValue)
    {
        return      getSeq().next(initValue);
    }

    @Override
    public boolean isDone()
    {
      return getSeq().isDone();
    }

  }
}

//Benchmark                                        (rowsPerSegment)  (schema)  (storageType)  Mode  Cnt    Score    Error  Units
//SqlWindowFunctionsBenchmark.windowWithSorter               100000      auto           mmap  avgt    5  812.024 ± 24.003  ms/op
//SqlWindowFunctionsBenchmark.windowWithoutSorter            100000      auto           mmap  avgt    5  611.424 ± 47.396  ms/op

//Benchmark                                        (rowsPerSegment)  (schema)  (storageType)  Mode  Cnt    Score    Error  Units
//SqlWindowFunctionsBenchmark.windowWithSorter               100000      auto           mmap  avgt    5  796.173 ± 23.359  ms/op
//SqlWindowFunctionsBenchmark.windowWithoutSorter            100000      auto           mmap  avgt    5  614.778 ± 32.237  ms/op

//Benchmark                                        (rowsPerSegment)  (schema)  (storageType)  Mode  Cnt    Score    Error  Units
//SqlWindowFunctionsBenchmark.windowWithSorter               100000      auto           mmap  avgt    5  867.023 ± 15.175  ms/op
//SqlWindowFunctionsBenchmark.windowWithoutSorter            100000      auto           mmap  avgt    5  660.928 ± 11.475  ms/op


//Benchmark                                        (rowsPerSegment)  (schema)  (storageType)  Mode  Cnt    Score    Error  Units
//SqlWindowFunctionsBenchmark.windowWithSorter               100000      auto           mmap  avgt    5  812.024 ± 24.003  ms/op
//SqlWindowFunctionsBenchmark.windowWithoutSorter            100000      auto           mmap  avgt    5  611.424 ± 47.396  ms/op

// ISO-8859-1
//Benchmark                                        (rowsPerSegment)  (schema)  (storageType)  Mode  Cnt    Score    Error  Units
//SqlWindowFunctionsBenchmark.windowWithSorter               100000      auto           mmap  avgt    5  779.933 ± 29.665  ms/op
//SqlWindowFunctionsBenchmark.windowWithoutSorter            100000      auto           mmap  avgt    5  613.465 ± 37.255  ms/op

