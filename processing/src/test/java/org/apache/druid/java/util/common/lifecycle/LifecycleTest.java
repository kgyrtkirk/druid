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

package org.apache.druid.java.util.common.lifecycle;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class LifecycleTest
{
  private static final Lifecycle.Handler DUMMY_HANDLER = new Lifecycle.Handler()
  {
    @Override
    public void start()
    {
      // do nothing
    }

    @Override
    public void stop()
    {
      // do nothing
    }
  };

  @Test
  public void testConcurrentStartStopOnce() throws Exception
  {
    final int numThreads = 10;
    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Execs.multiThreaded(
        numThreads,
        "LifecycleTest-%d"
    ));

    final Lifecycle lifecycle = new Lifecycle();
    final AtomicLong handlerFailedCount = new AtomicLong(0L);
    final Lifecycle.Handler exceptionalHandler = new Lifecycle.Handler()
    {
      final AtomicBoolean started = new AtomicBoolean(false);

      @Override
      public void start()
      {
        if (!started.compareAndSet(false, true)) {
          handlerFailedCount.incrementAndGet();
          throw new ISE("Already started");
        }
      }

      @Override
      public void stop()
      {
        if (!started.compareAndSet(true, false)) {
          handlerFailedCount.incrementAndGet();
          throw new ISE("Not yet started started");
        }
      }
    };
    lifecycle.addHandler(exceptionalHandler);
    Collection<ListenableFuture<?>> futures = new ArrayList<>(numThreads);
    final AtomicBoolean threadsStartLatch = new AtomicBoolean(false);
    final AtomicInteger threadFailedCount = new AtomicInteger(0);
    for (int i = 0; i < numThreads; ++i) {
      futures.add(
          executorService.submit(() -> {
            try {
              while (!threadsStartLatch.get()) {
                // await
              }
              lifecycle.start();
            }
            catch (Exception e) {
              threadFailedCount.incrementAndGet();
            }
          })
      );
    }
    try {
      threadsStartLatch.set(true);
      Futures.allAsList(futures).get();
    }
    finally {
      lifecycle.stop();
    }
    Assertions.assertEquals(numThreads - 1, threadFailedCount.get());
    Assertions.assertEquals(0, handlerFailedCount.get());
    executorService.shutdownNow();
  }

  @Test
  public void testStartStopOnce() throws Exception
  {
    final Lifecycle lifecycle = new Lifecycle();
    final AtomicLong failedCount = new AtomicLong(0L);
    Lifecycle.Handler exceptionalHandler = new Lifecycle.Handler()
    {
      final AtomicBoolean started = new AtomicBoolean(false);

      @Override
      public void start()
      {
        if (!started.compareAndSet(false, true)) {
          failedCount.incrementAndGet();
          throw new ISE("Already started");
        }
      }

      @Override
      public void stop()
      {
        if (!started.compareAndSet(true, false)) {
          failedCount.incrementAndGet();
          throw new ISE("Not yet started started");
        }
      }
    };
    lifecycle.addHandler(exceptionalHandler);
    lifecycle.start();
    lifecycle.stop();
    lifecycle.stop();
    lifecycle.stop();
    Assertions.assertEquals(0, failedCount.get());
    Exception ex = null;
    try {
      exceptionalHandler.stop();
    }
    catch (Exception e) {
      ex = e;
    }
    Assertions.assertNotNull(ex, "Should have exception");
  }

  @Test
  public void testSanity() throws Exception
  {
    Lifecycle lifecycle = new Lifecycle();

    List<Integer> startOrder = new ArrayList<>();
    List<Integer> stopOrder = new ArrayList<>();

    lifecycle.addManagedInstance(new ObjectToBeLifecycled(0, startOrder, stopOrder));
    lifecycle.addManagedInstance(new ObjectToBeLifecycled(1, startOrder, stopOrder), Lifecycle.Stage.NORMAL);
    lifecycle.addManagedInstance(new ObjectToBeLifecycled(2, startOrder, stopOrder), Lifecycle.Stage.NORMAL);
    lifecycle.addManagedInstance(new ObjectToBeLifecycled(3, startOrder, stopOrder), Lifecycle.Stage.ANNOUNCEMENTS);
    lifecycle.addStartCloseInstance(new ObjectToBeLifecycled(4, startOrder, stopOrder));
    lifecycle.addManagedInstance(new ObjectToBeLifecycled(5, startOrder, stopOrder));
    lifecycle.addStartCloseInstance(new ObjectToBeLifecycled(6, startOrder, stopOrder), Lifecycle.Stage.ANNOUNCEMENTS);
    lifecycle.addManagedInstance(new ObjectToBeLifecycled(7, startOrder, stopOrder));
    lifecycle.addStartCloseInstance(new ObjectToBeLifecycled(8, startOrder, stopOrder), Lifecycle.Stage.INIT);
    lifecycle.addStartCloseInstance(new ObjectToBeLifecycled(9, startOrder, stopOrder), Lifecycle.Stage.SERVER);

    final List<Integer> expectedOrder = Arrays.asList(8, 0, 1, 2, 4, 5, 7, 9, 3, 6);

    lifecycle.start();

    Assertions.assertEquals(10, startOrder.size());
    Assertions.assertEquals(0, stopOrder.size());
    Assertions.assertEquals(expectedOrder, startOrder);

    lifecycle.stop();

    Assertions.assertEquals(10, startOrder.size());
    Assertions.assertEquals(10, stopOrder.size());
    Assertions.assertEquals(Lists.reverse(expectedOrder), stopOrder);
  }

  @Test
  public void testAddToLifecycleInStartMethod() throws Exception
  {
    final Lifecycle lifecycle = new Lifecycle();

    final List<Integer> startOrder = new ArrayList<>();
    final List<Integer> stopOrder = new ArrayList<>();

    lifecycle.addManagedInstance(new ObjectToBeLifecycled(0, startOrder, stopOrder));
    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            lifecycle.addMaybeStartManagedInstance(
                new ObjectToBeLifecycled(1, startOrder, stopOrder),
                Lifecycle.Stage.NORMAL
            );
            lifecycle.addMaybeStartManagedInstance(
                new ObjectToBeLifecycled(2, startOrder, stopOrder),
                Lifecycle.Stage.INIT
            );
            lifecycle.addMaybeStartManagedInstance(
                new ObjectToBeLifecycled(3, startOrder, stopOrder),
                Lifecycle.Stage.ANNOUNCEMENTS
            );
            lifecycle.addMaybeStartStartCloseInstance(new ObjectToBeLifecycled(4, startOrder, stopOrder));
            lifecycle.addMaybeStartManagedInstance(new ObjectToBeLifecycled(5, startOrder, stopOrder));
            lifecycle.addMaybeStartStartCloseInstance(
                new ObjectToBeLifecycled(6, startOrder, stopOrder),
                Lifecycle.Stage.ANNOUNCEMENTS
            );
            lifecycle.addMaybeStartManagedInstance(new ObjectToBeLifecycled(7, startOrder, stopOrder));
            lifecycle.addMaybeStartManagedInstance(
                new ObjectToBeLifecycled(8, startOrder, stopOrder),
                Lifecycle.Stage.SERVER
            );
          }

          @Override
          public void stop()
          {

          }
        }
    );

    final List<Integer> expectedOrder = Arrays.asList(0, 1, 2, 4, 5, 7, 8, 3, 6);
    final List<Integer> expectedStopOrder = Arrays.asList(6, 3, 8, 7, 5, 4, 1, 0, 2);

    lifecycle.start();

    Assertions.assertEquals(expectedOrder, startOrder);
    Assertions.assertEquals(0, stopOrder.size());

    lifecycle.stop();

    Assertions.assertEquals(expectedOrder, startOrder);
    Assertions.assertEquals(expectedStopOrder, stopOrder);
  }

  public static class ObjectToBeLifecycled
  {
    private final int id;
    private final List<Integer> orderOfStarts;
    private final List<Integer> orderOfStops;

    ObjectToBeLifecycled(
        int id,
        List<Integer> orderOfStarts,
        List<Integer> orderOfStops
    )
    {
      this.id = id;
      this.orderOfStarts = orderOfStarts;
      this.orderOfStops = orderOfStops;
    }

    @LifecycleStart
    public void start()
    {
      orderOfStarts.add(id);
    }

    @LifecycleStop
    public void close()
    {
      orderOfStops.add(id);
    }
  }

  @Test
  public void testFailAddToLifecycleDuringStopMethod() throws Exception
  {
    CountDownLatch reachedStop = new CountDownLatch(1);
    CountDownLatch stopper = new CountDownLatch(1);
    Lifecycle.Handler stoppingHandler = new Lifecycle.Handler()
    {
      @Override
      public void start()
      {
        // do nothing
      }

      @Override
      public void stop()
      {
        reachedStop.countDown();
        try {
          stopper.await();
        }
        catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    };
    Lifecycle lifecycle = new Lifecycle();
    lifecycle.addHandler(stoppingHandler);
    lifecycle.start();
    new Thread(lifecycle::stop).start(); // will stop at stoppingHandler.stop()
    reachedStop.await();

    try {
      lifecycle.addHandler(DUMMY_HANDLER);
      Assertions.fail("Expected exception");
    }
    catch (IllegalStateException e) {
      Assertions.assertTrue(e.getMessage().contains("Cannot add a handler"));
    }

    try {
      lifecycle.addMaybeStartHandler(DUMMY_HANDLER);
      Assertions.fail("Expected exception");
    }
    catch (IllegalStateException e) {
      Assertions.assertTrue(e.getMessage().contains("Cannot add a handler"));
    }
  }
}
