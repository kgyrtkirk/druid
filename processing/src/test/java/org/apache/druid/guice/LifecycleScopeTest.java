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

package org.apache.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 */
public class LifecycleScopeTest
{
  @Test
  public void testAnnotation() throws Exception
  {
    final Injector injector = Guice.createInjector(
        new DruidGuiceExtensions(),
        new LifecycleModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(TestInterface.class).to(AnnotatedClass.class);
          }
        }
    );

    final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);

    final TestInterface instance = injector.getInstance(TestInterface.class);

    testIt(injector, lifecycle, instance);
  }

  @Test
  public void testExplicit() throws Exception
  {
    final Injector injector = Guice.createInjector(
        new DruidGuiceExtensions(),
        new LifecycleModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(TestInterface.class).to(ExplicitClass.class).in(ManageLifecycle.class);
          }
        }
    );

    final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);

    final TestInterface instance = injector.getInstance(TestInterface.class);

    testIt(injector, lifecycle, instance);
  }

  private void testIt(Injector injector, Lifecycle lifecycle, TestInterface instance)
      throws Exception
  {
    Assertions.assertEquals(0, instance.getStarted());
    Assertions.assertEquals(0, instance.getStopped());
    Assertions.assertEquals(0, instance.getRan());

    instance.run();
    Assertions.assertEquals(0, instance.getStarted());
    Assertions.assertEquals(0, instance.getStopped());
    Assertions.assertEquals(1, instance.getRan());

    lifecycle.start();
    Assertions.assertEquals(1, instance.getStarted());
    Assertions.assertEquals(0, instance.getStopped());
    Assertions.assertEquals(1, instance.getRan());

    injector.getInstance(TestInterface.class).run();  // It's a singleton
    Assertions.assertEquals(1, instance.getStarted());
    Assertions.assertEquals(0, instance.getStopped());
    Assertions.assertEquals(2, instance.getRan());

    lifecycle.stop();
    Assertions.assertEquals(1, instance.getStarted());
    Assertions.assertEquals(1, instance.getStopped());
    Assertions.assertEquals(2, instance.getRan());
  }

  /**
   * This is a test for documentation purposes.  It's there to show what weird things Guice will do when
   * it sees both the annotation and an explicit binding.
   *
   * @throws Exception
   */
  @Test
  public void testAnnotatedAndExplicit() throws Exception
  {
    final Injector injector = Guice.createInjector(
        new DruidGuiceExtensions(),
        new LifecycleModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(TestInterface.class).to(AnnotatedClass.class).in(ManageLifecycle.class);
          }
        }
    );

    final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);

    final TestInterface instance = injector.getInstance(TestInterface.class);

    Assertions.assertEquals(0, instance.getStarted());
    Assertions.assertEquals(0, instance.getStopped());
    Assertions.assertEquals(0, instance.getRan());

    instance.run();
    Assertions.assertEquals(0, instance.getStarted());
    Assertions.assertEquals(0, instance.getStopped());
    Assertions.assertEquals(1, instance.getRan());

    lifecycle.start();
    Assertions.assertEquals(2, instance.getStarted());
    Assertions.assertEquals(0, instance.getStopped());
    Assertions.assertEquals(1, instance.getRan());

    injector.getInstance(TestInterface.class).run();  // It's a singleton
    Assertions.assertEquals(2, instance.getStarted());
    Assertions.assertEquals(0, instance.getStopped());
    Assertions.assertEquals(2, instance.getRan());

    lifecycle.stop();
    Assertions.assertEquals(2, instance.getStarted());
    Assertions.assertEquals(2, instance.getStopped());
    Assertions.assertEquals(2, instance.getRan());
  }

  private interface TestInterface
  {
    void run();
    int getStarted();
    int getStopped();
    int getRan();
  }

  @ManageLifecycle
  public static class AnnotatedClass implements TestInterface
  {
    int started = 0;
    int stopped = 0;
    int ran = 0;

    @LifecycleStart
    public void start()
    {
      ++started;
    }

    @LifecycleStop
    public void stop()
    {
      ++stopped;
    }

    @Override
    public void run()
    {
      ++ran;
    }

    @Override
    public int getStarted()
    {
      return started;
    }

    @Override
    public int getStopped()
    {
      return stopped;
    }

    @Override
    public int getRan()
    {
      return ran;
    }
  }

  public static class ExplicitClass implements TestInterface
  {
    int started = 0;
    int stopped = 0;
    int ran = 0;

    @LifecycleStart
    public void start()
    {
      ++started;
    }

    @LifecycleStop
    public void stop()
    {
      ++stopped;
    }

    @Override
    public void run()
    {
      ++ran;
    }

    @Override
    public int getStarted()
    {
      return started;
    }

    @Override
    public int getStopped()
    {
      return stopped;
    }

    @Override
    public int getRan()
    {
      return ran;
    }
  }

}
