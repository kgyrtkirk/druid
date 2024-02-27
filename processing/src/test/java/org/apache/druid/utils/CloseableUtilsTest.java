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

package org.apache.druid.utils;

import com.google.common.base.Throwables;
import org.hamcrest.CoreMatchers;
import org.junit.internal.matchers.ThrowableCauseMatcher;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;

public class CloseableUtilsTest
{
  private final TestCloseable quietCloseable = new TestCloseable(null);
  private final TestCloseable quietCloseable2 = new TestCloseable(null);
  private final TestCloseable ioExceptionCloseable = new TestCloseable(new IOException());
  private final TestCloseable runtimeExceptionCloseable = new TestCloseable(new IllegalArgumentException());
  private final TestCloseable assertionErrorCloseable = new TestCloseable(new AssertionError());

  // For closeAndSuppressException tests.
  private final AtomicLong chomped = new AtomicLong();
  private final Consumer<Throwable> chomper = e -> chomped.incrementAndGet();

  @Test
  public void test_closeAll_array_quiet() throws IOException
  {
    CloseableUtils.closeAll(quietCloseable, null, quietCloseable2);
    assertClosed(quietCloseable, quietCloseable2);
  }

  @Test
  public void test_closeAll_list_quiet() throws IOException
  {
    CloseableUtils.closeAll(Arrays.asList(quietCloseable, null, quietCloseable2));
    assertClosed(quietCloseable, quietCloseable2);
  }

  @Test
  public void test_closeAll_array_loud()
  {
    Exception e = null;
    try {
      CloseableUtils.closeAll(quietCloseable, null, ioExceptionCloseable, quietCloseable2, runtimeExceptionCloseable);
    }
    catch (Exception e2) {
      e = e2;
    }

    assertClosed(quietCloseable, ioExceptionCloseable, quietCloseable2, runtimeExceptionCloseable);

    // First exception
    assertThat(e, CoreMatchers.instanceOf(IOException.class));

    // Second exception
    Assertions.assertEquals(1, e.getSuppressed().length);
    assertThat(e.getSuppressed()[0], CoreMatchers.instanceOf(IllegalArgumentException.class));
  }

  @Test
  public void test_closeAll_list_loud()
  {
    Exception e = null;
    try {
      CloseableUtils.closeAll(
          Arrays.asList(
              quietCloseable,
              null,
              ioExceptionCloseable,
              quietCloseable2,
              runtimeExceptionCloseable
          )
      );
    }
    catch (Exception e2) {
      e = e2;
    }

    assertClosed(quietCloseable, ioExceptionCloseable, quietCloseable2, runtimeExceptionCloseable);

    // First exception
    assertThat(e, CoreMatchers.instanceOf(IOException.class));

    // Second exception
    Assertions.assertEquals(1, e.getSuppressed().length);
    assertThat(e.getSuppressed()[0], CoreMatchers.instanceOf(IllegalArgumentException.class));
  }

  @Test
  public void test_closeAndWrapExceptions_null()
  {
    CloseableUtils.closeAndWrapExceptions(null);
    // Nothing happens.
  }

  @Test
  public void test_closeAndWrapExceptions_quiet()
  {
    CloseableUtils.closeAndWrapExceptions(quietCloseable);
    assertClosed(quietCloseable);
  }

  @Test
  public void test_closeAndWrapExceptions_ioException()
  {
    Exception e = null;
    try {
      CloseableUtils.closeAndWrapExceptions(ioExceptionCloseable);
    }
    catch (Exception e1) {
      e = e1;
    }

    assertClosed(ioExceptionCloseable);
    assertThat(e, CoreMatchers.instanceOf(RuntimeException.class));
  }

  @Test
  public void test_closeAndWrapExceptions_runtimeException()
  {
    Exception e = null;
    try {
      CloseableUtils.closeAndWrapExceptions(runtimeExceptionCloseable);
    }
    catch (Exception e1) {
      e = e1;
    }

    assertClosed(runtimeExceptionCloseable);
    assertThat(e, CoreMatchers.instanceOf(IllegalArgumentException.class));
  }

  @Test
  public void test_closeAndWrapExceptions_assertionError()
  {
    Throwable e = null;
    try {
      CloseableUtils.closeAndWrapExceptions(assertionErrorCloseable);
    }
    catch (Throwable e1) {
      e = e1;
    }

    assertClosed(assertionErrorCloseable);
    assertThat(e, CoreMatchers.instanceOf(AssertionError.class));
  }

  @Test
  public void test_closeAndSuppressExceptions_null()
  {
    CloseableUtils.closeAndSuppressExceptions(null, chomper);
    Assertions.assertEquals(0, chomped.get());
  }

  @Test
  public void test_closeAndSuppressExceptions_quiet()
  {
    CloseableUtils.closeAndSuppressExceptions(quietCloseable, chomper);
    assertClosed(quietCloseable);
    Assertions.assertEquals(0, chomped.get());
  }

  @Test
  public void test_closeAndSuppressExceptions_ioException()
  {
    CloseableUtils.closeAndSuppressExceptions(ioExceptionCloseable, chomper);
    assertClosed(ioExceptionCloseable);
    Assertions.assertEquals(1, chomped.get());
  }

  @Test
  public void test_closeAndSuppressExceptions_runtimeException()
  {
    CloseableUtils.closeAndSuppressExceptions(runtimeExceptionCloseable, chomper);
    assertClosed(runtimeExceptionCloseable);
    Assertions.assertEquals(1, chomped.get());
  }

  @Test
  public void test_closeAndSuppressExceptions_assertionError()
  {
    CloseableUtils.closeAndSuppressExceptions(assertionErrorCloseable, chomper);
    assertClosed(assertionErrorCloseable);
    Assertions.assertEquals(1, chomped.get());
  }

  @Test
  public void test_closeInCatch_improper()
  {
    Exception e = null;
    try {
      //noinspection ThrowableNotThrown
      CloseableUtils.closeInCatch(null, quietCloseable);
    }
    catch (Exception e1) {
      e = e1;
    }

    Assertions.assertTrue(quietCloseable.isClosed());

    assertThat(e, CoreMatchers.instanceOf(IllegalStateException.class));
    assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Must be called with non-null caught exception"))
    );
  }

  @Test
  public void test_closeInCatch_quiet()
  {
    Exception e = null;
    try {
      //noinspection ThrowableNotThrown
      CloseableUtils.closeInCatch(new RuntimeException("this one was caught"), quietCloseable);
    }
    catch (Exception e1) {
      e = e1;
    }

    Assertions.assertTrue(quietCloseable.isClosed());

    assertThat(e, CoreMatchers.instanceOf(RuntimeException.class));
    assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("this one was caught"))
    );
  }

  @Test
  public void test_closeInCatch_ioException()
  {
    Exception e = null;
    try {
      //noinspection ThrowableNotThrown
      CloseableUtils.closeInCatch(new IOException("this one was caught"), ioExceptionCloseable);
    }
    catch (Exception e1) {
      e = e1;
    }

    Assertions.assertTrue(ioExceptionCloseable.isClosed());

    // First exception
    assertThat(e, CoreMatchers.instanceOf(IOException.class));
    assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("this one was caught"))
    );

    // Second exception
    Assertions.assertEquals(1, e.getSuppressed().length);
    assertThat(e.getSuppressed()[0], CoreMatchers.instanceOf(IOException.class));
  }

  @Test
  public void test_closeInCatch_runtimeException()
  {
    Exception e = null;
    try {
      //noinspection ThrowableNotThrown
      CloseableUtils.closeInCatch(new RuntimeException("this one was caught"), runtimeExceptionCloseable);
    }
    catch (Exception e1) {
      e = e1;
    }

    Assertions.assertTrue(runtimeExceptionCloseable.isClosed());

    // First exception
    assertThat(e, CoreMatchers.instanceOf(RuntimeException.class));
    assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("this one was caught"))
    );

    // Second exception
    Assertions.assertEquals(1, e.getSuppressed().length);
    assertThat(e.getSuppressed()[0], CoreMatchers.instanceOf(IllegalArgumentException.class));
  }

  @Test
  public void test_closeAndWrapInCatch_improper()
  {
    Exception e = null;
    try {
      //noinspection ThrowableNotThrown
      CloseableUtils.closeAndWrapInCatch(null, quietCloseable);
    }
    catch (Exception e1) {
      e = e1;
    }

    Assertions.assertTrue(quietCloseable.isClosed());

    assertThat(e, CoreMatchers.instanceOf(IllegalStateException.class));
    assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Must be called with non-null caught exception"))
    );
  }

  @Test
  public void test_closeAndWrapInCatch_quiet()
  {
    Exception e = null;
    try {
      //noinspection ThrowableNotThrown
      CloseableUtils.closeAndWrapInCatch(new RuntimeException("this one was caught"), quietCloseable);
    }
    catch (Exception e1) {
      e = e1;
    }

    Assertions.assertTrue(quietCloseable.isClosed());

    assertThat(e, CoreMatchers.instanceOf(RuntimeException.class));
    assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("this one was caught"))
    );
  }

  @Test
  public void test_closeAndWrapInCatch_ioException()
  {
    Exception e = null;
    try {
      //noinspection ThrowableNotThrown
      CloseableUtils.closeAndWrapInCatch(new IOException("this one was caught"), ioExceptionCloseable);
    }
    catch (Exception e1) {
      e = e1;
    }

    Assertions.assertTrue(ioExceptionCloseable.isClosed());

    // First exception
    assertThat(e, CoreMatchers.instanceOf(RuntimeException.class));
    assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("java.io.IOException: this one was caught"))
    );
    assertThat(e, ThrowableCauseMatcher.hasCause(CoreMatchers.instanceOf(IOException.class)));
    assertThat(
        e,
        ThrowableCauseMatcher.hasCause(
            ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("this one was caught"))
        )
    );

    // Second exception
    Assertions.assertEquals(1, e.getCause().getSuppressed().length);
    assertThat(e.getCause().getSuppressed()[0], CoreMatchers.instanceOf(IOException.class));
  }

  @Test
  public void test_closeAndWrapInCatch_runtimeException()
  {
    Exception e = null;
    try {
      //noinspection ThrowableNotThrown
      CloseableUtils.closeAndWrapInCatch(new RuntimeException("this one was caught"), runtimeExceptionCloseable);
    }
    catch (Exception e1) {
      e = e1;
    }

    Assertions.assertTrue(runtimeExceptionCloseable.isClosed());

    // First exception
    assertThat(e, CoreMatchers.instanceOf(RuntimeException.class));
    assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("this one was caught"))
    );

    // Second exception
    Assertions.assertEquals(1, e.getSuppressed().length);
    assertThat(e.getSuppressed()[0], CoreMatchers.instanceOf(IllegalArgumentException.class));
  }

  @Test
  public void test_closeAndWrapInCatch_assertionError()
  {
    Exception e = null;
    try {
      //noinspection ThrowableNotThrown
      CloseableUtils.closeAndWrapInCatch(new RuntimeException("this one was caught"), assertionErrorCloseable);
    }
    catch (Exception e1) {
      e = e1;
    }

    Assertions.assertTrue(assertionErrorCloseable.isClosed());

    // First exception
    assertThat(e, CoreMatchers.instanceOf(RuntimeException.class));
    assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("this one was caught"))
    );

    // Second exception
    Assertions.assertEquals(1, e.getSuppressed().length);
    assertThat(e.getSuppressed()[0], CoreMatchers.instanceOf(AssertionError.class));
  }

  private static void assertClosed(final TestCloseable... closeables)
  {
    for (TestCloseable closeable : closeables) {
      Assertions.assertTrue(closeable.isClosed());
    }
  }

  private static class TestCloseable implements Closeable
  {
    @Nullable
    private final Throwable e;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    TestCloseable(@Nullable Throwable e)
    {
      this.e = e;
    }

    @Override
    public void close() throws IOException
    {
      closed.set(true);
      if (e != null) {
        Throwables.propagateIfInstanceOf(e, IOException.class);
        throw Throwables.propagate(e);
      }
    }

    public boolean isClosed()
    {
      return closed.get();
    }
  }
}
