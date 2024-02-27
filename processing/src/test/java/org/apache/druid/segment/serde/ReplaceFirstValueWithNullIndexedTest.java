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

package org.apache.druid.segment.serde;

import com.google.common.collect.Lists;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test for {@link ReplaceFirstValueWithNullIndexed}.
 */
public class ReplaceFirstValueWithNullIndexedTest extends InitializedNullHandlingTest
{
  @Test
  public void testSizeZero()
  {
    final IllegalStateException e = Assertions.assertThrows(
        IllegalStateException.class,
        () -> new ReplaceFirstValueWithNullIndexed<>(Indexed.empty())
    );

    assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Size[0] must be >= 1"))
    );
  }

  @Test
  public void testSizeOne()
  {
    final ReplaceFirstValueWithNullIndexed<String> indexed =
        new ReplaceFirstValueWithNullIndexed<>(new ListIndexed<>("bar"));

    Assertions.assertEquals(0, indexed.indexOf(null));
    Assertions.assertEquals(-2, indexed.indexOf(""));
    Assertions.assertEquals(-2, indexed.indexOf("foo"));
    Assertions.assertEquals(-2, indexed.indexOf("bar"));
    Assertions.assertEquals(-2, indexed.indexOf("baz"));
    Assertions.assertEquals(-2, indexed.indexOf("qux"));
    Assertions.assertEquals(1, indexed.size());
    Assertions.assertNull(indexed.get(0));
    Assertions.assertFalse(indexed.isSorted()); // Matches delegate. See class doc for ReplaceFirstValueWithNullIndexed.
    Assertions.assertEquals(Collections.singletonList(null), Lists.newArrayList(indexed));
  }

  @Test
  public void testSizeTwo()
  {
    final ReplaceFirstValueWithNullIndexed<String> indexed =
        new ReplaceFirstValueWithNullIndexed<>(new ListIndexed<>("bar", "foo"));

    Assertions.assertEquals(0, indexed.indexOf(null));
    Assertions.assertEquals(1, indexed.indexOf("foo"));
    Assertions.assertEquals(-2, indexed.indexOf(""));
    Assertions.assertEquals(-2, indexed.indexOf("bar"));
    Assertions.assertEquals(-2, indexed.indexOf("baz"));
    Assertions.assertEquals(-2, indexed.indexOf("qux"));
    Assertions.assertEquals(2, indexed.size());
    Assertions.assertNull(indexed.get(0));
    Assertions.assertEquals("foo", indexed.get(1));
    Assertions.assertFalse(indexed.isSorted()); // Matches delegate. See class doc for ReplaceFirstValueWithNullIndexed.
    Assertions.assertEquals(Lists.newArrayList(null, "foo"), Lists.newArrayList(indexed));
  }

  @Test
  public void testSizeOneSorted()
  {
    final ReplaceFirstValueWithNullIndexed<String> indexed =
        new ReplaceFirstValueWithNullIndexed<>(
            GenericIndexed.fromArray(
                new String[]{"bar"},
                GenericIndexed.STRING_STRATEGY
            )
        );

    Assertions.assertEquals(0, indexed.indexOf(null));
    Assertions.assertEquals(-2, indexed.indexOf(""));
    Assertions.assertEquals(-2, indexed.indexOf("foo"));
    Assertions.assertEquals(-2, indexed.indexOf("bar"));
    Assertions.assertEquals(-2, indexed.indexOf("baz"));
    Assertions.assertEquals(-2, indexed.indexOf("qux"));
    Assertions.assertEquals(1, indexed.size());
    Assertions.assertNull(indexed.get(0));
    Assertions.assertTrue(indexed.isSorted()); // Matches delegate. See class doc for ReplaceFirstValueWithNullIndexed.
    Assertions.assertEquals(Collections.singletonList(null), Lists.newArrayList(indexed));
  }

  @Test
  public void testSizeTwoSorted()
  {
    final ReplaceFirstValueWithNullIndexed<String> indexed =
        new ReplaceFirstValueWithNullIndexed<>(
            GenericIndexed.fromArray(
                new String[]{"bar", "foo"},
                GenericIndexed.STRING_STRATEGY
            )
        );

    Assertions.assertEquals(0, indexed.indexOf(null));
    Assertions.assertEquals(1, indexed.indexOf("foo"));
    Assertions.assertEquals(-2, indexed.indexOf(""));
    Assertions.assertEquals(-2, indexed.indexOf("bar"));
    Assertions.assertEquals(-2, indexed.indexOf("baz"));
    Assertions.assertEquals(-3, indexed.indexOf("qux"));
    Assertions.assertEquals(2, indexed.size());
    Assertions.assertNull(indexed.get(0));
    Assertions.assertEquals("foo", indexed.get(1));
    Assertions.assertTrue(indexed.isSorted()); // Matches delegate. See class doc for ReplaceFirstValueWithNullIndexed.
    Assertions.assertEquals(Lists.newArrayList(null, "foo"), Lists.newArrayList(indexed));
  }
}
