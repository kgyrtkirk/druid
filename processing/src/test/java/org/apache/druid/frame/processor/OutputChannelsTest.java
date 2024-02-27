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

package org.apache.druid.frame.processor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.frame.allocation.HeapMemoryAllocator;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.hamcrest.CoreMatchers;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;

public class OutputChannelsTest
{
  @Test
  public void test_none()
  {
    final OutputChannels channels = OutputChannels.none();

    Assertions.assertEquals(IntSets.emptySet(), channels.getPartitionNumbers());
    Assertions.assertEquals(Collections.emptyList(), channels.getAllChannels());
    Assertions.assertEquals(Collections.emptyList(), channels.getChannelsForPartition(0));
    Assertions.assertTrue(channels.areReadableChannelsReady());
  }

  @Test
  public void test_wrap()
  {
    final OutputChannels channels = OutputChannels.wrap(ImmutableList.of(OutputChannel.nil(1)));

    Assertions.assertEquals(IntSet.of(1), channels.getPartitionNumbers());
    Assertions.assertEquals(1, channels.getAllChannels().size());
    Assertions.assertEquals(Collections.emptyList(), channels.getChannelsForPartition(0));
    Assertions.assertEquals(1, channels.getChannelsForPartition(1).size());
    Assertions.assertTrue(channels.areReadableChannelsReady());
  }

  @Test
  public void test_readOnly()
  {
    final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
    final OutputChannels channels = OutputChannels.wrap(
        ImmutableList.of(
            OutputChannel.immediatelyReadablePair(
                channel.writable(),
                HeapMemoryAllocator.unlimited(),
                channel.readable(),
                1
            )
        )
    );

    final OutputChannels readOnlyChannels = channels.readOnly();
    Assertions.assertEquals(IntSet.of(1), readOnlyChannels.getPartitionNumbers());
    Assertions.assertEquals(1, readOnlyChannels.getAllChannels().size());
    Assertions.assertEquals(1, channels.getChannelsForPartition(1).size());
    Assertions.assertTrue(channels.areReadableChannelsReady());

    final IllegalStateException e = Assertions.assertThrows(
        IllegalStateException.class,
        () -> Iterables.getOnlyElement(readOnlyChannels.getAllChannels()).getWritableChannel()
    );

    assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo(
            "Writable channel is not available. The output channel might be marked as read-only, hence no writes are allowed."))
    );

    final IllegalStateException e2 = Assertions.assertThrows(
        IllegalStateException.class,
        () -> Iterables.getOnlyElement(readOnlyChannels.getAllChannels()).getFrameMemoryAllocator()
    );

    assertThat(
        e2,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.equalTo(
            "Frame allocator is not available. The output channel might be marked as read-only, hence memory allocator is not required."))
    );
  }
}
