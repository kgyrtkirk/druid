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

package org.apache.druid.segment.data;

import com.google.common.collect.ImmutableList;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Iterator;


public class FixedIndexedTest extends InitializedNullHandlingTest
{
  private static final Long[] LONGS = new Long[64];

  public static Collection<Object[]> constructorFeeder()
  {
    return ImmutableList.of(new Object[]{ByteOrder.LITTLE_ENDIAN}, new Object[]{ByteOrder.BIG_ENDIAN});
  }

  @BeforeAll
  public static void setup()
  {
    for (int i = 0; i < LONGS.length; i++) {
      LONGS[i] = i * 10L;
    }
  }

  private ByteOrder order;

  public void initFixedIndexedTest(ByteOrder byteOrder)
  {
    this.order = byteOrder;
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testGet(ByteOrder byteOrder) throws IOException
  {
    initFixedIndexedTest(byteOrder);
    ByteBuffer buffer = ByteBuffer.allocate(1 << 14);
    fillBuffer(buffer, order, false);
    FixedIndexed<Long> fixedIndexed =
        FixedIndexed.<Long>read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES).get();
    Assertions.assertEquals(64, fixedIndexed.size());
    for (int i = 0; i < LONGS.length; i++) {
      Assertions.assertEquals(LONGS[i], fixedIndexed.get(i));
      Assertions.assertEquals(i, fixedIndexed.indexOf(LONGS[i]));
    }

    Assertions.assertThrows(IllegalArgumentException.class, () -> fixedIndexed.get(-1));
    Assertions.assertThrows(IllegalArgumentException.class, () -> fixedIndexed.get(LONGS.length));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testIterator(ByteOrder byteOrder) throws IOException
  {
    initFixedIndexedTest(byteOrder);
    ByteBuffer buffer = ByteBuffer.allocate(1 << 14);
    fillBuffer(buffer, order, false);
    FixedIndexed<Long> fixedIndexed =
        FixedIndexed.<Long>read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES).get();
    Iterator<Long> iterator = fixedIndexed.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      Assertions.assertEquals(LONGS[i++], iterator.next());
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testGetWithNull(ByteOrder byteOrder) throws IOException
  {
    initFixedIndexedTest(byteOrder);
    ByteBuffer buffer = ByteBuffer.allocate(1 << 14);
    fillBuffer(buffer, order, true);
    FixedIndexed<Long> fixedIndexed =
        FixedIndexed.<Long>read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES).get();
    Assertions.assertEquals(65, fixedIndexed.size());
    Assertions.assertNull(fixedIndexed.get(0));
    for (int i = 0; i < LONGS.length; i++) {
      Assertions.assertEquals(LONGS[i], fixedIndexed.get(i + 1));
      Assertions.assertEquals(i + 1, fixedIndexed.indexOf(LONGS[i]));
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testIteratorWithNull(ByteOrder byteOrder) throws IOException
  {
    initFixedIndexedTest(byteOrder);
    ByteBuffer buffer = ByteBuffer.allocate(1 << 14);
    fillBuffer(buffer, order, true);
    FixedIndexed<Long> fixedIndexed =
        FixedIndexed.<Long>read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES).get();
    Iterator<Long> iterator = fixedIndexed.iterator();
    Assertions.assertNull(iterator.next());
    int i = 0;
    while (iterator.hasNext()) {
      Assertions.assertEquals(LONGS[i++], iterator.next());
    }
  }

  private static void fillBuffer(ByteBuffer buffer, ByteOrder order, boolean withNull) throws IOException
  {
    buffer.position(0);
    FixedIndexedWriter<Long> writer = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        ColumnType.LONG.getStrategy(),
        order,
        Long.BYTES,
        true
    );
    writer.open();
    if (withNull) {
      writer.write(null);
    }
    for (Long aLong : LONGS) {
      writer.write(aLong);
    }
    Iterator<Long> longIterator = writer.getIterator();
    int ctr = 0;
    int totalCount = withNull ? 1 + LONGS.length : LONGS.length;
    for (int i = 0; i < totalCount; i++) {
      if (withNull) {
        if (i == 0) {
          Assertions.assertNull(writer.get(i));
        } else {
          Assertions.assertEquals(LONGS[i - 1], writer.get(i), " index: " + i);
        }
      } else {
        Assertions.assertEquals(LONGS[i], writer.get(i), " index: " + i);
      }
    }
    while (longIterator.hasNext()) {
      if (withNull) {
        if (ctr == 0) {
          Assertions.assertNull(longIterator.next());
          Assertions.assertNull(writer.get(ctr));
        } else {
          Assertions.assertEquals(LONGS[ctr - 1], longIterator.next());
          Assertions.assertEquals(LONGS[ctr - 1], writer.get(ctr));
        }
      } else {
        Assertions.assertEquals(LONGS[ctr], longIterator.next());
        Assertions.assertEquals(LONGS[ctr], writer.get(ctr));
      }
      ctr++;
    }
    WritableByteChannel channel = new WritableByteChannel()
    {
      @Override
      public int write(ByteBuffer src)
      {
        int size = src.remaining();
        buffer.put(src);
        return size;
      }

      @Override
      public boolean isOpen()
      {
        return true;
      }

      @Override
      public void close()
      {
      }
    };
    long size = writer.getSerializedSize();
    buffer.position(0);
    writer.writeTo(channel, null);
    Assertions.assertEquals(size, buffer.position());
    buffer.position(0);
  }
}
