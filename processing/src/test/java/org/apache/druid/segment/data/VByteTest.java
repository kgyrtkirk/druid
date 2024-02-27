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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collection;

public class VByteTest
{
  public static Collection<Object[]> constructorFeeder()
  {
    return ImmutableList.of(new Object[]{ByteOrder.LITTLE_ENDIAN}, new Object[]{ByteOrder.BIG_ENDIAN});
  }

  private ByteOrder order;

  public void initVByteTest(ByteOrder byteOrder)
  {
    this.order = byteOrder;
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testVbyte(ByteOrder byteOrder)
  {
    initVByteTest(byteOrder);
    ByteBuffer buffer = ByteBuffer.allocate(24).order(order);
    roundTrip(buffer, 0, 0, 1);
    roundTrip(buffer, 0, 4, 1);
    roundTrip(buffer, 0, 224, 2);
    roundTrip(buffer, 0, 1024, 2);
    roundTrip(buffer, 0, 1 << 14 - 1, 2);
    roundTrip(buffer, 0, 1 << 14, 3);
    roundTrip(buffer, 0, 1 << 16, 3);
    roundTrip(buffer, 0, 1 << 25, 4);
    roundTrip(buffer, 0, 1 << 28 - 1, 4);
    roundTrip(buffer, 0, 1 << 28, 5);
    roundTrip(buffer, 0, Integer.MAX_VALUE, 5);
  }

  private static void roundTrip(ByteBuffer buffer, int position, int value, int expectedSize)
  {
    Assertions.assertEquals(expectedSize, VByte.computeIntSize(value));
    buffer.position(position);
    VByte.writeInt(buffer, value);
    Assertions.assertEquals(expectedSize, buffer.position() - position);
    buffer.position(position);
    Assertions.assertEquals(value, VByte.readInt(buffer));
    Assertions.assertEquals(expectedSize, buffer.position() - position);
  }
}
