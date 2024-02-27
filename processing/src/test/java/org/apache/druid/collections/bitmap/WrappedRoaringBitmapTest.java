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

package org.apache.druid.collections.bitmap;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.IntIterator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class WrappedRoaringBitmapTest
{
  private static final int[] DATA = new int[]{1, 3, 5, 7, 9, 10, 11, 100, 122};

  private int cardinality;
  private WrappedRoaringBitmap bitmap;

  public void initWrappedRoaringBitmapTest(int cardinality)
  {
    this.cardinality = cardinality;
  }

  public static List<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (int i = 0; i < DATA.length; i++) {
      constructors.add(new Object[]{i});
    }
    return constructors;
  }

  @BeforeEach
  public void setUp()
  {
    bitmap = (WrappedRoaringBitmap) RoaringBitmapFactory.INSTANCE.makeEmptyMutableBitmap();
    for (int i = 0; i < cardinality; i++) {
      bitmap.add(DATA[i]);
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest
  public void testGet(int cardinality)
  {
    initWrappedRoaringBitmapTest(cardinality);
    for (int i = 0; i < DATA.length; i++) {
      Assertions.assertEquals(i < cardinality, bitmap.get(DATA[i]), String.valueOf(i));
    }

    Assertions.assertFalse(bitmap.get(-1));
    Assertions.assertFalse(bitmap.get(Integer.MAX_VALUE));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest
  public void testSize(int cardinality)
  {
    initWrappedRoaringBitmapTest(cardinality);
    Assertions.assertEquals(cardinality, bitmap.size());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest
  public void testRemove(int cardinality)
  {
    initWrappedRoaringBitmapTest(cardinality);
    bitmap.remove(Integer.MAX_VALUE);
    Assertions.assertEquals(cardinality, bitmap.size());

    if (cardinality > 0) {
      bitmap.remove(DATA[0]);
      Assertions.assertEquals(cardinality - 1, bitmap.size());
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest
  public void testClear(int cardinality)
  {
    initWrappedRoaringBitmapTest(cardinality);
    bitmap.clear();
    Assertions.assertEquals(0, bitmap.size());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest
  public void testIterator(int cardinality)
  {
    initWrappedRoaringBitmapTest(cardinality);
    final IntIterator iterator = bitmap.iterator();

    int i = 0;
    while (iterator.hasNext()) {
      final int n = iterator.next();
      Assertions.assertEquals(DATA[i], n, String.valueOf(i));
      i++;
    }
    Assertions.assertEquals(i, cardinality, "number of elements");
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest
  public void testSerialize(int cardinality)
  {
    initWrappedRoaringBitmapTest(cardinality);
    byte[] buffer = new byte[bitmap.getSizeInBytes()];
    ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
    bitmap.serialize(byteBuffer);
    byteBuffer.flip();
    ImmutableBitmap immutableBitmap = new RoaringBitmapFactory().mapImmutableBitmap(byteBuffer);
    Assertions.assertEquals(cardinality, immutableBitmap.size());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest
  public void testToByteArray(int cardinality)
  {
    initWrappedRoaringBitmapTest(cardinality);
    ImmutableBitmap immutableBitmap = new RoaringBitmapFactory().mapImmutableBitmap(ByteBuffer.wrap(bitmap.toBytes()));
    Assertions.assertEquals(cardinality, immutableBitmap.size());
  }
}
