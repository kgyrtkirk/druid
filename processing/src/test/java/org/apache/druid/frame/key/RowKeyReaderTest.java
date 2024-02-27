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

package org.apache.druid.frame.key;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;

public class RowKeyReaderTest extends InitializedNullHandlingTest
{
  private final RowSignature signature =
      RowSignature.builder()
                  .add("long", ColumnType.LONG)
                  .add("longDefault", ColumnType.LONG)
                  .add("float", ColumnType.FLOAT)
                  .add("floatDefault", ColumnType.FLOAT)
                  .add("string", ColumnType.STRING)
                  .add("stringNull", ColumnType.STRING)
                  .add("multiValueString", ColumnType.STRING)
                  .add("double", ColumnType.DOUBLE)
                  .add("doubleDefault", ColumnType.DOUBLE)
                  .add("stringArray", ColumnType.STRING_ARRAY)
                  .build();

  private final List<Object> objects = Arrays.asList(
      5L,
      NullHandling.defaultLongValue(),
      6f,
      NullHandling.defaultFloatValue(),
      "foo",
      null,
      Arrays.asList("bar", "qux"),
      7d,
      NullHandling.defaultDoubleValue(),
      new Object[]{"abc", "xyz"}
  );

  private final RowKey key = KeyTestUtils.createKey(signature, objects.toArray());

  private final RowKeyReader keyReader = RowKeyReader.create(signature);

  @Test
  public void test_read_all()
  {
    FrameTestUtil.assertRowEqual(objects, keyReader.read(key));
  }

  @Test
  public void test_read_oneField()
  {
    for (int i = 0; i < signature.size(); i++) {
      final Object keyPart = keyReader.read(key, i);

      if (objects.get(i) instanceof Object[]) {
        assertThat(keyPart, CoreMatchers.instanceOf(Object[].class));
        Assertions.assertArrayEquals(
            (Object[]) objects.get(i),
            (Object[]) keyPart,
            "read: " + signature.getColumnName(i)
        );
      } else {
        Assertions.assertEquals(
            objects.get(i),
            keyPart,
            "read: " + signature.getColumnName(i)
        );
      }
    }
  }

  @Test
  public void test_hasMultipleValues()
  {
    for (int i = 0; i < signature.size(); i++) {
      Assertions.assertEquals(
          objects.get(i) instanceof List || objects.get(i) instanceof Object[],
          keyReader.hasMultipleValues(key, i),
          "hasMultipleValues: " + signature.getColumnName(i)
      );
    }
  }

  @Test
  public void test_trim_zero()
  {
    Assertions.assertEquals(RowKey.empty(), keyReader.trim(key, 0));
  }

  @Test
  public void test_trim_one()
  {
    Assertions.assertEquals(
        KeyTestUtils.createKey(
            RowSignature.builder().add(signature.getColumnName(0), signature.getColumnType(0).get()).build(),
            objects.get(0)
        ),
        keyReader.trim(key, 1)
    );
  }

  @Test
  public void test_trim_oneLessThanFullLength()
  {
    final int numFields = signature.size() - 1;
    RowSignature.Builder trimmedSignature = RowSignature.builder();
    IntStream.range(0, numFields)
             .forEach(i -> trimmedSignature.add(signature.getColumnName(i), signature.getColumnType(i).get()));

    Assertions.assertEquals(
        KeyTestUtils.createKey(trimmedSignature.build(), objects.subList(0, numFields).toArray()),
        keyReader.trim(key, numFields)
    );
  }

  @Test
  public void test_trim_fullLength()
  {
    Assertions.assertEquals(key, keyReader.trim(key, signature.size()));
  }

  @Test
  public void test_trim_beyondFullLength()
  {
    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> keyReader.trim(key, signature.size() + 1)
    );

    assertThat(e, ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Cannot trim")));
  }

  @Test
  public void test_trimmedKeyReader_zero()
  {
    RowKey trimmedKey = keyReader.trim(key, 0);
    RowKeyReader trimmedKeyReader = keyReader.trimmedKeyReader(0);

    Assertions.assertEquals(
        Collections.emptyList(),
        trimmedKeyReader.read(trimmedKey)
    );
  }

  @Test
  public void test_trimmedKeyReader_one()
  {
    RowKey trimmedKey = keyReader.trim(key, 1);
    RowKeyReader trimmedKeyReader = keyReader.trimmedKeyReader(1);

    Assertions.assertEquals(
        objects.subList(0, 1),
        trimmedKeyReader.read(trimmedKey)
    );
  }

  @Test
  public void test_trimmedKeyReader_oneLessThanFullLength()
  {
    final int numFields = signature.size() - 1;
    RowKey trimmedKey = keyReader.trim(key, numFields);
    RowKeyReader trimmedKeyReader = keyReader.trimmedKeyReader(numFields);

    Assertions.assertEquals(
        objects.subList(0, numFields),
        trimmedKeyReader.read(trimmedKey)
    );
  }
}
