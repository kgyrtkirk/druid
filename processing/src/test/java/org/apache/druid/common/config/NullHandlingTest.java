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

package org.apache.druid.common.config;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.druid.common.config.NullHandling.defaultValueForClass;
import static org.apache.druid.common.config.NullHandling.defaultValueForType;
import static org.apache.druid.common.config.NullHandling.replaceWithDefault;
import static org.junit.jupiter.api.Assertions.assertEquals;

public final class NullHandlingTest extends InitializedNullHandlingTest
{
  @Test
  public void test_defaultValueForClass_float()
  {
    assertEquals(
        replaceWithDefault() ? 0f : null,
        NullHandling.defaultValueForClass(Float.class)
    );
  }

  @Test
  public void test_defaultValueForClass_double()
  {
    assertEquals(
        replaceWithDefault() ? 0d : null,
        NullHandling.defaultValueForClass(Double.class)
    );
  }

  @Test
  public void test_defaultValueForClass_integer()
  {
    Assertions.assertNull(NullHandling.defaultValueForClass(Integer.class));
  }

  @Test
  public void test_defaultValueForClass_long()
  {
    assertEquals(
        replaceWithDefault() ? 0L : null,
        NullHandling.defaultValueForClass(Long.class)
    );
  }

  @Test
  public void test_defaultValueForClass_number()
  {
    assertEquals(
        replaceWithDefault() ? 0d : null,
        NullHandling.defaultValueForClass(Number.class)
    );
  }

  @Test
  public void test_defaultValueForClass_string()
  {
    assertEquals(
        replaceWithDefault() ? "" : null,
        NullHandling.defaultValueForClass(String.class)
    );
  }

  @Test
  public void test_defaultValueForClass_object()
  {
    Assertions.assertNull(NullHandling.defaultValueForClass(Object.class));
  }

  @Test
  public void test_defaultValueForType()
  {
    assertEquals(defaultValueForClass(Float.class), defaultValueForType(ValueType.FLOAT));
    assertEquals(defaultValueForClass(Double.class), defaultValueForType(ValueType.DOUBLE));
    assertEquals(defaultValueForClass(Long.class), defaultValueForType(ValueType.LONG));
    assertEquals(defaultValueForClass(String.class), defaultValueForType(ValueType.STRING));
    assertEquals(defaultValueForClass(Object.class), defaultValueForType(ValueType.COMPLEX));
    assertEquals(defaultValueForClass(Object.class), defaultValueForType(ValueType.ARRAY));
  }

  @Test
  public void test_ignoreNullsStrings()
  {
    try {
      NullHandling.initializeForTestsWithValues(false, true);
      Assertions.assertFalse(NullHandling.ignoreNullsForStringCardinality());

      NullHandling.initializeForTestsWithValues(true, false);
      Assertions.assertFalse(NullHandling.ignoreNullsForStringCardinality());
    }
    finally {
      NullHandling.initializeForTests();
    }
  }

  @Test
  public void test_mustCombineNullAndEmptyInDictionary()
  {
    Assertions.assertFalse(
        NullHandling.mustCombineNullAndEmptyInDictionary(
            new ListIndexed<>(Collections.singletonList(null))
        )
    );

    Assertions.assertFalse(
        NullHandling.mustCombineNullAndEmptyInDictionary(
            new ListIndexed<>(StringUtils.toUtf8ByteBuffer("foo"))
        )
    );

    Assertions.assertFalse(
        NullHandling.mustCombineNullAndEmptyInDictionary(
            new ListIndexed<>(StringUtils.toUtf8ByteBuffer(""))
        )
    );

    Assertions.assertFalse(
        NullHandling.mustCombineNullAndEmptyInDictionary(
            new ListIndexed<>(StringUtils.toUtf8ByteBuffer(""), StringUtils.toUtf8ByteBuffer("foo"))
        )
    );

    Assertions.assertEquals(
        NullHandling.replaceWithDefault(),
        NullHandling.mustCombineNullAndEmptyInDictionary(
            new ListIndexed<>(null, StringUtils.toUtf8ByteBuffer(""))
        )
    );

    Assertions.assertEquals(
        NullHandling.replaceWithDefault(),
        NullHandling.mustCombineNullAndEmptyInDictionary(
            new ListIndexed<>(null, StringUtils.toUtf8ByteBuffer(""), StringUtils.toUtf8ByteBuffer("foo")))
    );
  }

  @Test
  public void test_mustReplaceFirstValueWithNullInDictionary()
  {
    Assertions.assertFalse(
        NullHandling.mustReplaceFirstValueWithNullInDictionary(
            new ListIndexed<>(Collections.singletonList(null))
        )
    );

    Assertions.assertFalse(
        NullHandling.mustReplaceFirstValueWithNullInDictionary(
            new ListIndexed<>(StringUtils.toUtf8ByteBuffer("foo"))
        )
    );

    Assertions.assertEquals(
        NullHandling.replaceWithDefault(),
        NullHandling.mustReplaceFirstValueWithNullInDictionary(
            new ListIndexed<>(StringUtils.toUtf8ByteBuffer(""))
        )
    );

    Assertions.assertEquals(
        NullHandling.replaceWithDefault(),
        NullHandling.mustReplaceFirstValueWithNullInDictionary(
            new ListIndexed<>(StringUtils.toUtf8ByteBuffer(""), StringUtils.toUtf8ByteBuffer("foo"))
        )
    );

    Assertions.assertFalse(
        NullHandling.mustReplaceFirstValueWithNullInDictionary(
            new ListIndexed<>(null, StringUtils.toUtf8ByteBuffer(""))
        )
    );

    Assertions.assertFalse(
        NullHandling.mustReplaceFirstValueWithNullInDictionary(
            new ListIndexed<>(null, StringUtils.toUtf8ByteBuffer(""), StringUtils.toUtf8ByteBuffer("foo")))
    );
  }
}
