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

package org.apache.druid.data.input.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

public class JsonLineReaderTest
{
  @Test
  public void testParseRow() throws IOException
  {
    final JsonInputFormat format = new JsonInputFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz", "baz"),
                new JSONPathFieldSpec(JSONPathFieldType.ROOT, "root_baz2", "baz2"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg", "$.o.mg"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg2", "$.o.mg2"),
                new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg", ".o.mg"),
                new JSONPathFieldSpec(JSONPathFieldType.JQ, "jq_omg2", ".o.mg2"),
                new JSONPathFieldSpec(JSONPathFieldType.TREE, "tree_baz", null, Collections.singletonList("baz")),
                new JSONPathFieldSpec(JSONPathFieldType.TREE, "tree_baz2", null, Collections.singletonList("baz2")),
                new JSONPathFieldSpec(JSONPathFieldType.TREE, "tree_omg", null, Arrays.asList("o", "mg")),
                new JSONPathFieldSpec(JSONPathFieldType.TREE, "tree_omg2", null, Arrays.asList("o", "mg2"))
            )
        ),
        null,
        null,
        null,
        null
    );

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":1}}")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("bar", "foo"))),
            ColumnsFilter.all()
        ),
        source,
        null
    );
    final int numExpectedIterations = 1;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {
        final InputRow row = iterator.next();
        Assertions.assertEquals(DateTimes.of("2019-01-01"), row.getTimestamp());
        Assertions.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("tree_baz")));
        Assertions.assertEquals("1", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assertions.assertEquals("1", Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assertions.assertEquals("1", Iterables.getOnlyElement(row.getDimension("tree_omg")));

        Assertions.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assertions.assertTrue(row.getDimension("tree_baz2").isEmpty());
        Assertions.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assertions.assertTrue(row.getDimension("jq_omg2").isEmpty());
        Assertions.assertTrue(row.getDimension("tree_omg2").isEmpty());
        numActualIterations++;
      }
      Assertions.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testParseRowWithConditional() throws IOException
  {
    final JsonInputFormat format = new JsonInputFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "foo", "$.[?(@.maybe_object)].maybe_object.foo.test"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "baz", "$.maybe_object_2.foo.test"),
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "bar", "$.[?(@.something_else)].something_else.foo")
            )
        ),
        null,
        null,
        null,
        null
    );

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"something_else\": {\"foo\": \"test\"}}")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("foo"))),
            ColumnsFilter.all()
        ),
        source,
        null
    );

    final int numExpectedIterations = 1;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {
        final InputRow row = iterator.next();
        Assertions.assertEquals("test", Iterables.getOnlyElement(row.getDimension("bar")));
        // Since foo is in the JSONPathSpec it comes as an array of [null]
        // row.getRaw("foo") comes out as an array of nulls but the
        // row.getDimension("foo") stringifies it as "null". A future developer should aim to relieve this
        Assertions.assertEquals(Collections.singletonList(null), row.getRaw("foo"));
        Assertions.assertEquals(Collections.singletonList("null"), row.getDimension("foo"));
        Assertions.assertTrue(row.getDimension("baz").isEmpty());
        numActualIterations++;
      }
      Assertions.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testParseRowKeepNullColumns() throws IOException
  {
    final JsonInputFormat format = new JsonInputFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg", "$.o.mg"),
                new JSONPathFieldSpec(JSONPathFieldType.TREE, "tree_omg", null, Arrays.asList("o", "mg"))
            )
        ),
        null,
        true,
        null,
        null
    );

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"o\":{\"mg\":null}}")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Collections.emptyList())),
            ColumnsFilter.all()
        ),
        source,
        null
    );
    final int numExpectedIterations = 1;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {
        final InputRow row = iterator.next();
        Assertions.assertEquals(Arrays.asList("path_omg", "tree_omg", "bar", "foo"), row.getDimensions());
        Assertions.assertTrue(row.getDimension("bar").isEmpty());
        Assertions.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assertions.assertTrue(row.getDimension("path_omg").isEmpty());
        Assertions.assertTrue(row.getDimension("tree_omg").isEmpty());
        numActualIterations++;
      }
      Assertions.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testKeepNullColumnsWithNoNullValues() throws IOException
  {
    final JsonInputFormat format = new JsonInputFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg", "$.o.mg"),
                new JSONPathFieldSpec(JSONPathFieldType.TREE, "tree_omg", null, Arrays.asList("o", "mg"))
            )
        ),
        null,
        true,
        null,
        null
    );

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":1,\"foo\":\"x\",\"o\":{\"mg\":\"a\"}}")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Collections.emptyList())),
            ColumnsFilter.all()
        ),
        source,
        null
    );
    final int numExpectedIterations = 1;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {
        final InputRow row = iterator.next();
        Assertions.assertEquals(Arrays.asList("path_omg", "tree_omg", "bar", "foo"), row.getDimensions());
        Assertions.assertEquals("1", Iterables.getOnlyElement(row.getDimension("bar")));
        Assertions.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assertions.assertEquals("a", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assertions.assertEquals("a", Iterables.getOnlyElement(row.getDimension("tree_omg")));
        numActualIterations++;
      }
      Assertions.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testFalseKeepNullColumns() throws IOException
  {
    final JsonInputFormat format = new JsonInputFormat(
        new JSONPathSpec(
            true,
            ImmutableList.of(
                new JSONPathFieldSpec(JSONPathFieldType.PATH, "path_omg", "$.o.mg"),
                new JSONPathFieldSpec(JSONPathFieldType.TREE, "tree_omg", null, Arrays.asList("o", "mg"))
            )
        ),
        null,
        false,
        null,
        null
    );

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"o\":{\"mg\":\"a\"}}")
    );

    final InputEntityReader reader = format.createReader(
        new InputRowSchema(
            new TimestampSpec("timestamp", "iso", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Collections.emptyList())),
            ColumnsFilter.all()
        ),
        source,
        null
    );
    final int numExpectedIterations = 1;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {
        final InputRow row = iterator.next();
        Assertions.assertEquals(Arrays.asList("path_omg", "tree_omg", "foo"), row.getDimensions());
        Assertions.assertTrue(row.getDimension("bar").isEmpty());
        Assertions.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assertions.assertEquals("a", Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assertions.assertEquals("a", Iterables.getOnlyElement(row.getDimension("tree_omg")));
        numActualIterations++;
      }
      Assertions.assertEquals(numExpectedIterations, numActualIterations);
    }
  }
}
