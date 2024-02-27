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
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.JSONPathFieldSpec;
import org.apache.druid.java.util.common.parsers.JSONPathFieldType;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class JsonReaderTest
{

  @Test
  public void testParseMultipleRows() throws IOException
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
        false, //make sure JsonReader is used
        false,
        false
    );

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":1}}"
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":2}}\n"
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":3}}\n")
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

    final int numExpectedIterations = 3;
    try (CloseableIterator<InputRow> iterator = reader.read()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {

        final InputRow row = iterator.next();

        final String msgId = String.valueOf(++numActualIterations);
        Assertions.assertEquals(DateTimes.of("2019-01-01"), row.getTimestamp());
        Assertions.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
        Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("tree_baz")));
        Assertions.assertEquals(msgId, Iterables.getOnlyElement(row.getDimension("path_omg")));
        Assertions.assertEquals(msgId, Iterables.getOnlyElement(row.getDimension("jq_omg")));
        Assertions.assertEquals(msgId, Iterables.getOnlyElement(row.getDimension("tree_omg")));

        Assertions.assertTrue(row.getDimension("root_baz2").isEmpty());
        Assertions.assertTrue(row.getDimension("tree_baz2").isEmpty());
        Assertions.assertTrue(row.getDimension("path_omg2").isEmpty());
        Assertions.assertTrue(row.getDimension("jq_omg2").isEmpty());
        Assertions.assertTrue(row.getDimension("tree_omg2").isEmpty());
      }

      Assertions.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testParsePrettyFormatJSON() throws IOException
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
        false, //make sure JsonReader is used
        false,
        false
    );

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\n"
                           + "    \"timestamp\": \"2019-01-01\",\n"
                           + "    \"bar\": null,\n"
                           + "    \"foo\": \"x\",\n"
                           + "    \"baz\": 4,\n"
                           + "    \"o\": {\n"
                           + "        \"mg\": 1\n"
                           + "    }\n"
                           + "}")
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
  public void testInvalidJSONText() throws IOException
  {
    assertThrows(ParseException.class, () -> {
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
          false, //make sure JsonReader is used
          false,
          false
      );

      final ByteEntity source = new ByteEntity(
          StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":1}}"
              + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4xxx,\"o\":{\"mg\":2}}"
              //baz property is illegal
              + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":3}}")
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

      // the 2nd line is ill-formed, so the parse of this text chunk aborts
      final int numExpectedIterations = 0;

      try (CloseableIterator<InputRow> iterator = reader.read()) {
        int numActualIterations = 0;
        while (iterator.hasNext()) {
          iterator.next();
          ++numActualIterations;
        }

        Assertions.assertEquals(numExpectedIterations, numActualIterations);
      }
    });
  }

  @Test
  public void testSampleMultipleRows() throws IOException
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
        false, //make sure JsonReader is used
        false,
        false
    );

    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":1}}"
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":2}}\n"
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":3}}\n")
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

    int acturalRowCount = 0;
    try (CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample()) {
      while (iterator.hasNext()) {

        final InputRowListPlusRawValues rawValues = iterator.next();

        // 3 rows returned together
        Assertions.assertEquals(3, rawValues.getInputRows().size());

        for (int i = 0; i < 3; i++) {
          InputRow row = rawValues.getInputRows().get(i);

          final String msgId = String.valueOf(++acturalRowCount);
          Assertions.assertEquals(DateTimes.of("2019-01-01"), row.getTimestamp());
          Assertions.assertEquals("x", Iterables.getOnlyElement(row.getDimension("foo")));
          Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("baz")));
          Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("root_baz")));
          Assertions.assertEquals("4", Iterables.getOnlyElement(row.getDimension("tree_baz")));
          Assertions.assertEquals(msgId, Iterables.getOnlyElement(row.getDimension("path_omg")));
          Assertions.assertEquals(msgId, Iterables.getOnlyElement(row.getDimension("jq_omg")));
          Assertions.assertEquals(msgId, Iterables.getOnlyElement(row.getDimension("tree_omg")));

          Assertions.assertTrue(row.getDimension("root_baz2").isEmpty());
          Assertions.assertTrue(row.getDimension("tree_baz2").isEmpty());
          Assertions.assertTrue(row.getDimension("path_omg2").isEmpty());
          Assertions.assertTrue(row.getDimension("jq_omg2").isEmpty());
          Assertions.assertTrue(row.getDimension("tree_omg2").isEmpty());
        }
      }
    }

    Assertions.assertEquals(3, acturalRowCount);
  }

  @Test
  public void testSamplInvalidJSONText() throws IOException
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
        false, //make sure JsonReader is used
        false,
        false
    );

    //2nd row is ill-formed
    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":1}}"
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4xxx,\"o\":{\"mg\":2}}\n"
                           //value of baz is invalid
                           + "{\"timestamp\":\"2019-01-01\",\"bar\":null,\"foo\":\"x\",\"baz\":4,\"o\":{\"mg\":3}}\n")
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

    // the invalid character in line 2 stops parsing of the 3-line text in a whole
    // so the total num of iteration is 1
    final int numExpectedIterations = 1;

    try (CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {
        numActualIterations++;

        final InputRowListPlusRawValues rawValues = iterator.next();

        Assertions.assertNotNull(rawValues.getParseException());
      }

      Assertions.assertEquals(numExpectedIterations, numActualIterations);
    }
  }

  @Test
  public void testEmptyJSONText() throws IOException
  {
    assertThrows(ParseException.class, () -> {
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
          false, //make sure JsonReader is used
          false,
          false
      );

      //input is empty
      final ByteEntity source = new ByteEntity(
          StringUtils.toUtf8(
              "" // empty row
          )
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

      // the 2nd line is ill-formed, so the parse of this text chunk aborts
      final int numExpectedIterations = 0;

      try (CloseableIterator<InputRow> iterator = reader.read()) {
        int numActualIterations = 0;
        while (iterator.hasNext()) {
          iterator.next();
          ++numActualIterations;
        }

        Assertions.assertEquals(numExpectedIterations, numActualIterations);
      }
    });
  }



  @Test
  public void testSampleEmptyText() throws IOException
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
        false, //make sure JsonReader is used
        false,
        false
    );

    //input is empty
    final ByteEntity source = new ByteEntity(
        StringUtils.toUtf8("")
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

    // the total num of iteration is 1
    final int numExpectedIterations = 1;

    try (CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample()) {
      int numActualIterations = 0;
      while (iterator.hasNext()) {
        numActualIterations++;

        final InputRowListPlusRawValues rawValues = iterator.next();

        Assertions.assertNotNull(rawValues.getParseException());
      }

      Assertions.assertEquals(numExpectedIterations, numActualIterations);
    }
  }
}
