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

package org.apache.druid.java.util.common.parsers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.AbstractFlatTextFormatParser.FlatTextFormat;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FlatTextFormatParserTest extends InitializedNullHandlingTest
{
  public static Collection<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{FlatTextFormat.CSV},
        new Object[]{FlatTextFormat.DELIMITED}
    );
  }

  private static final FlatTextFormatParserFactory PARSER_FACTORY = new FlatTextFormatParserFactory();

  private FlatTextFormat format;

  public void initFlatTextFormatParserTest(FlatTextFormat format)
  {
    this.format = format;
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testValidHeader(FlatTextFormat format)
  {
    initFlatTextFormatParserTest(format);
    final String header = concat(format, "time", "value1", "value2");
    final Parser<String, Object> parser = PARSER_FACTORY.get(format, header);
    Assertions.assertEquals(ImmutableList.of("time", "value1", "value2"), parser.getFieldNames());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testDuplicatedColumnName(FlatTextFormat format)
  {
    Throwable exception = assertThrows(ParseException.class, () -> {
      initFlatTextFormatParserTest(format);
      final String header = concat(format, "time", "value1", "value2", "value2");

      PARSER_FACTORY.get(format, header);
    });
    assertTrue(exception.getMessage().contains(StringUtils.format("Unable to parse header [%s]", header)));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testWithHeader(FlatTextFormat format)
  {
    initFlatTextFormatParserTest(format);
    final String header = concat(format, "time", "value1", "value2");
    final Parser<String, Object> parser = PARSER_FACTORY.get(format, header);
    final String body = concat(format, "hello", "world", "foo");
    final Map<String, Object> jsonMap = parser.parseToMap(body);
    Assertions.assertEquals(
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo"),
        jsonMap,
        "jsonMap"
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testWithoutHeader(FlatTextFormat format)
  {
    initFlatTextFormatParserTest(format);
    final Parser<String, Object> parser = PARSER_FACTORY.get(format);
    final String body = concat(format, "hello", "world", "foo");
    final Map<String, Object> jsonMap = parser.parseToMap(body);
    Assertions.assertEquals(
        ImmutableMap.of("column_1", "hello", "column_2", "world", "column_3", "foo"),
        jsonMap,
        "jsonMap"
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testWithSkipHeaderRows(FlatTextFormat format)
  {
    initFlatTextFormatParserTest(format);
    final int skipHeaderRows = 2;
    final Parser<String, Object> parser = PARSER_FACTORY.get(format, false, skipHeaderRows);
    parser.startFileFromBeginning();
    final String[] body = new String[]{
        concat(format, "header", "line", "1"),
        concat(format, "header", "line", "2"),
        concat(format, "hello", "world", "foo")
    };
    int index;
    for (index = 0; index < skipHeaderRows; index++) {
      Assertions.assertNull(parser.parseToMap(body[index]));
    }
    final Map<String, Object> jsonMap = parser.parseToMap(body[index]);
    Assertions.assertEquals(
        ImmutableMap.of("column_1", "hello", "column_2", "world", "column_3", "foo"),
        jsonMap,
        "jsonMap"
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testWithHeaderRow(FlatTextFormat format)
  {
    initFlatTextFormatParserTest(format);
    final Parser<String, Object> parser = PARSER_FACTORY.get(format, true, 0);
    parser.startFileFromBeginning();
    final String[] body = new String[]{
        concat(format, "time", "value1", "value2"),
        concat(format, "hello", "world", "foo")
    };
    Assertions.assertNull(parser.parseToMap(body[0]));
    final Map<String, Object> jsonMap = parser.parseToMap(body[1]);
    Assertions.assertEquals(
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo"),
        jsonMap,
        "jsonMap"
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testWithHeaderRowOfEmptyColumns(FlatTextFormat format)
  {
    initFlatTextFormatParserTest(format);
    final Parser<String, Object> parser = PARSER_FACTORY.get(format, true, 0);
    parser.startFileFromBeginning();
    final String[] body = new String[]{
        concat(format, "time", "", "value2", ""),
        concat(format, "hello", "world", "foo", "bar")
    };
    Assertions.assertNull(parser.parseToMap(body[0]));
    final Map<String, Object> jsonMap = parser.parseToMap(body[1]);
    Assertions.assertEquals(
        ImmutableMap.of("time", "hello", "column_2", "world", "value2", "foo", "column_4", "bar"),
        jsonMap,
        "jsonMap"
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testWithDifferentHeaderRows(FlatTextFormat format)
  {
    initFlatTextFormatParserTest(format);
    final Parser<String, Object> parser = PARSER_FACTORY.get(format, true, 0);
    parser.startFileFromBeginning();
    final String[] body = new String[]{
        concat(format, "time", "value1", "value2"),
        concat(format, "hello", "world", "foo")
    };
    Assertions.assertNull(parser.parseToMap(body[0]));
    Map<String, Object> jsonMap = parser.parseToMap(body[1]);
    Assertions.assertEquals(
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo"),
        jsonMap,
        "jsonMap"
    );

    parser.startFileFromBeginning();
    final String[] body2 = new String[]{
        concat(format, "time", "value1", "value2", "value3"),
        concat(format, "hello", "world", "foo", "bar")
    };
    Assertions.assertNull(parser.parseToMap(body2[0]));
    jsonMap = parser.parseToMap(body2[1]);
    Assertions.assertEquals(
        ImmutableMap.of("time", "hello", "value1", "world", "value2", "foo", "value3", "bar"),
        jsonMap,
        "jsonMap"
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testWithoutStartFileFromBeginning(FlatTextFormat format)
  {
    Throwable exception = assertThrows(UnsupportedOperationException.class, () -> {
      initFlatTextFormatParserTest(format);

      final int skipHeaderRows = 2;
      final Parser<String, Object> parser = PARSER_FACTORY.get(format, false, skipHeaderRows);
      final String[] body = new String[]{
          concat(format, "header", "line", "1"),
          concat(format, "header", "line", "2"),
          concat(format, "hello", "world", "foo")
      };
      parser.parseToMap(body[0]);
    });
    assertTrue(exception.getMessage().contains("hasHeaderRow or maxSkipHeaderRows is not supported. Please check the indexTask supports these options."));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testWithNullValues(FlatTextFormat format)
  {
    initFlatTextFormatParserTest(format);
    final Parser<String, Object> parser = PARSER_FACTORY.get(format, true, 0);
    parser.startFileFromBeginning();
    final String[] body = new String[]{
        concat(format, "time", "value1", "value2"),
        concat(format, "hello", "world", "")
    };
    Assertions.assertNull(parser.parseToMap(body[0]));
    final Map<String, Object> jsonMap = parser.parseToMap(body[1]);
    Assertions.assertNull(jsonMap.get("value2"));
  }

  private static class FlatTextFormatParserFactory
  {
    public Parser<String, Object> get(FlatTextFormat format)
    {
      return get(format, false, 0);
    }

    public Parser<String, Object> get(FlatTextFormat format, boolean hasHeaderRow, int maxSkipHeaderRows)
    {
      switch (format) {
        case CSV:
          return new CSVParser(null, hasHeaderRow, maxSkipHeaderRows);
        case DELIMITED:
          return new DelimitedParser("\t", null, hasHeaderRow, maxSkipHeaderRows);
        default:
          throw new IAE("Unknown format[%s]", format);
      }
    }

    public Parser<String, Object> get(FlatTextFormat format, String header)
    {
      switch (format) {
        case CSV:
          return new CSVParser(null, header);
        case DELIMITED:
          return new DelimitedParser("\t", null, header);
        default:
          throw new IAE("Unknown format[%s]", format);
      }
    }
  }

  private static String concat(FlatTextFormat format, String... values)
  {
    return Arrays.stream(values).collect(Collectors.joining(format.getDefaultDelimiter()));
  }
}
