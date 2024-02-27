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

package org.apache.druid.segment.indexing;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.druid.common.utils.IdUtilsTest;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JSONParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.indexing.granularity.ArbitraryGranularitySpec;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DataSchemaTest extends InitializedNullHandlingTest
{

  private final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

  @Test
  public void testDefaultExclusions()
  {
    Map<String, Object> parser = jsonMapper.convertValue(
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dimB", "dimA"))),
                null,
                null,
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    DataSchema schema = new DataSchema(
        IdUtilsTest.VALID_ID_CHARS,
        parser,
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
            },
        new ArbitraryGranularitySpec(Granularities.DAY, ImmutableList.of(Intervals.of("2014/2015"))),
        null,
        jsonMapper
    );

    Assertions.assertEquals(
        ImmutableSet.of("__time", "time", "col1", "col2", "metric1", "metric2"),
        schema.getDimensionsSpec().getDimensionExclusions()
    );
  }

  @Test
  public void testExplicitInclude()
  {
    Map<String, Object> parser = jsonMapper.convertValue(
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                DimensionsSpec.builder()
                              .setDimensions(
                                  DimensionsSpec.getDefaultSchemas(ImmutableList.of("time", "dimA", "dimB", "col2"))
                              )
                              .setDimensionExclusions(ImmutableList.of("dimC"))
                              .build(),
                null,
                null,
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    DataSchema schema = new DataSchema(
        IdUtilsTest.VALID_ID_CHARS,
        parser,
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
            },
        new ArbitraryGranularitySpec(Granularities.DAY, ImmutableList.of(Intervals.of("2014/2015"))),
        null,
        jsonMapper
    );

    Assertions.assertEquals(
        ImmutableSet.of("__time", "dimC", "col1", "metric1", "metric2"),
        schema.getParser().getParseSpec().getDimensionsSpec().getDimensionExclusions()
    );
  }

  @Test
  public void testTransformSpec()
  {
    Map<String, Object> parserMap = jsonMapper.convertValue(
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                new DimensionsSpec(
                    DimensionsSpec.getDefaultSchemas(ImmutableList.of("time", "dimA", "dimB", "col2"))
                ),
                null,
                null,
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    DataSchema schema = new DataSchema(
        IdUtilsTest.VALID_ID_CHARS,
        parserMap,
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
            },
        new ArbitraryGranularitySpec(Granularities.DAY, ImmutableList.of(Intervals.of("2014/2015"))),
        new TransformSpec(
            new SelectorDimFilter("dimA", "foo", null),
            ImmutableList.of(
                new ExpressionTransform("expr", "concat(dimA,dimA)", TestExprMacroTable.INSTANCE)
            )
        ),
        jsonMapper
    );

    // Test hack that produces a StringInputRowParser.
    final StringInputRowParser parser = (StringInputRowParser) schema.getParser();

    final InputRow row1bb = parser.parseBatch(
        ByteBuffer.wrap("{\"time\":\"2000-01-01\",\"dimA\":\"foo\"}".getBytes(StandardCharsets.UTF_8))
    ).get(0);
    Assertions.assertEquals(DateTimes.of("2000-01-01"), row1bb.getTimestamp());
    Assertions.assertEquals("foo", row1bb.getRaw("dimA"));
    Assertions.assertEquals("foofoo", row1bb.getRaw("expr"));

    final InputRow row1string = parser.parse("{\"time\":\"2000-01-01\",\"dimA\":\"foo\"}");
    Assertions.assertEquals(DateTimes.of("2000-01-01"), row1string.getTimestamp());
    Assertions.assertEquals("foo", row1string.getRaw("dimA"));
    Assertions.assertEquals("foofoo", row1string.getRaw("expr"));

    final InputRow row2 = parser.parseBatch(
        ByteBuffer.wrap("{\"time\":\"2000-01-01\",\"dimA\":\"x\"}".getBytes(StandardCharsets.UTF_8))
    ).get(0);
    Assertions.assertNull(row2);
  }

  @Test
  public void testOverlapMetricNameAndDim()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      Map<String, Object> parser = jsonMapper.convertValue(
          new StringInputRowParser(
              new JSONParseSpec(
                  new TimestampSpec("time", "auto", null),
                  DimensionsSpec.builder()
                      .setDimensions(
                          DimensionsSpec.getDefaultSchemas(
                              ImmutableList.of(
                                  "time",
                                  "dimA",
                                  "dimB",
                                  "metric1"
                              )
                          )
                      )
                      .setDimensionExclusions(ImmutableList.of("dimC"))
                      .build(),
                  null,
                  null,
                  null
              ),
              null
          ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );

      DataSchema schema = new DataSchema(
          IdUtilsTest.VALID_ID_CHARS,
          parser,
          new AggregatorFactory[]{
              new DoubleSumAggregatorFactory("metric1", "col1"),
              new DoubleSumAggregatorFactory("metric2", "col2"),
          },
          new ArbitraryGranularitySpec(Granularities.DAY, ImmutableList.of(Intervals.of("2014/2015"))),
          null,
          jsonMapper
      );

      schema.getParser();
    });
    assertTrue(exception.getMessage().contains("Cannot specify a column more than once: [metric1] seen in dimensions list, metricsSpec list"));
  }

  @Test
  public void testOverlapTimeAndDim()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {

      DataSchema schema = new DataSchema(
          IdUtilsTest.VALID_ID_CHARS,
          new TimestampSpec("time", "auto", null),
          DimensionsSpec.builder()
              .setDimensions(
                  DimensionsSpec.getDefaultSchemas(ImmutableList.of("__time", "dimA", "dimB", "metric1"))
              )
              .setDimensionExclusions(ImmutableList.of("dimC"))
              .build(),
          null,
          new ArbitraryGranularitySpec(Granularities.DAY, ImmutableList.of(Intervals.of("2014/2015"))),
          null,
          null,
          jsonMapper
      );
    });
    assertTrue(exception.getMessage().contains("Cannot specify a column more than once: [__time] seen in dimensions list, "
        + "primary timestamp (__time cannot appear as a dimension or metric)"));
  }

  @Test
  public void testOverlapTimeAndDimLegacy()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      Map<String, Object> parser = jsonMapper.convertValue(
          new StringInputRowParser(
              new JSONParseSpec(
                  new TimestampSpec("time", "auto", null),
                  DimensionsSpec.builder()
                      .setDimensions(
                          DimensionsSpec.getDefaultSchemas(
                              ImmutableList.of(
                                  "__time",
                                  "dimA",
                                  "dimB",
                                  "metric1"
                              )
                          )
                      )
                      .setDimensionExclusions(ImmutableList.of("dimC"))
                      .build(),
                  null,
                  null,
                  null
              ),
              null
          ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );

      DataSchema schema = new DataSchema(
          IdUtilsTest.VALID_ID_CHARS,
          parser,
          null,
          new ArbitraryGranularitySpec(Granularities.DAY, ImmutableList.of(Intervals.of("2014/2015"))),
          null,
          jsonMapper
      );

      schema.getParser();
    });
    assertTrue(exception.getMessage().contains("Cannot specify a column more than once: [__time] seen in dimensions list, primary timestamp "
        + "(__time cannot appear as a dimension or metric)"));
  }

  @Test
  public void testDuplicateAggregators()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      Map<String, Object> parser = jsonMapper.convertValue(
          new StringInputRowParser(
              new JSONParseSpec(
                  new TimestampSpec("time", "auto", null),
                  DimensionsSpec.builder()
                      .setDimensions(DimensionsSpec.getDefaultSchemas(ImmutableList.of("time")))
                      .setDimensionExclusions(ImmutableList.of("dimC"))
                      .build(),
                  null,
                  null,
                  null
              ),
              null
          ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );

      DataSchema schema = new DataSchema(
          IdUtilsTest.VALID_ID_CHARS,
          parser,
          new AggregatorFactory[]{
              new DoubleSumAggregatorFactory("metric1", "col1"),
              new DoubleSumAggregatorFactory("metric2", "col2"),
              new DoubleSumAggregatorFactory("metric1", "col3"),
              new DoubleSumAggregatorFactory("metric3", "col4"),
              new DoubleSumAggregatorFactory("metric3", "col5"),
          },
          new ArbitraryGranularitySpec(Granularities.DAY, ImmutableList.of(Intervals.of("2014/2015"))),
          null,
          jsonMapper
      );
    });
    assertTrue(exception.getMessage().contains("Cannot specify a column more than once: [metric1] seen in metricsSpec list (2 occurrences); "
        + "[metric3] seen in metricsSpec list (2 occurrences)"));
  }

  @Test
  public void testSerdeWithInvalidParserMap() throws Exception
  {
    String jsonStr = "{"
                     + "\"dataSource\":\"" + StringEscapeUtils.escapeJson(IdUtilsTest.VALID_ID_CHARS) + "\","
                     + "\"parser\":{\"type\":\"invalid\"},"
                     + "\"metricsSpec\":[{\"type\":\"doubleSum\",\"name\":\"metric1\",\"fieldName\":\"col1\"}],"
                     + "\"granularitySpec\":{"
                     + "\"type\":\"arbitrary\","
                     + "\"queryGranularity\":{\"type\":\"duration\",\"duration\":86400000,\"origin\":\"1970-01-01T00:00:00.000Z\"},"
                     + "\"intervals\":[\"2014-01-01T00:00:00.000Z/2015-01-01T00:00:00.000Z\"]}}";


    //no error on serde as parser is converted to InputRowParser lazily when really needed
    DataSchema schema = jsonMapper.readValue(
        jsonMapper.writeValueAsString(
            jsonMapper.readValue(jsonStr, DataSchema.class)
        ),
        DataSchema.class
    );

    expectedException.expect(CoreMatchers.instanceOf(IllegalArgumentException.class));
    expectedException.expectCause(CoreMatchers.instanceOf(JsonMappingException.class));
    expectedException.expectMessage(
        "Cannot construct instance of `org.apache.druid.data.input.impl.StringInputRowParser`, problem: parseSpec"
    );

    // Jackson creates a default type parser (StringInputRowParser) for an invalid type.
    schema.getParser();
  }

  @Test
  public void testEmptyDatasource()
  {
    Map<String, Object> parser = jsonMapper.convertValue(
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                DimensionsSpec.builder()
                              .setDimensions(
                                  DimensionsSpec.getDefaultSchemas(ImmutableList.of("time", "dimA", "dimB", "col2"))
                              )
                              .setDimensionExclusions(ImmutableList.of("dimC"))
                              .build(),
                null,
                null,
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    DruidExceptionMatcher
        .invalidInput()
        .expectMessageIs("Invalid value for field [dataSource]: must not be null")
        .assertThrowsAndMatches(
            () -> new DataSchema(
                "",
                parser,
                new AggregatorFactory[]{
                    new DoubleSumAggregatorFactory("metric1", "col1"),
                    new DoubleSumAggregatorFactory("metric2", "col2"),
                    },
                new ArbitraryGranularitySpec(
                    Granularities.DAY,
                    ImmutableList.of(Intervals.of("2014/2015"))
                ),
                null,
                jsonMapper
            ));
  }


  @Test
  public void testInvalidWhitespaceDatasource()
  {
    Map<String, String> invalidCharToDataSourceName = ImmutableMap.of(
        "\\t", "\tab\t",
        "\\r", "\rcarriage\return\r",
        "\\n", "\nnew\nline\n"
    );

    for (Map.Entry<String, String> entry : invalidCharToDataSourceName.entrySet()) {
      String dataSource = entry.getValue();
      final String msg = StringUtils.format(
          "Invalid value for field [dataSource]: Value [%s] contains illegal whitespace characters.  Only space is allowed.",
          dataSource
      );
      DruidExceptionMatcher.invalidInput().expectMessageIs(msg).assertThrowsAndMatches(
          () -> new DataSchema(
              dataSource,
              Collections.emptyMap(),
              null,
              null,
              null,
              jsonMapper
          )
      );
    }
  }

  @Test
  public void testSerde() throws Exception
  {
    String jsonStr = "{"
                     + "\"dataSource\":\"" + StringEscapeUtils.escapeJson(IdUtilsTest.VALID_ID_CHARS) + "\","
                     + "\"parser\":{"
                     + "\"type\":\"string\","
                     + "\"parseSpec\":{"
                     + "\"format\":\"json\","
                     + "\"timestampSpec\":{\"column\":\"xXx\", \"format\": \"auto\", \"missingValue\": null},"
                     + "\"dimensionsSpec\":{\"dimensions\":[], \"dimensionExclusions\":[]},"
                     + "\"flattenSpec\":{\"useFieldDiscovery\":true, \"fields\":[]},"
                     + "\"featureSpec\":{}},"
                     + "\"encoding\":\"UTF-8\""
                     + "},"
                     + "\"metricsSpec\":[{\"type\":\"doubleSum\",\"name\":\"metric1\",\"fieldName\":\"col1\"}],"
                     + "\"granularitySpec\":{"
                     + "\"type\":\"arbitrary\","
                     + "\"queryGranularity\":{\"type\":\"duration\",\"duration\":86400000,\"origin\":\"1970-01-01T00:00:00.000Z\"},"
                     + "\"intervals\":[\"2014-01-01T00:00:00.000Z/2015-01-01T00:00:00.000Z\"]}}";

    DataSchema actual = jsonMapper.readValue(
        jsonMapper.writeValueAsString(
            jsonMapper.readValue(jsonStr, DataSchema.class)
        ),
        DataSchema.class
    );

    Assertions.assertEquals(actual.getDataSource(), IdUtilsTest.VALID_ID_CHARS);
    Assertions.assertEquals(
        actual.getParser().getParseSpec(),
        new JSONParseSpec(
            new TimestampSpec("xXx", null, null),
            DimensionsSpec.builder().setDimensionExclusions(Arrays.asList("__time", "metric1", "xXx", "col1")).build(),
            null,
            null,
            null
        )
    );
    Assertions.assertArrayEquals(
        actual.getAggregators(),
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1")
        }
    );
    Assertions.assertEquals(
        actual.getGranularitySpec(),
        new ArbitraryGranularitySpec(
            new DurationGranularity(86400000, null),
            ImmutableList.of(Intervals.of("2014/2015"))
        )
    );
  }

  @Test
  public void testSerializeWithInvalidDataSourceName() throws Exception
  {
    // Escape backslashes to insert a tab character in the datasource name.
    Map<String, String> datasourceToErrorMsg = ImmutableMap.of(
        "",
        "Invalid value for field [dataSource]: must not be null",

        "../invalid",
        "Invalid value for field [dataSource]: Value [../invalid] cannot start with '.'.",

        "\tname",
        "Invalid value for field [dataSource]: Value [\tname] contains illegal whitespace characters.  Only space is allowed.",

        "name\t invalid",
        "Invalid value for field [dataSource]: Value [name\t invalid] contains illegal whitespace characters.  Only space is allowed."
    );
    for (Map.Entry<String, String> entry : datasourceToErrorMsg.entrySet()) {
      String jsonStr = "{"
                       + "\"dataSource\":\"" + StringEscapeUtils.escapeJson(entry.getKey()) + "\","
                       + "\"parser\":{"
                       + "\"type\":\"string\","
                       + "\"parseSpec\":{"
                       + "\"format\":\"json\","
                       + "\"timestampSpec\":{\"column\":\"xXx\", \"format\": \"auto\", \"missingValue\": null},"
                       + "\"dimensionsSpec\":{\"dimensions\":[], \"dimensionExclusions\":[]},"
                       + "\"flattenSpec\":{\"useFieldDiscovery\":true, \"fields\":[]},"
                       + "\"featureSpec\":{}},"
                       + "\"encoding\":\"UTF-8\""
                       + "},"
                       + "\"metricsSpec\":[{\"type\":\"doubleSum\",\"name\":\"metric1\",\"fieldName\":\"col1\"}],"
                       + "\"granularitySpec\":{"
                       + "\"type\":\"arbitrary\","
                       + "\"queryGranularity\":{\"type\":\"duration\",\"duration\":86400000,\"origin\":\"1970-01-01T00:00:00.000Z\"},"
                       + "\"intervals\":[\"2014-01-01T00:00:00.000Z/2015-01-01T00:00:00.000Z\"]}}";
      try {
        jsonMapper.readValue(
            jsonMapper.writeValueAsString(
                jsonMapper.readValue(jsonStr, DataSchema.class)
            ),
            DataSchema.class
        );
      }
      catch (ValueInstantiationException e) {
        assertThat(
            entry.getKey(),
            e.getCause(),
            DruidExceptionMatcher.invalidInput().expectMessageIs(
                entry.getValue()
            )
        );
        continue;
      }
      Assertions.fail("Serialization of datasource " + entry.getKey() + " should have failed.");
    }
  }

  @Test
  public void testSerdeWithUpdatedDataSchemaAddedField() throws IOException
  {
    Map<String, Object> parser = jsonMapper.convertValue(
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dimB", "dimA"))),
                null,
                null,
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    DataSchema originalSchema = new DataSchema(
        IdUtilsTest.VALID_ID_CHARS,
        parser,
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
            },
        new ArbitraryGranularitySpec(Granularities.DAY, ImmutableList.of(Intervals.of("2014/2015"))),
        null,
        jsonMapper
    );

    String serialized = jsonMapper.writeValueAsString(originalSchema);
    TestModifiedDataSchema deserialized = jsonMapper.readValue(serialized, TestModifiedDataSchema.class);

    Assertions.assertEquals(null, deserialized.getExtra());
    Assertions.assertEquals(originalSchema.getDataSource(), deserialized.getDataSource());
    Assertions.assertEquals(originalSchema.getGranularitySpec(), deserialized.getGranularitySpec());
    Assertions.assertEquals(originalSchema.getParser().getParseSpec(), deserialized.getParser().getParseSpec());
    Assertions.assertArrayEquals(originalSchema.getAggregators(), deserialized.getAggregators());
    Assertions.assertEquals(originalSchema.getTransformSpec(), deserialized.getTransformSpec());
    Assertions.assertEquals(originalSchema.getParserMap(), deserialized.getParserMap());
  }

  @Test
  public void testSerdeWithUpdatedDataSchemaRemovedField() throws IOException
  {
    Map<String, Object> parser = jsonMapper.convertValue(
        new StringInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("time", "auto", null),
                new DimensionsSpec(DimensionsSpec.getDefaultSchemas(ImmutableList.of("dimB", "dimA"))),
                null,
                null,
                null
            ),
            null
        ), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    TestModifiedDataSchema originalSchema = new TestModifiedDataSchema(
        IdUtilsTest.VALID_ID_CHARS,
        null,
        null,
        new AggregatorFactory[]{
            new DoubleSumAggregatorFactory("metric1", "col1"),
            new DoubleSumAggregatorFactory("metric2", "col2"),
            },
        new ArbitraryGranularitySpec(Granularities.DAY, ImmutableList.of(Intervals.of("2014/2015"))),
        null,
        parser,
        jsonMapper,
        "some arbitrary string"
    );

    String serialized = jsonMapper.writeValueAsString(originalSchema);
    DataSchema deserialized = jsonMapper.readValue(serialized, DataSchema.class);

    Assertions.assertEquals(originalSchema.getDataSource(), deserialized.getDataSource());
    Assertions.assertEquals(originalSchema.getGranularitySpec(), deserialized.getGranularitySpec());
    Assertions.assertEquals(originalSchema.getParser().getParseSpec(), deserialized.getParser().getParseSpec());
    Assertions.assertArrayEquals(originalSchema.getAggregators(), deserialized.getAggregators());
    Assertions.assertEquals(originalSchema.getTransformSpec(), deserialized.getTransformSpec());
    Assertions.assertEquals(originalSchema.getParserMap(), deserialized.getParserMap());
  }

  @Test
  public void testWithDimensionSpec()
  {
    TimestampSpec tsSpec = Mockito.mock(TimestampSpec.class);
    GranularitySpec gSpec = Mockito.mock(GranularitySpec.class);
    DimensionsSpec oldDimSpec = Mockito.mock(DimensionsSpec.class);
    DimensionsSpec newDimSpec = Mockito.mock(DimensionsSpec.class);
    AggregatorFactory aggFactory = Mockito.mock(AggregatorFactory.class);
    Mockito.when(aggFactory.getName()).thenReturn("myAgg");
    TransformSpec transSpec = Mockito.mock(TransformSpec.class);
    Map<String, Object> parserMap = Mockito.mock(Map.class);
    Mockito.when(newDimSpec.withDimensionExclusions(ArgumentMatchers.any(Set.class))).thenReturn(newDimSpec);

    DataSchema oldSchema = new DataSchema("dataSource", tsSpec, oldDimSpec,
                                          new AggregatorFactory[]{aggFactory}, gSpec,
                                          transSpec, parserMap, jsonMapper
    );
    DataSchema newSchema = oldSchema.withDimensionsSpec(newDimSpec);
    Assertions.assertSame(oldSchema.getDataSource(), newSchema.getDataSource());
    Assertions.assertSame(oldSchema.getTimestampSpec(), newSchema.getTimestampSpec());
    Assertions.assertSame(newDimSpec, newSchema.getDimensionsSpec());
    Assertions.assertSame(oldSchema.getAggregators(), newSchema.getAggregators());
    Assertions.assertSame(oldSchema.getGranularitySpec(), newSchema.getGranularitySpec());
    Assertions.assertSame(oldSchema.getTransformSpec(), newSchema.getTransformSpec());
    Assertions.assertSame(oldSchema.getParserMap(), newSchema.getParserMap());

  }
}
