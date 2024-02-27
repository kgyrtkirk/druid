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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.utils.CompressionUtils;
import org.hamcrest.CoreMatchers;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CsvInputFormatTest extends InitializedNullHandlingTest
{

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final CsvInputFormat format = new CsvInputFormat(Collections.singletonList("a"), "|", null, true, 10);
    final byte[] bytes = mapper.writeValueAsBytes(format);
    final CsvInputFormat fromJson = (CsvInputFormat) mapper.readValue(bytes, InputFormat.class);
    Assertions.assertEquals(format, fromJson);
  }

  @Test
  public void testDeserializeWithoutColumnsWithHasHeaderRow() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final CsvInputFormat inputFormat = (CsvInputFormat) mapper.readValue(
        "{\"type\":\"csv\",\"hasHeaderRow\":true}",
        InputFormat.class
    );
    Assertions.assertTrue(inputFormat.isFindColumnsFromHeader());
  }

  @Test
  public void testDeserializeWithoutColumnsWithFindColumnsFromHeaderTrue() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final CsvInputFormat inputFormat = (CsvInputFormat) mapper.readValue(
        "{\"type\":\"csv\",\"findColumnsFromHeader\":true}",
        InputFormat.class
    );
    Assertions.assertTrue(inputFormat.isFindColumnsFromHeader());
  }

  @Test
  public void testDeserializeWithoutColumnsWithFindColumnsFromHeaderFalse()
  {
    final ObjectMapper mapper = new ObjectMapper();
    final JsonProcessingException e = Assertions.assertThrows(
        JsonProcessingException.class,
        () -> mapper.readValue(
            "{\"type\":\"csv\",\"findColumnsFromHeader\":false}",
            InputFormat.class
        )
    );
    assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
            "Cannot construct instance of `org.apache.druid.data.input.impl.CsvInputFormat`, problem: "
            + "If [columns] is not set, the first row of your data must have your header and "
            + "[findColumnsFromHeader] must be set to true."))
    );
  }

  @Test
  public void testDeserializeWithoutColumnsWithBothHeaderProperties()
  {
    final ObjectMapper mapper = new ObjectMapper();
    final JsonProcessingException e = Assertions.assertThrows(
        JsonProcessingException.class,
        () -> mapper.readValue(
            "{\"type\":\"csv\",\"findColumnsFromHeader\":true,\"hasHeaderRow\":true}",
            InputFormat.class
        )
    );
    assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
            "Cannot construct instance of `org.apache.druid.data.input.impl.CsvInputFormat`, problem: "
            + "Cannot accept both [findColumnsFromHeader] and [hasHeaderRow]"))
    );
  }

  @Test
  public void testDeserializeWithoutAnyProperties()
  {
    final ObjectMapper mapper = new ObjectMapper();
    final JsonProcessingException e = Assertions.assertThrows(
        JsonProcessingException.class,
        () -> mapper.readValue("{\"type\":\"csv\"}", InputFormat.class)
    );
    assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith(
            "Cannot construct instance of `org.apache.druid.data.input.impl.CsvInputFormat`, problem: "
            + "Either [columns] or [findColumnsFromHeader] must be set"))
    );
  }

  @Test
  public void testComma()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      new CsvInputFormat(Collections.singletonList("a,"), "|", null, false, 0);
    });
    assertTrue(exception.getMessage().contains("Column[a,] cannot have the delimiter[,] in its name"));
  }

  @Test
  public void testDelimiter()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      new CsvInputFormat(Collections.singletonList("a\t"), ",", null, false, 0);
    });
    assertTrue(exception.getMessage().contains("Cannot have same delimiter and list delimiter of [,]"));
  }

  @Test
  public void testFindColumnsFromHeaderWithColumnsReturningItsValue()
  {
    final CsvInputFormat format = new CsvInputFormat(Collections.singletonList("a"), null, null, true, 0);
    Assertions.assertTrue(format.isFindColumnsFromHeader());
  }

  @Test
  public void testFindColumnsFromHeaderWithMissingColumnsReturningItsValue()
  {
    final CsvInputFormat format = new CsvInputFormat(null, null, null, true, 0);
    Assertions.assertTrue(format.isFindColumnsFromHeader());
  }

  @Test
  public void testMissingFindColumnsFromHeaderWithMissingColumnsThrowingError()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      new CsvInputFormat(null, null, null, null, 0);
    });
    assertTrue(exception.getMessage().contains("Either [columns] or [findColumnsFromHeader] must be set"));
  }

  @Test
  public void testMissingFindColumnsFromHeaderWithColumnsReturningFalse()
  {
    final CsvInputFormat format = new CsvInputFormat(Collections.singletonList("a"), null, null, null, 0);
    Assertions.assertFalse(format.isFindColumnsFromHeader());
  }

  @Test
  public void testHasHeaderRowWithMissingFindColumnsThrowingError()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      new CsvInputFormat(null, null, true, false, 0);
    });
    assertTrue(exception.getMessage().contains("Cannot accept both [findColumnsFromHeader] and [hasHeaderRow]"));
  }

  @Test
  public void testHasHeaderRowWithMissingColumnsReturningItsValue()
  {
    final CsvInputFormat format = new CsvInputFormat(null, null, true, null, 0);
    Assertions.assertTrue(format.isFindColumnsFromHeader());
  }

  @Test
  public void test_getWeightedSize_withoutCompression()
  {
    final CsvInputFormat format = new CsvInputFormat(null, null, true, null, 0);
    final long unweightedSize = 100L;
    Assertions.assertEquals(unweightedSize, format.getWeightedSize("file.csv", unweightedSize));
  }

  @Test
  public void test_getWeightedSize_withGzCompression()
  {
    final CsvInputFormat format = new CsvInputFormat(null, null, true, null, 0);
    final long unweightedSize = 100L;
    Assertions.assertEquals(
        unweightedSize * CompressionUtils.COMPRESSED_TEXT_WEIGHT_FACTOR,
        format.getWeightedSize("file.csv.gz", unweightedSize)
    );
  }
}
