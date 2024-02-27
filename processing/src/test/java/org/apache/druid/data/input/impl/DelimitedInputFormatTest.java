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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.utils.CompressionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DelimitedInputFormatTest
{

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final DelimitedInputFormat format = new DelimitedInputFormat(
        Collections.singletonList("a"),
        "|",
        "delim",
        null,
        true,
        10
    );
    final byte[] bytes = mapper.writeValueAsBytes(format);
    final DelimitedInputFormat fromJson = (DelimitedInputFormat) mapper.readValue(bytes, InputFormat.class);
    Assertions.assertEquals(format, fromJson);
  }

  @Test
  public void testTab()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      new DelimitedInputFormat(Collections.singletonList("a\t"), ",", null, null, false, 0);
    });
    assertTrue(exception.getMessage().contains("Column[a\t] cannot have the delimiter[\t] in its name"));
  }

  @Test
  public void testDelimiterAndListDelimiter()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      new DelimitedInputFormat(Collections.singletonList("a\t"), ",", ",", null, false, 0);
    });
    assertTrue(exception.getMessage().contains("Cannot have same delimiter and list delimiter of [,]"));
  }

  @Test
  public void testCustomizeSeparator()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      new DelimitedInputFormat(Collections.singletonList("a|"), ",", "|", null, false, 0);
    });
    assertTrue(exception.getMessage().contains("Column[a|] cannot have the delimiter[|] in its name"));
  }

  @Test
  public void testFindColumnsFromHeaderWithColumnsReturningItsValue()
  {
    final DelimitedInputFormat format = new DelimitedInputFormat(
        Collections.singletonList("a"),
        null,
        "delim",
        null,
        true,
        0
    );
    Assertions.assertTrue(format.isFindColumnsFromHeader());
  }

  @Test
  public void testFindColumnsFromHeaderWithMissingColumnsReturningItsValue()
  {
    final DelimitedInputFormat format = new DelimitedInputFormat(
        null,
        null,
        "delim",
        null,
        true,
        0
    );
    Assertions.assertTrue(format.isFindColumnsFromHeader());
  }

  @Test
  public void testMissingFindColumnsFromHeaderWithMissingColumnsThrowingError()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      new DelimitedInputFormat(null, null, "delim", null, null, 0);
    });
    assertTrue(exception.getMessage().contains("Either [columns] or [findColumnsFromHeader] must be set"));
  }

  @Test
  public void testMissingFindColumnsFromHeaderWithColumnsReturningFalse()
  {
    final DelimitedInputFormat format = new DelimitedInputFormat(
        Collections.singletonList("a"),
        null,
        "delim",
        null,
        null,
        0
    );
    Assertions.assertFalse(format.isFindColumnsFromHeader());
  }

  @Test
  public void testHasHeaderRowWithMissingFindColumnsThrowingError()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      new DelimitedInputFormat(null, null, "delim", true, false, 0);
    });
    assertTrue(exception.getMessage().contains("Cannot accept both [findColumnsFromHeader] and [hasHeaderRow]"));
  }

  @Test
  public void testHasHeaderRowWithMissingColumnsReturningItsValue()
  {
    final DelimitedInputFormat format = new DelimitedInputFormat(null, null, "delim", true, null, 0);
    Assertions.assertTrue(format.isFindColumnsFromHeader());
  }
  @Test
  public void test_getWeightedSize_withoutCompression()
  {
    final DelimitedInputFormat format = new DelimitedInputFormat(null, null, "delim", true, null, 0);
    final long unweightedSize = 100L;
    Assertions.assertEquals(unweightedSize, format.getWeightedSize("file.tsv", unweightedSize));
  }

  @Test
  public void test_getWeightedSize_withGzCompression()
  {
    final DelimitedInputFormat format = new DelimitedInputFormat(null, null, "delim", true, null, 0);
    final long unweightedSize = 100L;
    Assertions.assertEquals(
        unweightedSize * CompressionUtils.COMPRESSED_TEXT_WEIGHT_FACTOR,
        format.getWeightedSize("file.tsv.gz", unweightedSize)
    );
  }
}
