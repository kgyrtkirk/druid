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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.mozilla.javascript.EvaluatorException;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 */
public class JavaScriptParserTest
{
  @Test
  public void testParse()
  {
    final String function = "function(str) { var parts = str.split(\"-\"); return { one: parts[0], two: parts[1] } }";

    final Parser<String, Object> parser = new JavaScriptParser(
        function
    );
    String data = "foo-val1";

    final Map<String, Object> parsed = parser.parseToMap(data);
    ImmutableMap.Builder builder = ImmutableMap.builder();
    builder.put("one", "foo");
    builder.put("two", "val1");
    assertEquals(
        builder.build(),
        parsed,
        "result"
    );
  }

  @Test
  public void testParseWithMultiVal()
  {
    final String function = "function(str) { var parts = str.split(\"-\"); return { one: [parts[0], parts[1]] } }";

    final Parser<String, Object> parser = new JavaScriptParser(
        function
    );
    String data = "val1-val2";

    final Map<String, Object> parsed = parser.parseToMap(data);
    ImmutableMap.Builder builder = ImmutableMap.builder();
    builder.put("one", Lists.newArrayList("val1", "val2"));
    assertEquals(
        builder.build(),
        parsed,
        "result"
    );
  }

  @Test
  public void testFailure()
  {
    assertThrows(EvaluatorException.class, () -> {
      final String function = "i am bad javascript";

      new JavaScriptParser(function);
    });
  }
}
