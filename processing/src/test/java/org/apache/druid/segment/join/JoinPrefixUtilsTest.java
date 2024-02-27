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

package org.apache.druid.segment.join;

import org.apache.druid.segment.column.ColumnHolder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JoinPrefixUtilsTest
{

  @Test
  public void test_validatePrefix_null()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {

      JoinPrefixUtils.validatePrefix(null);
    });
    assertTrue(exception.getMessage().contains("Join clause cannot have null or empty prefix"));
  }

  @Test
  public void test_validatePrefix_empty()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {

      JoinPrefixUtils.validatePrefix("");
    });
    assertTrue(exception.getMessage().contains("Join clause cannot have null or empty prefix"));
  }

  @Test
  public void test_validatePrefix_underscore()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {

      JoinPrefixUtils.validatePrefix("_");
    });
    assertTrue(exception.getMessage().contains("Join clause cannot have prefix[_]"));
  }

  @Test
  public void test_validatePrefix_timeColumn()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {

      JoinPrefixUtils.validatePrefix(ColumnHolder.TIME_COLUMN_NAME);
    });
    assertTrue(exception.getMessage().contains("Join clause cannot have prefix[__time]"));
  }

  @Test
  public void test_isPrefixedBy()
  {
    Assertions.assertTrue(JoinPrefixUtils.isPrefixedBy("foo", ""));
    Assertions.assertTrue(JoinPrefixUtils.isPrefixedBy("foo", "f"));
    Assertions.assertTrue(JoinPrefixUtils.isPrefixedBy("foo", "fo"));
    Assertions.assertFalse(JoinPrefixUtils.isPrefixedBy("foo", "foo"));
  }
}
