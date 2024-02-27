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

package org.apache.druid.common.utils;

import org.apache.druid.java.util.common.ISE;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class PropUtilsTest
{
  @Test
  public void testNotSpecifiedGetProperty()
  {
    assertThrows(ISE.class, () -> {
      Properties prop = new Properties();
      PropUtils.getProperty(prop, "");
    });
  }

  @Test
  public void testGetProperty()
  {
    Properties prop = new Properties();
    prop.setProperty("key", "value");
    Assertions.assertEquals("value", PropUtils.getProperty(prop, "key"));
  }

  @Test
  public void testNotSpecifiedGetPropertyAsInt()
  {
    assertThrows(ISE.class, () -> {
      Properties prop = new Properties();
      PropUtils.getPropertyAsInt(prop, "", null);
    });
  }

  @Test
  public void testDefaultValueGetPropertyAsInt()
  {
    Properties prop = new Properties();
    int defaultValue = 1;
    int result = PropUtils.getPropertyAsInt(prop, "", defaultValue);
    Assertions.assertEquals(defaultValue, result);
  }

  @Test
  public void testParseGetPropertyAsInt()
  {
    Properties prop = new Properties();
    int expectedValue = 1;
    prop.setProperty("key", Integer.toString(expectedValue));
    int result = PropUtils.getPropertyAsInt(prop, "key");
    Assertions.assertEquals(expectedValue, result);
  }

  @Test
  public void testFormatExceptionGetPropertyAsInt()
  {
    assertThrows(ISE.class, () -> {
      Properties prop = new Properties();
      prop.setProperty("key", "1-value");
      PropUtils.getPropertyAsInt(prop, "key", null);
    });
  }
}
