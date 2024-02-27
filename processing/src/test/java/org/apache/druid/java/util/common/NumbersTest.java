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

package org.apache.druid.java.util.common;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NumbersTest
{

  @Test
  public void testParseLong()
  {
    final String strVal = "100";
    Assertions.assertEquals(100L, Numbers.parseLong(strVal));

    final Long longVal = 100L;
    Assertions.assertEquals(100L, Numbers.parseLong(longVal));

    final Double doubleVal = 100.;
    Assertions.assertEquals(100L, Numbers.parseLong(doubleVal));
  }

  @Test
  public void testParseLongWithNull()
  {
    Throwable exception = assertThrows(NullPointerException.class, () -> {
      Numbers.parseLong(null);
    });
    assertTrue(exception.getMessage().contains("Input is null"));
  }

  @Test
  public void testParseLongWithUnparseableString()
  {
    assertThrows(NumberFormatException.class, () -> {
      Numbers.parseLong("unparseable");
    });
  }

  @Test
  public void testParseLongWithUnparseableObject()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(CoreMatchers.startsWith("Unknown type"));
    Numbers.parseLong(new Object());
  }

  @Test
  public void testParseInt()
  {
    final String strVal = "100";
    Assertions.assertEquals(100, Numbers.parseInt(strVal));

    final Integer longVal = 100;
    Assertions.assertEquals(100, Numbers.parseInt(longVal));

    final Float floatVal = 100.F;
    Assertions.assertEquals(100, Numbers.parseInt(floatVal));
  }

  @Test
  public void testParseIntWithNull()
  {
    Throwable exception = assertThrows(NullPointerException.class, () -> {
      Numbers.parseInt(null);
    });
    assertTrue(exception.getMessage().contains("Input is null"));
  }

  @Test
  public void testParseIntWithUnparseableString()
  {
    assertThrows(NumberFormatException.class, () -> {
      Numbers.parseInt("unparseable");
    });
  }

  @Test
  public void testParseIntWithUnparseableObject()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(CoreMatchers.startsWith("Unknown type"));
    Numbers.parseInt(new Object());
  }

  @Test
  public void testParseBoolean()
  {
    final String strVal = "false";
    Assertions.assertEquals(false, Numbers.parseBoolean(strVal));

    final Boolean booleanVal = Boolean.FALSE;
    Assertions.assertEquals(false, Numbers.parseBoolean(booleanVal));
  }

  @Test
  public void testParseBooleanWithNull()
  {
    Throwable exception = assertThrows(NullPointerException.class, () -> {
      Numbers.parseBoolean(null);
    });
    assertTrue(exception.getMessage().contains("Input is null"));
  }

  @Test
  public void testParseBooleanWithUnparseableObject()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage(CoreMatchers.startsWith("Unknown type"));
    Numbers.parseBoolean(new Object());
  }

  @Test
  public void testParseLongObject()
  {
    Assertions.assertEquals(null, Numbers.parseLongObject(null));
    Assertions.assertEquals((Long) 1L, Numbers.parseLongObject("1"));
    Assertions.assertEquals((Long) 32L, Numbers.parseLongObject("32.1243"));
  }

  @Test
  public void testParseLongObjectUnparseable()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      Assertions.assertEquals((Long) 1337L, Numbers.parseLongObject("'1'"));
    });
    assertTrue(exception.getMessage().contains("Cannot parse string to long"));
  }

  @Test
  public void testParseDoubleObject()
  {
    Assertions.assertEquals(null, Numbers.parseLongObject(null));
    Assertions.assertEquals((Double) 1.0, Numbers.parseDoubleObject("1"));
    Assertions.assertEquals((Double) 32.1243, Numbers.parseDoubleObject("32.1243"));
  }

  @Test
  public void testParseDoubleObjectUnparseable()
  {
    Throwable exception = assertThrows(IllegalArgumentException.class, () -> {
      Assertions.assertEquals((Double) 300.0, Numbers.parseDoubleObject("'1.1'"));
    });
    assertTrue(exception.getMessage().contains("Cannot parse string to double"));
  }
}
