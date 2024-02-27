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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.utils.CollectionUtils;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.groups.Default;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HumanReadableBytesTest
{

  @Test
  public void testNumberString()
  {
    Assertions.assertEquals(0, HumanReadableBytes.parse("0"));
    Assertions.assertEquals(1, HumanReadableBytes.parse("1"));
    Assertions.assertEquals(10000000, HumanReadableBytes.parse("10000000"));
  }

  @Test
  public void testWithWhiteSpace()
  {
    Assertions.assertEquals(12345, HumanReadableBytes.parse(" 12345 "));
    Assertions.assertEquals(12345, HumanReadableBytes.parse("\t12345\t"));
  }

  @Test
  public void testK()
  {
    Assertions.assertEquals(1000, HumanReadableBytes.parse("1k"));
    Assertions.assertEquals(1000, HumanReadableBytes.parse("1K"));
  }

  @Test
  public void testM()
  {
    Assertions.assertEquals(1000_000, HumanReadableBytes.parse("1m"));
    Assertions.assertEquals(1000_000, HumanReadableBytes.parse("1M"));
  }

  @Test
  public void testG()
  {
    Assertions.assertEquals(1000_000_000, HumanReadableBytes.parse("1g"));
    Assertions.assertEquals(1000_000_000, HumanReadableBytes.parse("1G"));
  }

  @Test
  public void testT()
  {
    Assertions.assertEquals(1000_000_000_000L, HumanReadableBytes.parse("1t"));
    Assertions.assertEquals(1000_000_000_000L, HumanReadableBytes.parse("1T"));
  }

  @Test
  public void testKiB()
  {
    Assertions.assertEquals(1024, HumanReadableBytes.parse("1kib"));
    Assertions.assertEquals(9 * 1024, HumanReadableBytes.parse("9KiB"));
    Assertions.assertEquals(9 * 1024, HumanReadableBytes.parse("9Kib"));
    Assertions.assertEquals(9 * 1024, HumanReadableBytes.parse("9Ki"));
  }

  @Test
  public void testMiB()
  {
    Assertions.assertEquals(1024 * 1024, HumanReadableBytes.parse("1mib"));
    Assertions.assertEquals(9 * 1024 * 1024, HumanReadableBytes.parse("9MiB"));
    Assertions.assertEquals(9 * 1024 * 1024, HumanReadableBytes.parse("9Mib"));
    Assertions.assertEquals(9 * 1024 * 1024, HumanReadableBytes.parse("9Mi"));
  }

  @Test
  public void testGiB()
  {
    Assertions.assertEquals(1024 * 1024 * 1024, HumanReadableBytes.parse("1gib"));
    Assertions.assertEquals(1024 * 1024 * 1024, HumanReadableBytes.parse("1GiB"));
    Assertions.assertEquals(9L * 1024 * 1024 * 1024, HumanReadableBytes.parse("9Gib"));
    Assertions.assertEquals(9L * 1024 * 1024 * 1024, HumanReadableBytes.parse("9Gi"));
  }

  @Test
  public void testTiB()
  {
    Assertions.assertEquals(1024L * 1024 * 1024 * 1024, HumanReadableBytes.parse("1tib"));
    Assertions.assertEquals(9L * 1024 * 1024 * 1024 * 1024, HumanReadableBytes.parse("9TiB"));
    Assertions.assertEquals(9L * 1024 * 1024 * 1024 * 1024, HumanReadableBytes.parse("9Tib"));
    Assertions.assertEquals(9L * 1024 * 1024 * 1024 * 1024, HumanReadableBytes.parse("9Ti"));
  }

  @Test
  public void testPiB()
  {
    Assertions.assertEquals(1024L * 1024 * 1024 * 1024 * 1024, HumanReadableBytes.parse("1pib"));
    Assertions.assertEquals(9L * 1024 * 1024 * 1024 * 1024 * 1024, HumanReadableBytes.parse("9PiB"));
    Assertions.assertEquals(9L * 1024 * 1024 * 1024 * 1024 * 1024, HumanReadableBytes.parse("9Pib"));
    Assertions.assertEquals(9L * 1024 * 1024 * 1024 * 1024 * 1024, HumanReadableBytes.parse("9Pi"));
  }

  @Test
  public void testDefault()
  {
    Assertions.assertEquals(-123, HumanReadableBytes.parse(" ", -123));
    Assertions.assertEquals(-456, HumanReadableBytes.parse(null, -456));
    Assertions.assertEquals(-789, HumanReadableBytes.parse("\t", -789));
  }

  static class ExceptionMatcher implements Matcher
  {
    static ExceptionMatcher INVALIDFORMAT = new ExceptionMatcher("Invalid format");
    static ExceptionMatcher OVERFLOW = new ExceptionMatcher("Number overflow");

    private String prefix;

    public ExceptionMatcher(String prefix)
    {
      this.prefix = prefix;
    }

    @Override
    public boolean matches(Object item)
    {
      if (!(item instanceof IAE)) {
        return false;
      }

      return ((IAE) item).getMessage().startsWith(prefix);
    }

    @Override
    public void describeMismatch(Object item, Description mismatchDescription)
    {
    }

    @Override
    public void _dont_implement_Matcher___instead_extend_BaseMatcher_()
    {
    }

    @Override
    public void describeTo(Description description)
    {
    }
  }

  @Test
  public void testNull()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    HumanReadableBytes.parse(null);
  }

  @Test
  public void testEmpty()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    HumanReadableBytes.parse("");
  }

  @Test
  public void testWhitespace()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    HumanReadableBytes.parse("   ");
  }

  @Test
  public void testNegative()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    HumanReadableBytes.parse("-1");
  }

  @Test
  public void testInvalidFormatOneChar()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    HumanReadableBytes.parse("b");
  }

  @Test
  public void testInvalidFormatOneCharK8s()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    HumanReadableBytes.parse("i");
  }

  @Test
  public void testInvalidFormatOneChar2()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    HumanReadableBytes.parse("B");
  }

  @Test
  public void testInvalidFormatExtraSpace()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    HumanReadableBytes.parse("1 b");
  }

  @Test
  public void testInvalidFormat4()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    HumanReadableBytes.parse("1b");
  }

  @Test
  public void testInvalidFormatMiBExtraSpace()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    HumanReadableBytes.parse("1 mib");
  }

  @Test
  public void testInvalidFormatMiExtraSpace()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    HumanReadableBytes.parse("1 mi");
  }

  @Test
  public void testInvalidFormatTiB()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    HumanReadableBytes.parse("tib");
  }

  @Test
  public void testInvalidFormatTi()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    HumanReadableBytes.parse("ti");
  }


  @Test
  public void testInvalidFormatGiB()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    HumanReadableBytes.parse("gib");
  }

  @Test
  public void testInvalidFormatPiB()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    HumanReadableBytes.parse(" pib");
  }

  @Test
  public void testInvalidCharacter()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    HumanReadableBytes.parse("c");
  }

  @Test
  public void testExtraLargeNumber()
  {
    expectedException.expect(ExceptionMatcher.INVALIDFORMAT);
    String extraLarge = Long.MAX_VALUE + "1";
    HumanReadableBytes.parse(extraLarge);
  }

  @Test
  public void testOverflowK()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / 1000 + 1) + "k";
    HumanReadableBytes.parse(overflow);
  }

  @Test
  public void testOverflowM()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / 1000_000 + 1) + "m";
    HumanReadableBytes.parse(overflow);
  }

  @Test
  public void testOverflowG()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / 1000_000_000L + 1) + "g";
    HumanReadableBytes.parse(overflow);
  }

  @Test
  public void testOverflowT()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / 1000_000_000_000L + 1) + "t";
    HumanReadableBytes.parse(overflow);
  }

  @Test
  public void testOverflowP()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / 1_000_000_000_000_000L + 1) + "p";
    HumanReadableBytes.parse(overflow);
  }

  @Test
  public void testOverflowKiB()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / 1024 + 1) + "KiB";
    HumanReadableBytes.parse(overflow);
  }

  @Test
  public void testOverflowMiB()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / (1024 * 1024) + 1) + "MiB";
    HumanReadableBytes.parse(overflow);
  }

  @Test
  public void testOverflowGiB()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / (1024L * 1024 * 1024) + 1) + "GiB";
    HumanReadableBytes.parse(overflow);
  }

  @Test
  public void testOverflowTiB()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / (1024L * 1024 * 1024 * 1024) + 1) + "TiB";
    HumanReadableBytes.parse(overflow);
  }

  @Test
  public void testOverflowPiB()
  {
    expectedException.expect(ExceptionMatcher.OVERFLOW);
    String overflow = (Long.MAX_VALUE / (1024L * 1024 * 1024 * 1024 * 1024) + 1) + "PiB";
    HumanReadableBytes.parse(overflow);
  }

  @Test
  public void testJSON() throws JsonProcessingException
  {
    ObjectMapper mapper = new ObjectMapper();
    HumanReadableBytes bytes = new HumanReadableBytes("5m");
    String serialized = mapper.writeValueAsString(bytes);
    HumanReadableBytes deserialized = mapper.readValue(serialized, HumanReadableBytes.class);
    Assertions.assertEquals(bytes, deserialized);
  }

  @Test
  public void testGetInt()
  {
    Throwable exception = assertThrows(Exception.class, () -> {
      HumanReadableBytes bytes = new HumanReadableBytes("2GiB");
      bytes.getBytesInInt();
    });
    assertTrue(exception.getMessage().contains("Number [2147483648] exceeds range of Integer.MAX_VALUE"));
  }

  static class TestBytesRange
  {
    @HumanReadableBytesRange(min = 0, max = 5)
    HumanReadableBytes bytes;

    public TestBytesRange(HumanReadableBytes bytes)
    {
      this.bytes = bytes;
    }
  }

  @Test
  public void testBytesRange()
  {
    String message = validate(new TestBytesRange(HumanReadableBytes.valueOf(-1)));
    Assertions.assertEquals("value must be in the range of [0, 5]", message);

    message = validate(new TestBytesRange(HumanReadableBytes.valueOf(0)));
    Assertions.assertEquals(null, message);

    message = validate(new TestBytesRange(HumanReadableBytes.valueOf(5)));
    Assertions.assertEquals(null, message);

    message = validate(new TestBytesRange(HumanReadableBytes.valueOf(6)));
    Assertions.assertEquals("value must be in the range of [0, 5]", message);
  }

  @Test
  public void testFormatInBinaryByte()
  {
    Assertions.assertEquals("-8.00 EiB", HumanReadableBytes.format(Long.MIN_VALUE, 2, HumanReadableBytes.UnitSystem.BINARY_BYTE));
    Assertions.assertEquals("-8.000 EiB", HumanReadableBytes.format(Long.MIN_VALUE, 3, HumanReadableBytes.UnitSystem.BINARY_BYTE));

    Assertions.assertEquals("-2.00 GiB", HumanReadableBytes.format(Integer.MIN_VALUE, 2, HumanReadableBytes.UnitSystem.BINARY_BYTE));
    Assertions.assertEquals("-32.00 KiB", HumanReadableBytes.format(Short.MIN_VALUE, 2, HumanReadableBytes.UnitSystem.BINARY_BYTE));

    Assertions.assertEquals("-128 B", HumanReadableBytes.format(Byte.MIN_VALUE, 2, HumanReadableBytes.UnitSystem.BINARY_BYTE));
    Assertions.assertEquals("-1 B", HumanReadableBytes.format(-1, 2, HumanReadableBytes.UnitSystem.BINARY_BYTE));
    Assertions.assertEquals("0 B", HumanReadableBytes.format(0, 2, HumanReadableBytes.UnitSystem.BINARY_BYTE));
    Assertions.assertEquals("1 B", HumanReadableBytes.format(1, 2, HumanReadableBytes.UnitSystem.BINARY_BYTE));

    Assertions.assertEquals("1.00 KiB", HumanReadableBytes.format(1024L, 2, HumanReadableBytes.UnitSystem.BINARY_BYTE));
    Assertions.assertEquals("1.00 MiB", HumanReadableBytes.format(1024L * 1024, 2, HumanReadableBytes.UnitSystem.BINARY_BYTE));
    Assertions.assertEquals("1.00 GiB", HumanReadableBytes.format(1024L * 1024 * 1024, 2, HumanReadableBytes.UnitSystem.BINARY_BYTE));
    Assertions.assertEquals("1.00 TiB", HumanReadableBytes.format(1024L * 1024 * 1024 * 1024, 2, HumanReadableBytes.UnitSystem.BINARY_BYTE));
    Assertions.assertEquals("1.00 PiB", HumanReadableBytes.format(1024L * 1024 * 1024 * 1024 * 1024, 2, HumanReadableBytes.UnitSystem.BINARY_BYTE));
    Assertions.assertEquals("8.00 EiB", HumanReadableBytes.format(Long.MAX_VALUE, 2, HumanReadableBytes.UnitSystem.BINARY_BYTE));
  }

  @Test
  public void testPrecisionInBinaryFormat()
  {
    Assertions.assertEquals("1 KiB", HumanReadableBytes.format(1500, 0, HumanReadableBytes.UnitSystem.BINARY_BYTE));
    Assertions.assertEquals("1.5 KiB", HumanReadableBytes.format(1500, 1, HumanReadableBytes.UnitSystem.BINARY_BYTE));
    Assertions.assertEquals("1.46 KiB", HumanReadableBytes.format(1500, 2, HumanReadableBytes.UnitSystem.BINARY_BYTE));
    Assertions.assertEquals("1.465 KiB", HumanReadableBytes.format(1500, 3, HumanReadableBytes.UnitSystem.BINARY_BYTE));
  }

  @Test
  public void testPrecisionInDecimalFormat()
  {
    Assertions.assertEquals("1 KB", HumanReadableBytes.format(1456, 0, HumanReadableBytes.UnitSystem.DECIMAL_BYTE));
    Assertions.assertEquals("1.5 KB", HumanReadableBytes.format(1456, 1, HumanReadableBytes.UnitSystem.DECIMAL_BYTE));
    Assertions.assertEquals("1.46 KB", HumanReadableBytes.format(1456, 2, HumanReadableBytes.UnitSystem.DECIMAL_BYTE));
    Assertions.assertEquals("1.456 KB", HumanReadableBytes.format(1456, 3, HumanReadableBytes.UnitSystem.DECIMAL_BYTE));
  }

  @Test
  public void testFormatInDecimalByte()
  {
    Assertions.assertEquals("1 B", HumanReadableBytes.format(1, 2, HumanReadableBytes.UnitSystem.DECIMAL_BYTE));
    Assertions.assertEquals("1.00 KB", HumanReadableBytes.format(1000L, 2, HumanReadableBytes.UnitSystem.DECIMAL_BYTE));
    Assertions.assertEquals("1.00 MB", HumanReadableBytes.format(1000L * 1000, 2, HumanReadableBytes.UnitSystem.DECIMAL_BYTE));
    Assertions.assertEquals("1.00 GB", HumanReadableBytes.format(1000L * 1000 * 1000, 2, HumanReadableBytes.UnitSystem.DECIMAL_BYTE));
    Assertions.assertEquals("1.00 TB", HumanReadableBytes.format(1000L * 1000 * 1000 * 1000, 2, HumanReadableBytes.UnitSystem.DECIMAL_BYTE));
    Assertions.assertEquals("1.00 PB", HumanReadableBytes.format(1000L * 1000 * 1000 * 1000 * 1000, 2, HumanReadableBytes.UnitSystem.DECIMAL_BYTE));
    Assertions.assertEquals("9.22 EB", HumanReadableBytes.format(Long.MAX_VALUE, 2, HumanReadableBytes.UnitSystem.DECIMAL_BYTE));

    Assertions.assertEquals("100.00 KB", HumanReadableBytes.format(99999, 2, HumanReadableBytes.UnitSystem.DECIMAL_BYTE));
    Assertions.assertEquals("99.999 KB", HumanReadableBytes.format(99999, 3, HumanReadableBytes.UnitSystem.DECIMAL_BYTE));

    Assertions.assertEquals("999.9 PB", HumanReadableBytes.format(999_949_999_999_999_999L, 1, HumanReadableBytes.UnitSystem.DECIMAL_BYTE));
    Assertions.assertEquals("999.95 PB", HumanReadableBytes.format(999_949_999_999_999_999L, 2, HumanReadableBytes.UnitSystem.DECIMAL_BYTE));
    Assertions.assertEquals("999.949 PB", HumanReadableBytes.format(999_949_999_999_999_999L, 3, HumanReadableBytes.UnitSystem.DECIMAL_BYTE));
  }

  @Test
  public void testFormatInDecimal()
  {
    Assertions.assertEquals("1", HumanReadableBytes.format(1, 2, HumanReadableBytes.UnitSystem.DECIMAL));
    Assertions.assertEquals("999", HumanReadableBytes.format(999, 2, HumanReadableBytes.UnitSystem.DECIMAL));
    Assertions.assertEquals("-999", HumanReadableBytes.format(-999, 2, HumanReadableBytes.UnitSystem.DECIMAL));
    Assertions.assertEquals("-1.00 K", HumanReadableBytes.format(-1000, 2, HumanReadableBytes.UnitSystem.DECIMAL));
    Assertions.assertEquals("1.00 K", HumanReadableBytes.format(1000L, 2, HumanReadableBytes.UnitSystem.DECIMAL));
    Assertions.assertEquals("1.00 M", HumanReadableBytes.format(1000L * 1000, 2, HumanReadableBytes.UnitSystem.DECIMAL));
    Assertions.assertEquals("1.00 G", HumanReadableBytes.format(1000L * 1000 * 1000, 2, HumanReadableBytes.UnitSystem.DECIMAL));
    Assertions.assertEquals("1.00 T", HumanReadableBytes.format(1000L * 1000 * 1000 * 1000, 2, HumanReadableBytes.UnitSystem.DECIMAL));
    Assertions.assertEquals("1.00 P", HumanReadableBytes.format(1000L * 1000 * 1000 * 1000 * 1000, 2, HumanReadableBytes.UnitSystem.DECIMAL));
    Assertions.assertEquals("-9.22 E", HumanReadableBytes.format(Long.MIN_VALUE, 2, HumanReadableBytes.UnitSystem.DECIMAL));
    Assertions.assertEquals("9.22 E", HumanReadableBytes.format(Long.MAX_VALUE, 2, HumanReadableBytes.UnitSystem.DECIMAL));
  }

  @Test
  public void testInvalidPrecisionArgumentLowerBound()
  {
    Throwable exception = assertThrows(IAE.class, () -> {
      Assertions.assertEquals("1.00", HumanReadableBytes.format(1, -1, HumanReadableBytes.UnitSystem.DECIMAL));
    });
    assertTrue(exception.getMessage().contains("precision [-1] must be in the range of [0,3]"));
  }

  @Test
  public void testInvalidPrecisionArgumentUpperBound()
  {
    Throwable exception = assertThrows(IAE.class, () -> {
      Assertions.assertEquals("1", HumanReadableBytes.format(1, 3, HumanReadableBytes.UnitSystem.DECIMAL));
      Assertions.assertEquals("1", HumanReadableBytes.format(1, 4, HumanReadableBytes.UnitSystem.DECIMAL));
    });
    assertTrue(exception.getMessage().contains("precision [4] must be in the range of [0,3]"));
  }

  private static <T> String validate(T obj)
  {
    Validator validator = Validation.buildDefaultValidatorFactory()
                                    .getValidator();

    Map<String, StringBuilder> errorMap = new HashMap<>();
    Set<ConstraintViolation<T>> set = validator.validate(obj, Default.class);
    return CollectionUtils.isNullOrEmpty(set) ? null : set.stream().findFirst().get().getMessage();
  }
}
