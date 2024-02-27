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

package org.apache.druid.query.extraction;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class FunctionalExtractionTest
{
  private static class SimpleFunctionExtraction extends FunctionalExtraction
  {
    public void initFunctionalExtractionTest(
        Function<String, String> extractionFunction,
        Boolean retainMissingValue,
        String replaceMissingValueWith,
        Boolean uniqueProjections
    )
    {
      super(extractionFunction, retainMissingValue, replaceMissingValueWith, uniqueProjections);
    }

    @Override
    public byte[] getCacheKey()
    {
      return new byte[0];
    }
  }

  private static final Function<String, String> NULL_FN = new Function<String, String>()
  {
    @Nullable
    @Override
    public String apply(String input)
    {
      return null;
    }
  };

  private static final Function<String, String> TURTLE_FN = new Function<String, String>()
  {
    @Nullable
    @Override
    public String apply(@Nullable String input)
    {
      return "turtles";
    }
  };

  private static final Function<String, String> EMPTY_STR_FN = new Function<String, String>()
  {
    @Nullable
    @Override
    public String apply(@Nullable String input)
    {
      return "";
    }
  };

  private static final Function<String, String> IDENTITY_FN = new Function<String, String>()
  {
    @Nullable
    @Override
    public String apply(@Nullable String input)
    {
      return input;
    }
  };

  private static final Function<String, String> ONLY_PRESENT = new Function<String, String>()
  {
    @Nullable
    @Override
    public String apply(@Nullable String input)
    {
      return PRESENT_KEY.equals(input) ? PRESENT_VALUE : null;
    }
  };
  private static String PRESENT_KEY = "present";
  private static String PRESENT_VALUE = "present_value";
  private static String MISSING = "missing";


  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{"null", NULL_FN},
        new Object[]{"turtle", TURTLE_FN},
        new Object[]{"empty", EMPTY_STR_FN},
        new Object[]{"identity", IDENTITY_FN},
        new Object[]{"only_PRESENT", ONLY_PRESENT}
    );
  }

  private Function<String, String> fn;

  public void initFunctionalExtractionTest(String label, Function<String, String> fn)
  {
    this.fn = fn;
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testRetainMissing(String label, Function<String, String> fn)
  {
    initFunctionalExtractionTest(label, fn);
    final String in = "NOT PRESENT";
    final FunctionalExtraction exFn = new SimpleFunctionExtraction(
        fn,
        true,
        null,
        false
    );
    final String out = fn.apply(in);
    Assertions.assertEquals(NullHandling.isNullOrEquivalent(out) ? in : out, exFn.apply(in));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testRetainMissingButFound(String label, Function<String, String> fn)
  {
    initFunctionalExtractionTest(label, fn);
    final String in = PRESENT_KEY;
    final FunctionalExtraction exFn = new SimpleFunctionExtraction(
        fn,
        true,
        null,
        false
    );
    final String out = fn.apply(in);
    Assertions.assertEquals(NullHandling.isNullOrEquivalent(out) ? in : out, exFn.apply(in));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testReplaceMissing(String label, Function<String, String> fn)
  {
    initFunctionalExtractionTest(label, fn);
    final String in = "NOT PRESENT";
    final FunctionalExtraction exFn = new SimpleFunctionExtraction(
        fn,
        false,
        MISSING,
        false
    );
    final String out = fn.apply(in);
    if (NullHandling.replaceWithDefault()) {
      Assertions.assertEquals(NullHandling.isNullOrEquivalent(out) ? MISSING : out, exFn.apply(in));
    } else {
      Assertions.assertEquals(out == null ? MISSING : out, exFn.apply(in));
    }
  }


  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testReplaceMissingBlank(String label, Function<String, String> fn)
  {
    initFunctionalExtractionTest(label, fn);
    final String in = "NOT PRESENT";
    final FunctionalExtraction exFn = new SimpleFunctionExtraction(
        fn,
        false,
        "",
        false
    );
    final String out = fn.apply(in);
    if (NullHandling.replaceWithDefault()) {
      Assertions.assertEquals(Strings.isNullOrEmpty(out) ? null : out, exFn.apply(in));
    } else {
      Assertions.assertEquals(out == null ? "" : out, exFn.apply(in));
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testOnlyOneValuePresent(String label, Function<String, String> fn)
  {
    initFunctionalExtractionTest(label, fn);
    final String in = PRESENT_KEY;
    final FunctionalExtraction exFn = new SimpleFunctionExtraction(
        fn,
        false,
        "",
        false
    );
    final String out = fn.apply(in);
    if (NullHandling.replaceWithDefault()) {
      Assertions.assertEquals(Strings.isNullOrEmpty(out) ? null : out, exFn.apply(in));
    } else {
      Assertions.assertEquals(Strings.isNullOrEmpty(out) ? "" : out, exFn.apply(in));
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testNullInputs(String label, Function<String, String> fn)
  {
    initFunctionalExtractionTest(label, fn);
    final FunctionalExtraction exFn = new SimpleFunctionExtraction(
        fn,
        true,
        null,
        false
    );
    if (NullHandling.isNullOrEquivalent(fn.apply(null))) {
      Assertions.assertEquals(null, exFn.apply(null));
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testBadConfig(String label, Function<String, String> fn)
  {
    initFunctionalExtractionTest(label, fn);
    assertThrows(IllegalArgumentException.class, () -> {
      @SuppressWarnings("unused") // expected exception
      final FunctionalExtraction exFn = new SimpleFunctionExtraction(
          fn,
          true,
          MISSING,
          false
      );
    });
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testUniqueProjections(String label, Function<String, String> fn)
  {
    initFunctionalExtractionTest(label, fn);
    Assertions.assertEquals(
        ExtractionFn.ExtractionType.MANY_TO_ONE,
        new SimpleFunctionExtraction(
            fn,
            true,
            null,
            false
        ).getExtractionType()
    );
    Assertions.assertEquals(
        ExtractionFn.ExtractionType.MANY_TO_ONE,
        new SimpleFunctionExtraction(
            fn,
            true,
            null,
            false
        ).getExtractionType()
    );
    Assertions.assertEquals(
        ExtractionFn.ExtractionType.ONE_TO_ONE,
        new SimpleFunctionExtraction(
            fn,
            true,
            null,
            true
        ).getExtractionType()
    );
  }
}
