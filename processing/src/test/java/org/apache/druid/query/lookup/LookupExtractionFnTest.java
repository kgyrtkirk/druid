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

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class LookupExtractionFnTest
{
  public static Iterable<Object[]> constructorFeeder()
  {
    return Iterables.transform(
        Sets.cartesianProduct(
            ImmutableList.of(
                ImmutableSet.of(true, false),
                ImmutableSet.of("", "MISSING VALUE"),
                ImmutableSet.of(Optional.of(true), Optional.of(false), Optional.empty())
            )
        ),
        List::toArray
    );
  }

  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();
  private boolean retainMissing;
  private String replaceMissing;
  private Boolean injective;

  public void initLookupExtractionFnTest(boolean retainMissing, String replaceMissing, Optional<Boolean> injective)
  {
    this.replaceMissing = NullHandling.emptyToNullIfNeeded(replaceMissing);
    this.retainMissing = retainMissing;
    this.injective = injective.orElse(null);
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest
  public void testEqualsAndHash(boolean retainMissing, String replaceMissing, Optional<Boolean> injective)
  {
    initLookupExtractionFnTest(retainMissing, replaceMissing, injective);
    if (retainMissing && !NullHandling.isNullOrEquivalent(replaceMissing)) {
      // skip
      return;
    }
    final LookupExtractionFn lookupExtractionFn1 = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.of("foo", "bar"), false),
        retainMissing,
        replaceMissing,
        injective,
        false
    );
    final LookupExtractionFn lookupExtractionFn2 = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.of("foo", "bar"), false),
        retainMissing,
        replaceMissing,
        injective,
        false
    );


    final LookupExtractionFn lookupExtractionFn3 = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.of("foo", "bar2"), false),
        retainMissing,
        replaceMissing,
        injective,
        false
    );

    Assertions.assertEquals(lookupExtractionFn1, lookupExtractionFn2);
    Assertions.assertEquals(lookupExtractionFn1.hashCode(), lookupExtractionFn2.hashCode());
    Assertions.assertNotEquals(lookupExtractionFn1, lookupExtractionFn3);
    Assertions.assertNotEquals(lookupExtractionFn1.hashCode(), lookupExtractionFn3.hashCode());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest
  public void testSimpleSerDe(boolean retainMissing, String replaceMissing, Optional<Boolean> injective) throws IOException
  {
    initLookupExtractionFnTest(retainMissing, replaceMissing, injective);
    if (retainMissing && !NullHandling.isNullOrEquivalent(replaceMissing)) {
      // skip
      return;
    }
    final LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.of("foo", "bar"), false),
        retainMissing,
        replaceMissing,
        injective,
        false
    );
    final String str1 = OBJECT_MAPPER.writeValueAsString(lookupExtractionFn);

    final LookupExtractionFn lookupExtractionFn2 = OBJECT_MAPPER.readValue(str1, LookupExtractionFn.class);

    Assertions.assertEquals(retainMissing, lookupExtractionFn2.isRetainMissingValue());
    Assertions.assertEquals(replaceMissing, lookupExtractionFn2.getReplaceMissingValueWith());

    if (injective == null) {
      Assertions.assertEquals(lookupExtractionFn2.getLookup().isOneToOne(), lookupExtractionFn2.isInjective());
    } else {
      Assertions.assertEquals(injective, lookupExtractionFn2.isInjective());
    }

    Assertions.assertArrayEquals(lookupExtractionFn.getCacheKey(), lookupExtractionFn2.getCacheKey());

    Assertions.assertEquals(
        str1,
        OBJECT_MAPPER.writeValueAsString(lookupExtractionFn2)
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest
  public void testIllegalArgs(boolean retainMissing, String replaceMissing, Optional<Boolean> injective)
  {
    initLookupExtractionFnTest(retainMissing, replaceMissing, injective);
    assertThrows(IllegalArgumentException.class, () -> {
      if (retainMissing && !NullHandling.isNullOrEquivalent(replaceMissing)) {
        @SuppressWarnings("unused") // expected exception
        final LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(
            new MapLookupExtractor(ImmutableMap.of("foo", "bar"), false),
            retainMissing,
            NullHandling.emptyToNullIfNeeded(replaceMissing),
            injective,
            false
        );
      } else {
        throw new IAE("Case not valid");
      }
    });
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest
  public void testCacheKey(boolean retainMissing, String replaceMissing, Optional<Boolean> injective)
  {
    initLookupExtractionFnTest(retainMissing, replaceMissing, injective);
    if (retainMissing && !NullHandling.isNullOrEquivalent(replaceMissing)) {
      // skip
      return;
    }
    final Map<String, String> weirdMap = new HashMap<>();
    weirdMap.put("foobar", null);

    final LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(
        new MapLookupExtractor(ImmutableMap.of("foo", "bar"), false),
        retainMissing,
        replaceMissing,
        injective,
        false
    );

    if (NullHandling.isNullOrEquivalent(replaceMissing) || retainMissing) {
      Assertions.assertFalse(
          Arrays.equals(
              lookupExtractionFn.getCacheKey(),
              new LookupExtractionFn(
                  lookupExtractionFn.getLookup(),
                  !lookupExtractionFn.isRetainMissingValue(),
                  lookupExtractionFn.getReplaceMissingValueWith(),
                  lookupExtractionFn.isInjective(),
                  false
              ).getCacheKey()
          )
      );
      Assertions.assertFalse(
          Arrays.equals(
              lookupExtractionFn.getCacheKey(),
              new LookupExtractionFn(
                  lookupExtractionFn.getLookup(),
                  !lookupExtractionFn.isRetainMissingValue(),
                  lookupExtractionFn.getReplaceMissingValueWith(),
                  !lookupExtractionFn.isInjective(),
                  false
              ).getCacheKey()
          )
      );
    }
    Assertions.assertFalse(
        Arrays.equals(
            lookupExtractionFn.getCacheKey(),
            new LookupExtractionFn(
                new MapLookupExtractor(weirdMap, false),
                lookupExtractionFn.isRetainMissingValue(),
                lookupExtractionFn.getReplaceMissingValueWith(),
                lookupExtractionFn.isInjective(),
                false
            ).getCacheKey()
        )
    );
    Assertions.assertFalse(
        Arrays.equals(
            lookupExtractionFn.getCacheKey(),
            new LookupExtractionFn(
                lookupExtractionFn.getLookup(),
                lookupExtractionFn.isRetainMissingValue(),
                lookupExtractionFn.getReplaceMissingValueWith(),
                !lookupExtractionFn.isInjective(),
                false
            ).getCacheKey()
        )
    );
  }
}
