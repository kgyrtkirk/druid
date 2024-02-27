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

package org.apache.druid.query.dimension;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.MapLookupExtractorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class LookupDimensionSpecTest extends InitializedNullHandlingTest
{
  private static final Map<String, String> STRING_MAP = ImmutableMap.of("key", "value", "key2", "value2");
  private static LookupExtractor MAP_LOOKUP_EXTRACTOR = new MapLookupExtractor(STRING_MAP, true);

  private static final LookupExtractorFactoryContainerProvider LOOKUP_REF_MANAGER =
      EasyMock.createMock(LookupExtractorFactoryContainerProvider.class);

  static {
    EasyMock
        .expect(LOOKUP_REF_MANAGER.get(EasyMock.eq("lookupName")))
        .andReturn(
            Optional.of(new LookupExtractorFactoryContainer("v0", new MapLookupExtractorFactory(STRING_MAP, false)))
        )
        .anyTimes();
    EasyMock.replay(LOOKUP_REF_MANAGER);
  }

  private final DimensionSpec lookupDimSpec =
      new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, false, null, null, true, null);


  @MethodSource("parametersForTestSerDesr")
  @ParameterizedTest
  public void testSerDesr(DimensionSpec lookupDimSpec) throws IOException
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(new NamedType(LookupDimensionSpec.class, "lookup"));
    InjectableValues injectableValues = new InjectableValues.Std().addValue(
        LookupExtractorFactoryContainerProvider.class,
        LOOKUP_REF_MANAGER
    );
    String serLookup = mapper.writeValueAsString(lookupDimSpec);
    Assertions.assertEquals(
        lookupDimSpec,
        mapper.readerFor(DimensionSpec.class).with(injectableValues).readValue(serLookup)
    );
  }

  private static Object[] parametersForTestSerDesr()
  {
    return new Object[]{
        new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, true, null, null, true, null),
        new LookupDimensionSpec(
            "dimName",
            "outputName",
            MAP_LOOKUP_EXTRACTOR,
            false,
            "Missing_value",
            null,
            true,
            null
        ),
        new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, false, null, null, true, null),
        new LookupDimensionSpec("dimName", "outputName", null, false, null, "name", true, LOOKUP_REF_MANAGER)
    };
  }

  @ParameterizedTest
  public void testExceptionWhenNameAndLookupNotNull()
  {
    assertThrows(Exception.class, () -> {
      new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, false, "replace", "name", true, null);
    });
  }

  @ParameterizedTest
  public void testExceptionWhenNameAndLookupNull()
  {
    assertThrows(Exception.class, () -> {
      new LookupDimensionSpec("dimName", "outputName", null, false, "replace", "", true, null);
    });
  }

  @ParameterizedTest
  public void testGetDimension()
  {
    Assertions.assertEquals("dimName", lookupDimSpec.getDimension());
  }

  @ParameterizedTest
  public void testGetOutputName()
  {
    Assertions.assertEquals("outputName", lookupDimSpec.getOutputName());
  }

  public static Object[] parametersForTestApply()
  {
    return new Object[]{
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", null, true, null, "lookupName", true, LOOKUP_REF_MANAGER),
            STRING_MAP
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, true, null, null, true, null),
            STRING_MAP
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, false, null, null, true, null),
            TestHelper.makeMap("not there", null)
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", null, false, null, "lookupName", true, LOOKUP_REF_MANAGER),
            TestHelper.makeMap("not there", null)
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, false, "Missing_value", null,
                true,
                null
            ),
            ImmutableMap.of("not there", "Missing_value")
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", null, false, "Missing_value", "lookupName",
                true,
                LOOKUP_REF_MANAGER
            ),
            ImmutableMap.of("not there", "Missing_value")
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", null, true, null, "lookupName", true, LOOKUP_REF_MANAGER),
            ImmutableMap.of("not there", "not there")
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, true, null, "", true, null),
            ImmutableMap.of("not there", "not there")
        }

    };
  }

  @ParameterizedTest
  @MethodSource("parametersForTestApply")
  public void testApply(DimensionSpec dimensionSpec, Map<String, String> map)
  {
    for (Map.Entry<String, String> entry : map.entrySet()) {
      Assertions.assertEquals(
          NullHandling.emptyToNullIfNeeded(entry.getValue()),
          dimensionSpec.getExtractionFn().apply(entry.getKey())
      );
    }
  }

  public static Object[] parametersForTestGetCacheKey()
  {
    return new Object[]{
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, true, null, null, true, null),
            false
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, false, "Missing_value", null,
                true,
                null
            ),
            false
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName2", MAP_LOOKUP_EXTRACTOR, false, null, null, true, null),
            false
        },
        new Object[]{
            new LookupDimensionSpec("dimName2", "outputName2", MAP_LOOKUP_EXTRACTOR, false, null, null, true, null),
            false
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", MAP_LOOKUP_EXTRACTOR, false, null, null, true, null),
            true
        },
        new Object[]{
            new LookupDimensionSpec("dimName", "outputName", null, false, null, "name", true, LOOKUP_REF_MANAGER),
            false
        }
    };
  }

  @ParameterizedTest
  @MethodSource("parametersForTestGetCacheKey")
  public void testGetCacheKey(DimensionSpec dimensionSpec, boolean expectedResult)
  {
    Assertions.assertEquals(expectedResult, Arrays.equals(lookupDimSpec.getCacheKey(), dimensionSpec.getCacheKey()));
  }

  @ParameterizedTest
  public void testPreservesOrdering()
  {
    Assertions.assertFalse(lookupDimSpec.preservesOrdering());
  }

  @ParameterizedTest
  public void testIsOneToOne()
  {
    Assertions.assertEquals(lookupDimSpec.getExtractionFn().getExtractionType(), ExtractionFn.ExtractionType.ONE_TO_ONE);
  }
}
