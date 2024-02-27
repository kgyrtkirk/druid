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

package org.apache.druid.frame.field;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.StringPredicateDruidPredicateFactory;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DoubleFieldReaderTest extends InitializedNullHandlingTest
{
  private static final long MEMORY_POSITION = 1;

  @Mock
  public BaseDoubleColumnValueSelector writeSelector;

  private WritableMemory memory;
  private FieldWriter fieldWriter;

  @BeforeEach
  public void setUp()
  {
    memory = WritableMemory.allocate(1000);
    fieldWriter = DoubleFieldWriter.forPrimitive(writeSelector);
  }

  @AfterEach
  public void tearDown()
  {
    fieldWriter.close();
  }

  @Test
  public void test_isNull_defaultOrNull()
  {
    writeToMemory(NullHandling.defaultDoubleValue());
    Assertions.assertEquals(NullHandling.sqlCompatible(), DoubleFieldReader.forPrimitive().isNull(memory, MEMORY_POSITION));
  }

  @Test
  public void test_isNull_aValue()
  {
    writeToMemory(5.1d);
    Assertions.assertFalse(DoubleFieldReader.forPrimitive().isNull(memory, MEMORY_POSITION));
  }

  @Test
  public void test_makeColumnValueSelector_defaultOrNull()
  {
    writeToMemory(NullHandling.defaultDoubleValue());

    final ColumnValueSelector<?> readSelector =
        DoubleFieldReader.forPrimitive().makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION, -1));

    Assertions.assertEquals(!NullHandling.replaceWithDefault(), readSelector.isNull());

    if (NullHandling.replaceWithDefault()) {
      Assertions.assertEquals(NullHandling.defaultDoubleValue(), readSelector.getDouble(), 0);
    }
  }

  @Test
  public void test_makeColumnValueSelector_aValue()
  {
    writeToMemory(5.1d);

    final ColumnValueSelector<?> readSelector =
        DoubleFieldReader.forPrimitive().makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION, -1));

    Assertions.assertEquals(5.1d, readSelector.getObject());
  }

  @Test
  public void test_makeDimensionSelector_defaultOrNull()
  {
    writeToMemory(NullHandling.defaultDoubleValue());

    final DimensionSelector readSelector =
        DoubleFieldReader.forPrimitive()
                         .makeDimensionSelector(memory, new ConstantFieldPointer(MEMORY_POSITION, -1), null);

    // Data retrieval tests.
    final IndexedInts row = readSelector.getRow();
    Assertions.assertEquals(1, row.size());
    Assertions.assertEquals(NullHandling.replaceWithDefault() ? "0.0" : null, readSelector.lookupName(0));

    // Informational method tests.
    Assertions.assertFalse(readSelector.supportsLookupNameUtf8());
    Assertions.assertFalse(readSelector.nameLookupPossibleInAdvance());
    Assertions.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, readSelector.getValueCardinality());
    Assertions.assertEquals(String.class, readSelector.classOfObject());
    Assertions.assertNull(readSelector.idLookup());

    // Value matcher tests.
    if (NullHandling.replaceWithDefault()) {
      Assertions.assertTrue(readSelector.makeValueMatcher("0.0").matches(false));
      Assertions.assertFalse(readSelector.makeValueMatcher((String) null).matches(false));
      Assertions.assertTrue(readSelector.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("0.0")).matches(false));
      Assertions.assertFalse(readSelector.makeValueMatcher(StringPredicateDruidPredicateFactory.of(DruidObjectPredicate.isNull())).matches(false));
    } else {
      Assertions.assertFalse(readSelector.makeValueMatcher("0.0").matches(false));
      Assertions.assertTrue(readSelector.makeValueMatcher((String) null).matches(false));
      Assertions.assertFalse(readSelector.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("0.0")).matches(false));
      Assertions.assertTrue(readSelector.makeValueMatcher(StringPredicateDruidPredicateFactory.of(DruidObjectPredicate.isNull())).matches(false));
    }
  }

  @Test
  public void test_makeDimensionSelector_aValue()
  {
    writeToMemory(5.1d);

    final DimensionSelector readSelector =
        DoubleFieldReader.forPrimitive()
                         .makeDimensionSelector(memory, new ConstantFieldPointer(MEMORY_POSITION, -1), null);

    // Data retrieval tests.
    final IndexedInts row = readSelector.getRow();
    Assertions.assertEquals(1, row.size());
    Assertions.assertEquals("5.1", readSelector.lookupName(0));

    // Informational method tests.
    Assertions.assertFalse(readSelector.supportsLookupNameUtf8());
    Assertions.assertFalse(readSelector.nameLookupPossibleInAdvance());
    Assertions.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, readSelector.getValueCardinality());
    Assertions.assertEquals(String.class, readSelector.classOfObject());
    Assertions.assertNull(readSelector.idLookup());

    // Value matcher tests.
    Assertions.assertTrue(readSelector.makeValueMatcher("5.1").matches(false));
    Assertions.assertFalse(readSelector.makeValueMatcher("5").matches(false));
    Assertions.assertTrue(readSelector.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("5.1")).matches(false));
    Assertions.assertFalse(readSelector.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("5")).matches(false));
  }

  @Test
  public void test_makeDimensionSelector_aValue_extractionFn()
  {
    writeToMemory(20.5d);

    final DimensionSelector readSelector =
        DoubleFieldReader.forPrimitive().makeDimensionSelector(
            memory,
            new ConstantFieldPointer(MEMORY_POSITION, -1),
            new SubstringDimExtractionFn(1, null)
        );

    // Data retrieval tests.
    final IndexedInts row = readSelector.getRow();
    Assertions.assertEquals(1, row.size());
    Assertions.assertEquals("0.5", readSelector.lookupName(0));

    // Informational method tests.
    Assertions.assertFalse(readSelector.supportsLookupNameUtf8());
    Assertions.assertFalse(readSelector.nameLookupPossibleInAdvance());
    Assertions.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, readSelector.getValueCardinality());
    Assertions.assertEquals(String.class, readSelector.classOfObject());
    Assertions.assertNull(readSelector.idLookup());

    // Value matcher tests.
    Assertions.assertTrue(readSelector.makeValueMatcher("0.5").matches(false));
    Assertions.assertFalse(readSelector.makeValueMatcher("2").matches(false));
    Assertions.assertTrue(readSelector.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("0.5")).matches(false));
    Assertions.assertFalse(readSelector.makeValueMatcher(StringPredicateDruidPredicateFactory.equalTo("2")).matches(false));
  }

  private void writeToMemory(final Double value)
  {
    Mockito.when(writeSelector.isNull()).thenReturn(value == null);

    if (value != null) {
      Mockito.when(writeSelector.getDouble()).thenReturn(value);
    }

    if (fieldWriter.writeTo(memory, MEMORY_POSITION, memory.getCapacity() - MEMORY_POSITION) < 0) {
      throw new ISE("Could not write");
    }
  }
}
