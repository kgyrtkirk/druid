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

package org.apache.druid.query.groupby.having;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.apache.druid.query.groupby.ResultRow;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class DimensionSelectorHavingSpecTest
{

  private ResultRow getTestRow(Object dimensionValue)
  {
    return ResultRow.of(dimensionValue);
  }

  @Test
  public void testDimSelectorHavingClauseSerde()
  {
    HavingSpec dimHavingSpec = new DimensionSelectorHavingSpec("dim", "v", null);

    Map<String, Object> dimSelectMap = ImmutableMap.of(
        "type", "dimSelector",
        "dimension", "dim",
        "value", "v"
    );

    ObjectMapper mapper = new DefaultObjectMapper();
    Assertions.assertEquals(dimHavingSpec, mapper.convertValue(dimSelectMap, DimensionSelectorHavingSpec.class));
  }

  @Test
  public void testEquals()
  {
    ExtractionFn extractionFn1 = new RegexDimExtractionFn("^([^,]*),", false, "");
    ExtractionFn extractionFn2 = new RegexDimExtractionFn(",(.*)", false, "");
    ExtractionFn extractionFn3 = new RegexDimExtractionFn("^([^,]*),", false, "");

    HavingSpec dimHavingSpec1 = new DimensionSelectorHavingSpec("dim", "v", extractionFn1);
    HavingSpec dimHavingSpec2 = new DimensionSelectorHavingSpec("dim", "v", extractionFn3);
    HavingSpec dimHavingSpec3 = new DimensionSelectorHavingSpec("dim1", "v", null);
    HavingSpec dimHavingSpec4 = new DimensionSelectorHavingSpec("dim2", "v", null);
    HavingSpec dimHavingSpec5 = new DimensionSelectorHavingSpec("dim", "v1", null);
    HavingSpec dimHavingSpec6 = new DimensionSelectorHavingSpec("dim", "v2", null);
    HavingSpec dimHavingSpec7 = new DimensionSelectorHavingSpec("dim", null, null);
    HavingSpec dimHavingSpec8 = new DimensionSelectorHavingSpec("dim", null, null);
    HavingSpec dimHavingSpec9 = new DimensionSelectorHavingSpec("dim1", null, null);
    HavingSpec dimHavingSpec10 = new DimensionSelectorHavingSpec("dim2", null, null);
    HavingSpec dimHavingSpec11 = new DimensionSelectorHavingSpec("dim1", "v", null);
    HavingSpec dimHavingSpec12 = new DimensionSelectorHavingSpec("dim2", null, null);
    HavingSpec dimHavingSpec13 = new DimensionSelectorHavingSpec("dim", "value", extractionFn1);
    HavingSpec dimHavingSpec14 = new DimensionSelectorHavingSpec("dim", "value", extractionFn2);

    Assertions.assertEquals(dimHavingSpec1, dimHavingSpec2);
    Assertions.assertNotEquals(dimHavingSpec3, dimHavingSpec4);
    Assertions.assertNotEquals(dimHavingSpec5, dimHavingSpec6);
    Assertions.assertEquals(dimHavingSpec7, dimHavingSpec8);
    Assertions.assertNotEquals(dimHavingSpec9, dimHavingSpec10);
    Assertions.assertNotEquals(dimHavingSpec11, dimHavingSpec12);
    Assertions.assertNotEquals(dimHavingSpec13, dimHavingSpec14);
  }

  @Test
  public void testToString()
  {
    ExtractionFn extractionFn = new RegexDimExtractionFn("^([^,]*),", false, "");
    String expected = "DimensionSelectorHavingSpec{" +
                      "dimension='gender'," +
                      " value='m'," +
                      " extractionFn=regex(/^([^,]*),/, 1)}";
    Assertions.assertEquals(expected, new DimensionSelectorHavingSpec("gender", "m", extractionFn).toString());

    expected = "DimensionSelectorHavingSpec{" +
               "dimension='gender'," +
               " value='m'," +
               " extractionFn=Identity}";

    Assertions.assertEquals(expected, new DimensionSelectorHavingSpec("gender", "m", null).toString());
  }

  @Test
  public void testNullDimension()
  {
    assertThrows(NullPointerException.class, () -> {
      //noinspection ResultOfObjectAllocationIgnored (result is not needed)
      new DimensionSelectorHavingSpec(null, "value", null);
    });
  }

  @Test
  public void testDimensionFilterSpec()
  {
    DimensionSelectorHavingSpec spec = new DimensionSelectorHavingSpec("dimension", "v", null);
    Assertions.assertTrue(spec.eval(getTestRow("v")));
    Assertions.assertTrue(spec.eval(getTestRow(ImmutableList.of("v", "v1"))));
    Assertions.assertFalse(spec.eval(getTestRow(ImmutableList.of())));
    Assertions.assertFalse(spec.eval(getTestRow("v1")));

    spec = new DimensionSelectorHavingSpec("dimension", null, null);
    Assertions.assertTrue(spec.eval(getTestRow(ImmutableList.of())));
    Assertions.assertTrue(spec.eval(getTestRow(ImmutableList.of(""))));
    Assertions.assertFalse(spec.eval(getTestRow(ImmutableList.of("v"))));
    Assertions.assertFalse(spec.eval(getTestRow(ImmutableList.of("v", "v1"))));

    spec = new DimensionSelectorHavingSpec("dimension", "", null);
    Assertions.assertTrue(spec.eval(getTestRow(ImmutableList.of())));
    Assertions.assertTrue(spec.eval(getTestRow(ImmutableList.of(""))));
    Assertions.assertTrue(spec.eval(getTestRow(ImmutableList.of("v", "v1", ""))));
    Assertions.assertFalse(spec.eval(getTestRow(ImmutableList.of("v"))));
    Assertions.assertFalse(spec.eval(getTestRow(ImmutableList.of("v", "v1"))));

    ExtractionFn extractionFn = new RegexDimExtractionFn("^([^,]*),", true, "default");
    spec = new DimensionSelectorHavingSpec("dimension", "v", extractionFn);
    Assertions.assertTrue(spec.eval(getTestRow(ImmutableList.of("v,v1", "v2,v3"))));
    Assertions.assertFalse(spec.eval(getTestRow(ImmutableList.of("v1,v4"))));
    Assertions.assertFalse(spec.eval(getTestRow(ImmutableList.of("v"))));
    Assertions.assertFalse(spec.eval(getTestRow(ImmutableList.of("v1", "default"))));
    Assertions.assertTrue(spec.eval(getTestRow(ImmutableList.of("v,default", "none"))));

    spec = new DimensionSelectorHavingSpec("dimension", "default", extractionFn);
    Assertions.assertTrue(spec.eval(getTestRow(ImmutableList.of("v1,v2", "none"))));
  }
}
