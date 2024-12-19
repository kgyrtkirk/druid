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

package org.apache.druid.sql.calcite.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.FloatDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class MapBasedTestDataset implements TestDataSet
{
  protected final String name;

  protected MapBasedTestDataset(String name)
  {
    this.name = name;
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public final QueryableIndex makeIndex(File tmpDir)
  {
    return IndexBuilder
        .create()
        .tmpDir(tmpDir)
        .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
        .schema(getIndexSchema())
        .rows(getRows())
        .buildMMappedIndex();
  }

  protected final Iterable<InputRow> getRows()
  {
    return getRawRows()
        .stream()
        .map(raw -> TestDataBuilder.createRow(raw, getInputRowSchema()))
        .collect(Collectors.toList());
  }

  protected abstract InputRowSchema getInputRowSchema();

  protected abstract IncrementalIndexSchema getIndexSchema();

  protected abstract List<Map<String, Object>> getRawRows();


  static class NumFoo extends MapBasedTestDataset
  {
    protected NumFoo()
    {
      super("numfoo");
    }

    protected final InputRowSchema getInputRowSchema()
    {
      return new InputRowSchema(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", null),
          new DimensionsSpec(
              ImmutableList.<DimensionSchema>builder()
                  .addAll(
                      DimensionsSpec.getDefaultSchemas(
                          ImmutableList.of(
                              "dim1",
                              "dim2",
                              "dim3",
                              "dim4",
                              "dim5",
                              "dim6"
                          )
                      )
                  )
                  .add(new DoubleDimensionSchema("dbl1"))
                  .add(new DoubleDimensionSchema("dbl2"))
                  .add(new FloatDimensionSchema("f1"))
                  .add(new FloatDimensionSchema("f2"))
                  .add(new LongDimensionSchema("l1"))
                  .add(new LongDimensionSchema("l2"))
                  .build()
          ),
          null
      );
    }

    protected IncrementalIndexSchema getIndexSchema()
    {
      return new IncrementalIndexSchema.Builder()
          .withMetrics(
              new CountAggregatorFactory("cnt"),
              new FloatSumAggregatorFactory("m1", "m1"),
              new DoubleSumAggregatorFactory("m2", "m2"),
              new HyperUniquesAggregatorFactory("unique_dim1", "dim1")
          )
          .withDimensionsSpec(getInputRowSchema().getDimensionsSpec())
          .withRollup(false)
          .build();
    }

    protected List<Map<String, Object>> getRawRows()
    {
      return ImmutableList.of(
          ImmutableMap.<String, Object>builder()
              .put("t", "2000-01-01")
              .put("m1", "1.0")
              .put("m2", "1.0")
              .put("dbl1", 1.0)
              .put("f1", 1.0f)
              .put("l1", 7L)
              .put("dim1", "")
              .put("dim2", ImmutableList.of("a"))
              .put("dim3", ImmutableList.of("a", "b"))
              .put("dim4", "a")
              .put("dim5", "aa")
              .put("dim6", "1")
              .build(),
          ImmutableMap.<String, Object>builder()
              .put("t", "2000-01-02")
              .put("m1", "2.0")
              .put("m2", "2.0")
              .put("dbl1", 1.7)
              .put("dbl2", 1.7)
              .put("f1", 0.1f)
              .put("f2", 0.1f)
              .put("l1", 325323L)
              .put("l2", 325323L)
              .put("dim1", "10.1")
              .put("dim2", ImmutableList.of())
              .put("dim3", ImmutableList.of("b", "c"))
              .put("dim4", "a")
              .put("dim5", "ab")
              .put("dim6", "2")
              .build(),
          ImmutableMap.<String, Object>builder()
              .put("t", "2000-01-03")
              .put("m1", "3.0")
              .put("m2", "3.0")
              .put("dbl1", 0.0)
              .put("dbl2", 0.0)
              .put("f1", 0.0)
              .put("f2", 0.0)
              .put("l1", 0)
              .put("l2", 0)
              .put("dim1", "2")
              .put("dim2", ImmutableList.of(""))
              .put("dim3", ImmutableList.of("d"))
              .put("dim4", "a")
              .put("dim5", "ba")
              .put("dim6", "3")
              .build(),
          ImmutableMap.<String, Object>builder()
              .put("t", "2001-01-01")
              .put("m1", "4.0")
              .put("m2", "4.0")
              .put("dim1", "1")
              .put("dim2", ImmutableList.of("a"))
              .put("dim3", ImmutableList.of(""))
              .put("dim4", "b")
              .put("dim5", "ad")
              .put("dim6", "4")
              .build(),
          ImmutableMap.<String, Object>builder()
              .put("t", "2001-01-02")
              .put("m1", "5.0")
              .put("m2", "5.0")
              .put("dim1", "def")
              .put("dim2", ImmutableList.of("abc"))
              .put("dim3", ImmutableList.of())
              .put("dim4", "b")
              .put("dim5", "aa")
              .put("dim6", "5")
              .build(),
          ImmutableMap.<String, Object>builder()
              .put("t", "2001-01-03")
              .put("m1", "6.0")
              .put("m2", "6.0")
              .put("dim1", "abc")
              .put("dim4", "b")
              .put("dim5", "ab")
              .put("dim6", "6")
              .build()
      );
    }
  }

}