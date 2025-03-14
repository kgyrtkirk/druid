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

package org.apache.druid.benchmark.indexing;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.generator.DataGenerator;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class IndexMergeBenchmark
{

  @Param({"5"})
  private int numSegments;

  @Param({"75000"})
  private int rowsPerSegment;

  @Param({"basic"})
  private String schema;

  @Param({"true", "false"})
  private boolean rollup;

  @Param({"OFF_HEAP", "TMP_FILE", "ON_HEAP"})
  private SegmentWriteOutType factoryType;


  private static final Logger log = new Logger(IndexMergeBenchmark.class);
  private static final int RNG_SEED = 9999;

  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  private List<QueryableIndex> indexesToMerge;
  private GeneratorSchemaInfo schemaInfo;
  private File tmpDir;
  private IndexMergerV9 indexMergerV9;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(ExprMacroTable.class, ExprMacroTable.nil());
    JSON_MAPPER.setInjectableValues(injectableValues);
    INDEX_IO = new IndexIO(JSON_MAPPER, ColumnConfig.DEFAULT);
  }

  @Setup
  public void setup() throws IOException
  {

    log.info("SETUP CALLED AT " + System.currentTimeMillis());
    indexMergerV9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO, getSegmentWriteOutMediumFactory(factoryType));
    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());

    indexesToMerge = new ArrayList<>();

    schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get(schema);

    for (int i = 0; i < numSegments; i++) {
      DataGenerator gen = new DataGenerator(
          schemaInfo.getColumnSchemas(),
          RNG_SEED + i,
          schemaInfo.getDataInterval(),
          rowsPerSegment
      );

      IncrementalIndex incIndex = makeIncIndex();

      gen.addToIndex(incIndex, rowsPerSegment);

      tmpDir = FileUtils.createTempDir();
      log.info("Using temp dir: " + tmpDir.getAbsolutePath());

      File indexFile = indexMergerV9.persist(
          incIndex,
          tmpDir,
          IndexSpec.DEFAULT,
          null
      );

      QueryableIndex qIndex = INDEX_IO.loadIndex(indexFile);
      indexesToMerge.add(qIndex);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void mergeV9(Blackhole blackhole) throws Exception
  {
    File tmpFile = File.createTempFile("IndexMergeBenchmark-MERGEDFILE-V9-" + System.currentTimeMillis(), ".TEMPFILE");
    tmpFile.delete();
    FileUtils.mkdirp(tmpFile);
    try {
      log.info(tmpFile.getAbsolutePath() + " isFile: " + tmpFile.isFile() + " isDir:" + tmpFile.isDirectory());

      File mergedFile = indexMergerV9.mergeQueryableIndex(
          indexesToMerge,
          rollup,
          schemaInfo.getAggsArray(),
          tmpFile,
          IndexSpec.DEFAULT,
          null,
          -1
      );

      blackhole.consume(mergedFile);
    }
    finally {
      tmpFile.delete();
    }
  }

  @TearDown
  public void tearDown() throws IOException
  {
    FileUtils.deleteDirectory(tmpDir);
  }

  public enum SegmentWriteOutType
  {
    TMP_FILE,
    OFF_HEAP,
    ON_HEAP
  }

  private SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory(SegmentWriteOutType type)
  {
    switch (type) {
      case TMP_FILE:
        return TmpFileSegmentWriteOutMediumFactory.instance();
      case OFF_HEAP:
        return OffHeapMemorySegmentWriteOutMediumFactory.instance();
      case ON_HEAP:
        return OnHeapMemorySegmentWriteOutMediumFactory.instance();
    }
    throw new RuntimeException("Could not create SegmentWriteOutMediumFactory of type: " + type);
  }

  private IncrementalIndex makeIncIndex()
  {
    return new OnheapIncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMetrics(schemaInfo.getAggsArray())
                .withRollup(rollup)
                .build()
        )
        .setMaxRowCount(rowsPerSegment)
        .build();
  }
}
