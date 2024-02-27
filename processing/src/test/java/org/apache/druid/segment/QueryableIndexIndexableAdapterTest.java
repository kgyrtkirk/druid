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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import org.apache.druid.segment.data.BitmapValues;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.ConciseBitmapSerdeFactory;
import org.apache.druid.segment.data.IncrementalIndexTest;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

public class QueryableIndexIndexableAdapterTest
{
  private static final IndexSpec INDEX_SPEC = IndexSpec.builder()
                                                       .withBitmapSerdeFactory(new ConciseBitmapSerdeFactory())
                                                       .withDimensionCompression(CompressionStrategy.LZ4)
                                                       .withMetricCompression(CompressionStrategy.LZ4)
                                                       .withLongEncoding(CompressionFactory.LongEncodingStrategy.LONGS)
                                                       .build();

  public static Collection<?> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[] {TmpFileSegmentWriteOutMediumFactory.instance()},
        new Object[] {OffHeapMemorySegmentWriteOutMediumFactory.instance()}
    );
  }

  @TempDir
  public File temporaryFolder;
  @Rule
  public final CloserRule closer = new CloserRule(false);

  private IndexMerger indexMerger;
  private IndexIO indexIO;

  public void initQueryableIndexIndexableAdapterTest(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory)
  {
    indexMerger = TestHelper.getTestIndexMergerV9(segmentWriteOutMediumFactory);
    indexIO = TestHelper.getTestIndexIO();
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest
  public void testGetBitmapIndex(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory) throws Exception
  {
    initQueryableIndexIndexableAdapterTest(segmentWriteOutMediumFactory);
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex toPersist = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist);

    final File tempDir = newFolder(temporaryFolder, "junit");
    QueryableIndex index = closer.closeLater(
        indexIO.loadIndex(
            indexMerger.persist(
                toPersist,
                tempDir,
                INDEX_SPEC,
                null
            )
        )
    );

    IndexableAdapter adapter = new QueryableIndexIndexableAdapter(index);
    String dimension = "dim1";
    @SuppressWarnings("UnusedAssignment") //null is added to all dimensions with value
    BitmapValues bitmapValues = adapter.getBitmapValues(dimension, 0);
    try (CloseableIndexed<String> dimValueLookup = adapter.getDimValueLookup(dimension)) {
      for (int i = 0; i < dimValueLookup.size(); i++) {
        bitmapValues = adapter.getBitmapValues(dimension, i);
        Assertions.assertEquals(1, bitmapValues.size());
      }
    }
  }

  private static File newFolder(File root, String... subDirs) throws IOException {
    String subFolder = String.join("/", subDirs);
    File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }
}
