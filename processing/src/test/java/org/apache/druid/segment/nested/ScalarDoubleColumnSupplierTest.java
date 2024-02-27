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

package org.apache.druid.segment.nested;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.NestedDataModule;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.filter.SelectorPredicateFactory;
import org.apache.druid.segment.AutoTypeColumnIndexer;
import org.apache.druid.segment.AutoTypeColumnMerger;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.IndexableAdapter;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.index.semantic.DruidPredicateIndexes;
import org.apache.druid.segment.index.semantic.NullValueIndex;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
import org.apache.druid.segment.index.semantic.ValueIndexes;
import org.apache.druid.segment.vector.NoFilterVectorOffset;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class ScalarDoubleColumnSupplierTest extends InitializedNullHandlingTest
{
  private static final String NO_MATCH = "no";

  @TempDir
  public File tempFolder;

  BitmapSerdeFactory bitmapSerdeFactory = RoaringBitmapSerdeFactory.getInstance();
  DefaultBitmapResultFactory resultFactory = new DefaultBitmapResultFactory(bitmapSerdeFactory.getBitmapFactory());

  List<Double> data = Arrays.asList(
      1.0,
      0.0,
      null,
      2.0,
      3.3,
      9.9
  );

  Closer closer = Closer.create();

  SmooshedFileMapper fileMapper;

  ByteBuffer baseBuffer;

  @BeforeAll
  public static void staticSetup()
  {
    NestedDataModule.registerHandlersAndSerde();
  }

  @BeforeEach
  public void setup() throws IOException
  {
    final String fileNameBase = "test";
    fileMapper = smooshify(fileNameBase, newFolder(tempFolder, "junit"), data);
    baseBuffer = fileMapper.mapFile(fileNameBase);
  }

  private SmooshedFileMapper smooshify(
      String fileNameBase,
      File tmpFile,
      List<?> data
  )
      throws IOException
  {
    SegmentWriteOutMediumFactory writeOutMediumFactory = TmpFileSegmentWriteOutMediumFactory.instance();
    try (final FileSmoosher smoosher = new FileSmoosher(tmpFile)) {
      ScalarDoubleColumnSerializer serializer = new ScalarDoubleColumnSerializer(
          fileNameBase,
          IndexSpec.DEFAULT,
          writeOutMediumFactory.makeSegmentWriteOutMedium(newFolder(tempFolder, "junit")),
          closer
      );

      AutoTypeColumnIndexer indexer = new AutoTypeColumnIndexer("test", null);
      for (Object o : data) {
        indexer.processRowValsToUnsortedEncodedKeyComponent(o, false);
      }
      SortedMap<String, FieldTypeInfo.MutableTypeSet> sortedFields = new TreeMap<>();

      IndexableAdapter.NestedColumnMergable mergable = closer.register(
          new IndexableAdapter.NestedColumnMergable(
              indexer.getSortedValueLookups(),
              indexer.getFieldTypeInfo(),
              false,
              false,
              null
          )
      );
      SortedValueDictionary globalDictionarySortedCollector = mergable.getValueDictionary();
      mergable.mergeFieldsInto(sortedFields);

      serializer.openDictionaryWriter();
      serializer.serializeDictionaries(
          globalDictionarySortedCollector.getSortedStrings(),
          globalDictionarySortedCollector.getSortedLongs(),
          globalDictionarySortedCollector.getSortedDoubles(),
          () -> new AutoTypeColumnMerger.ArrayDictionaryMergingIterator(
              new Iterable[]{globalDictionarySortedCollector.getSortedArrays()},
              serializer.getGlobalLookup()
          )
      );
      serializer.open();

      NestedDataColumnSupplierTest.SettableSelector valueSelector = new NestedDataColumnSupplierTest.SettableSelector();
      for (Object o : data) {
        valueSelector.setObject(StructuredData.wrap(o));
        serializer.serialize(valueSelector);
      }

      try (SmooshedWriter writer = smoosher.addWithSmooshedWriter(fileNameBase, serializer.getSerializedSize())) {
        serializer.writeTo(writer, smoosher);
      }
      smoosher.close();
      return closer.register(SmooshedFileMapper.load(tmpFile));
    }
  }

  @AfterEach
  public void teardown() throws IOException
  {
    closer.close();
  }

  @Test
  public void testBasicFunctionality()
  {
    ColumnBuilder bob = new ColumnBuilder();
    bob.setFileMapper(fileMapper);
    ScalarDoubleColumnAndIndexSupplier supplier = ScalarDoubleColumnAndIndexSupplier.read(
        ByteOrder.nativeOrder(),
        bitmapSerdeFactory,
        baseBuffer,
        bob,
        NestedFieldColumnIndexSupplierTest.ALWAYS_USE_INDEXES
    );
    try (ScalarDoubleColumn column = (ScalarDoubleColumn) supplier.get()) {
      smokeTest(supplier, column);
    }
  }

  @Test
  public void testConcurrency() throws ExecutionException, InterruptedException
  {
    // if this test ever starts being to be a flake, there might be thread safety issues
    ColumnBuilder bob = new ColumnBuilder();
    bob.setFileMapper(fileMapper);
    ScalarDoubleColumnAndIndexSupplier supplier = ScalarDoubleColumnAndIndexSupplier.read(
        ByteOrder.nativeOrder(),
        bitmapSerdeFactory,
        baseBuffer,
        bob,
        NestedFieldColumnIndexSupplierTest.ALWAYS_USE_INDEXES
    );
    final String expectedReason = "none";
    final AtomicReference<String> failureReason = new AtomicReference<>(expectedReason);

    final int threads = 10;
    ListeningExecutorService executorService = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(threads, "StandardNestedColumnSupplierTest-%d")
    );
    Collection<ListenableFuture<?>> futures = new ArrayList<>(threads);
    final CountDownLatch threadsStartLatch = new CountDownLatch(1);
    for (int i = 0; i < threads; ++i) {
      futures.add(
          executorService.submit(() -> {
            try {
              threadsStartLatch.await();
              for (int iter = 0; iter < 5000; iter++) {
                try (ScalarDoubleColumn column = (ScalarDoubleColumn) supplier.get()) {
                  smokeTest(supplier, column);
                }
              }
            }
            catch (Throwable ex) {
              failureReason.set(ex.getMessage());
            }
          })
      );
    }
    threadsStartLatch.countDown();
    Futures.allAsList(futures).get();
    Assertions.assertEquals(expectedReason, failureReason.get());
  }

  private void smokeTest(ScalarDoubleColumnAndIndexSupplier supplier, ScalarDoubleColumn column)
  {
    SimpleAscendingOffset offset = new SimpleAscendingOffset(data.size());
    NoFilterVectorOffset vectorOffset = new NoFilterVectorOffset(1, 0, data.size());
    ColumnValueSelector<?> valueSelector = column.makeColumnValueSelector(offset);
    VectorValueSelector vectorValueSelector = column.makeVectorValueSelector(vectorOffset);

    ValueIndexes valueIndexes = supplier.as(ValueIndexes.class);
    StringValueSetIndexes valueSetIndex = supplier.as(StringValueSetIndexes.class);
    DruidPredicateIndexes predicateIndex = supplier.as(DruidPredicateIndexes.class);
    NullValueIndex nullValueIndex = supplier.as(NullValueIndex.class);

    SortedMap<String, FieldTypeInfo.MutableTypeSet> fields = column.getFieldTypeInfo();
    Assertions.assertEquals(
        ImmutableMap.of(NestedPathFinder.JSON_PATH_ROOT, new FieldTypeInfo.MutableTypeSet().add(ColumnType.DOUBLE)),
        fields
    );

    for (int i = 0; i < data.size(); i++) {
      Double row = data.get(i);

      // in default value mode, even though the input row had an empty string, the selector spits out null, so we want
      // to take the null checking path

      if (row != null) {
        Assertions.assertEquals(row, valueSelector.getObject());
        Assertions.assertEquals(row, valueSelector.getDouble(), 0.0);
        Assertions.assertFalse(valueSelector.isNull());
        Assertions.assertEquals(row, vectorValueSelector.getDoubleVector()[0], 0.0);
        Assertions.assertEquals(row.longValue(), vectorValueSelector.getLongVector()[0]);
        Assertions.assertEquals(row.floatValue(), vectorValueSelector.getFloatVector()[0], 0.0);
        boolean[] nullVector = vectorValueSelector.getNullVector();
        if (NullHandling.sqlCompatible() && nullVector != null) {
          Assertions.assertFalse(nullVector[0]);
        } else {
          Assertions.assertNull(nullVector);
        }

        Assertions.assertTrue(valueSetIndex.forValue(String.valueOf(row)).computeBitmapResult(resultFactory, false).get(i));
        Assertions.assertTrue(valueIndexes.forValue(row, ColumnType.DOUBLE).computeBitmapResult(resultFactory, false).get(i));
        Assertions.assertTrue(valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of(String.valueOf(row))))
                                       .computeBitmapResult(resultFactory, false)
                                       .get(i));
        Assertions.assertTrue(predicateIndex.forPredicate(new SelectorPredicateFactory(String.valueOf(row)))
                                        .computeBitmapResult(resultFactory, false)
                                        .get(i));
        Assertions.assertFalse(valueSetIndex.forValue(NO_MATCH).computeBitmapResult(resultFactory, false).get(i));
        Assertions.assertFalse(valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of(NO_MATCH)))
                                        .computeBitmapResult(resultFactory, false)
                                        .get(i));
        Assertions.assertFalse(predicateIndex.forPredicate(new SelectorPredicateFactory(NO_MATCH))
                                         .computeBitmapResult(resultFactory, false)
                                         .get(i));
        Assertions.assertFalse(nullValueIndex.get().computeBitmapResult(resultFactory, false).get(i));

      } else {
        if (NullHandling.sqlCompatible()) {
          Assertions.assertNull(valueSelector.getObject());
          Assertions.assertTrue(valueSelector.isNull());
          Assertions.assertTrue(vectorValueSelector.getNullVector()[0]);
          Assertions.assertTrue(valueSetIndex.forValue(null).computeBitmapResult(resultFactory, false).get(i));
          Assertions.assertTrue(nullValueIndex.get().computeBitmapResult(resultFactory, false).get(i));
          Assertions.assertTrue(predicateIndex.forPredicate(new SelectorPredicateFactory(null))
                                          .computeBitmapResult(resultFactory, false)
                                          .get(i));
        } else {
          Assertions.assertEquals(NullHandling.defaultDoubleValue(), valueSelector.getObject());
          Assertions.assertFalse(valueSelector.isNull());
          Assertions.assertEquals(NullHandling.defaultDoubleValue(), vectorValueSelector.getDoubleVector()[0], 0.0);
          Assertions.assertNull(vectorValueSelector.getNullVector());

          Assertions.assertFalse(valueSetIndex.forValue(null).computeBitmapResult(resultFactory, false).get(i));
          Assertions.assertFalse(nullValueIndex.get().computeBitmapResult(resultFactory, false).get(i));
          Assertions.assertFalse(predicateIndex.forPredicate(new SelectorPredicateFactory(null))
                                          .computeBitmapResult(resultFactory, false)
                                          .get(i));
          final String defaultString = String.valueOf(NullHandling.defaultDoubleValue());
          Assertions.assertTrue(valueSetIndex.forValue(defaultString).computeBitmapResult(resultFactory, false).get(i));
          Assertions.assertTrue(predicateIndex.forPredicate(new SelectorPredicateFactory(defaultString))
                                          .computeBitmapResult(resultFactory, false)
                                          .get(i));
        }



        Assertions.assertFalse(valueSetIndex.forValue(NO_MATCH).computeBitmapResult(resultFactory, false).get(i));
        Assertions.assertFalse(valueSetIndex.forValue(NO_MATCH).computeBitmapResult(resultFactory, false).get(i));
        Assertions.assertFalse(predicateIndex.forPredicate(new SelectorPredicateFactory(NO_MATCH))
                                         .computeBitmapResult(resultFactory, false)
                                         .get(i));
      }

      offset.increment();
      vectorOffset.advance();
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
