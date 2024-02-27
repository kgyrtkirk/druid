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

package org.apache.druid.frame.processor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.ByteTracker;
import org.apache.druid.frame.channel.ReadableFileFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameChannel;
import org.apache.druid.frame.channel.WritableFrameFileChannel;
import org.apache.druid.frame.file.FrameFile;
import org.apache.druid.frame.file.FrameFileWriter;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartition;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.key.KeyTestUtils;
import org.apache.druid.frame.key.RowKey;
import org.apache.druid.frame.key.RowKeyReader;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;



public class SuperSorterTest
{
  private static final Logger log = new Logger(SuperSorterTest.class);

  /**
   * Non-parameterized test cases that
   */
  @Nested
  public class NonParameterizedCasesTest extends InitializedNullHandlingTest
  {
    private static final int NUM_THREADS = 1;
    private static final int FRAME_SIZE = 1_000_000;

    @TempDir
    public File temporaryFolder;

    private FrameProcessorExecutor exec;

    @BeforeEach
    public void setUp()
    {
      exec = new FrameProcessorExecutor(
          MoreExecutors.listeningDecorator(Execs.multiThreaded(NUM_THREADS, "super-sorter-test-%d"))
      );
    }

    @AfterEach
    public void tearDown()
    {
      exec.getExecutorService().shutdownNow();
    }

    @Test
    public void testSingleEmptyInputChannel_fileStorage() throws Exception
    {
      final BlockingQueueFrameChannel inputChannel = BlockingQueueFrameChannel.minimal();
      inputChannel.writable().close();

      final SettableFuture<ClusterByPartitions> outputPartitionsFuture = SettableFuture.create();
      final SuperSorterProgressTracker superSorterProgressTracker = new SuperSorterProgressTracker();

      final File tempFolder = newFolder(temporaryFolder, "junit");
      final SuperSorter superSorter = new SuperSorter(
          Collections.singletonList(inputChannel.readable()),
          FrameReader.create(RowSignature.empty()),
          Collections.emptyList(),
          outputPartitionsFuture,
          exec,
          new FileOutputChannelFactory(tempFolder, FRAME_SIZE, null),
          new FileOutputChannelFactory(tempFolder, FRAME_SIZE, null),
          2,
          2,
          -1,
          null,
          superSorterProgressTracker
      );

      superSorter.setNoWorkRunnable(() -> outputPartitionsFuture.set(ClusterByPartitions.oneUniversalPartition()));
      final OutputChannels channels = superSorter.run().get();
      Assertions.assertEquals(1, channels.getAllChannels().size());

      final ReadableFrameChannel channel = Iterables.getOnlyElement(channels.getAllChannels()).getReadableChannel();
      Assertions.assertTrue(channel.isFinished());
      Assertions.assertEquals(1.0, superSorterProgressTracker.snapshot().getProgressDigest(), 0.0f);
      channel.close();
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

  /**
   * Parameterized test cases that use {@link TestIndex#getNoRollupIncrementalTestIndex} with various frame sizes,
   * numbers of channels, and worker configurations.
   */
  @Nested
  public class ParameterizedCasesTest extends InitializedNullHandlingTest
  {
    @TempDir
    public File temporaryFolder;

    private int maxRowsPerFrame;
    private int maxBytesPerFrame;
    private int numChannels;
    private int maxActiveProcessors;
    private int maxChannelsPerProcessor;
    private int numThreads;
    private boolean isComposedStorage;

    private StorageAdapter adapter;
    private RowSignature signature;
    private FrameProcessorExecutor exec;
    private List<ReadableFrameChannel> inputChannels;
    private FrameReader frameReader;

    public void initParameterizedCasesTest(
        int maxRowsPerFrame,
        int maxBytesPerFrame,
        int numChannels,
        int maxActiveProcessors,
        int maxChannelsPerProcessor,
        int numThreads,
        boolean isComposedStorage
    )
    {
      this.maxRowsPerFrame = maxRowsPerFrame;
      this.maxBytesPerFrame = maxBytesPerFrame;
      this.numChannels = numChannels;
      this.maxActiveProcessors = maxActiveProcessors;
      this.maxChannelsPerProcessor = maxChannelsPerProcessor;
      this.numThreads = numThreads;
      this.isComposedStorage = isComposedStorage;
    }

    public static Iterable<Object[]> constructorFeeder()
    {
      final List<Object[]> constructors = new ArrayList<>();

      for (int maxRowsPerFrame : new int[]{Integer.MAX_VALUE, 50, 1}) {
        for (int maxBytesPerFrame : new int[]{20000, 200000}) {
          for (int numChannels : new int[]{1, 3}) {
            for (int maxActiveProcessors : new int[]{1, 2, 4}) {
              for (int maxChannelsPerProcessor : new int[]{2, 3, 8}) {
                for (int numThreads : new int[]{1, 3}) {
                  for (boolean isComposedStorage : new boolean[]{true, false}) {
                    if (maxActiveProcessors >= maxChannelsPerProcessor) {
                      constructors.add(
                          new Object[]{
                              maxRowsPerFrame,
                              maxBytesPerFrame,
                              numChannels,
                              maxActiveProcessors,
                              maxChannelsPerProcessor,
                              numThreads,
                              isComposedStorage
                          }
                      );
                    }
                  }
                }
              }
            }
          }
        }
      }

      return constructors;
    }

    @BeforeEach
    public void setUp()
    {
      exec = new FrameProcessorExecutor(
          MoreExecutors.listeningDecorator(Execs.multiThreaded(numThreads, getClass().getSimpleName() + "[%d]"))
      );
      adapter = new QueryableIndexStorageAdapter(TestIndex.getNoRollupMMappedTestIndex());
    }

    @AfterEach
    public void tearDown() throws Exception
    {
      if (exec != null) {
        exec.getExecutorService().shutdownNow();
        if (!exec.getExecutorService().awaitTermination(5, TimeUnit.SECONDS)) {
          log.warn("Executor did not terminate after 5 seconds");
        }
      }
    }

    /**
     * Creates input channels.
     *
     * Sets {@link #inputChannels}, {@link #signature}, and {@link #frameReader}.
     */
    private void setUpInputChannels(final ClusterBy clusterBy) throws Exception
    {
      if (signature != null || inputChannels != null) {
        throw new ISE("Channels already created for this case");
      }

      final FrameSequenceBuilder frameSequenceBuilder =
          FrameSequenceBuilder.fromAdapter(adapter)
                              .maxRowsPerFrame(maxRowsPerFrame)
                              .sortBy(clusterBy.getColumns())
                              .allocator(ArenaMemoryAllocator.create(ByteBuffer.allocate(maxBytesPerFrame)))
                              .frameType(FrameType.ROW_BASED)
                              .populateRowNumber();

      inputChannels = makeFileChannels(frameSequenceBuilder.frames(), newFolder(temporaryFolder, "junit"), numChannels);
      signature = frameSequenceBuilder.signature();
      frameReader = FrameReader.create(signature);
    }

    private OutputChannels verifySuperSorter(
        final ClusterBy clusterBy,
        final ClusterByPartitions clusterByPartitions
    ) throws Exception
    {
      final File tempFolder = newFolder(temporaryFolder, "junit");
      final OutputChannelFactory outputChannelFactory = isComposedStorage ? new ComposingOutputChannelFactory(
          ImmutableList.of(
              new FileOutputChannelFactory(new File(tempFolder, "1"), maxBytesPerFrame, null),
              new FileOutputChannelFactory(new File(tempFolder, "2"), maxBytesPerFrame, null)
          ),
          maxBytesPerFrame
      ) : new FileOutputChannelFactory(tempFolder, maxBytesPerFrame, null);
      final RowKeyReader keyReader = clusterBy.keyReader(signature);
      final Comparator<RowKey> keyComparator = clusterBy.keyComparator();
      final SettableFuture<ClusterByPartitions> clusterByPartitionsFuture = SettableFuture.create();
      final SuperSorterProgressTracker superSorterProgressTracker = new SuperSorterProgressTracker();

      final SuperSorter superSorter = new SuperSorter(
          inputChannels,
          frameReader,
          clusterBy.getColumns(),
          clusterByPartitionsFuture,
          exec,
          new FileOutputChannelFactory(tempFolder, maxBytesPerFrame, null),
          outputChannelFactory,
          maxActiveProcessors,
          maxChannelsPerProcessor,
          -1,
          null,
          superSorterProgressTracker
      );

      superSorter.setNoWorkRunnable(() -> clusterByPartitionsFuture.set(clusterByPartitions));
      final OutputChannels outputChannels = superSorter.run().get();
      Assertions.assertEquals(clusterByPartitions.size(), outputChannels.getAllChannels().size());
      Assertions.assertEquals(1.0, superSorterProgressTracker.snapshot().getProgressDigest(), 0.0f);

      final int[] clusterByPartColumns = clusterBy.getColumns().stream().mapToInt(
          part -> signature.indexOf(part.columnName())
      ).toArray();

      final List<Sequence<List<Object>>> outputSequences = new ArrayList<>();
      for (int partitionNumber : outputChannels.getPartitionNumbers()) {
        final ClusterByPartition partition = clusterByPartitions.get(partitionNumber);
        final ReadableFrameChannel outputChannel =
            Iterables.getOnlyElement(outputChannels.getChannelsForPartition(partitionNumber)).getReadableChannel();

        // Validate that everything in this channel is in the correct key range.
        FrameTestUtil.readRowsFromFrameChannel(
            duplicateOutputChannel(outputChannel),
            frameReader
        ).forEach(
            row -> {
              final Object[] array = new Object[clusterByPartColumns.length];

              for (int i = 0; i < array.length; i++) {
                array[i] = row.get(clusterByPartColumns[i]);
              }

              final RowKey key = createKey(clusterBy, array);

              Assertions.assertTrue(
                  partition.getStart() == null || keyComparator.compare(key, partition.getStart()) >= 0,
                  StringUtils.format(
                      "Key %s >= partition %,d start %s",
                      keyReader.read(key),
                      partitionNumber,
                      partition.getStart() == null ? null : keyReader.read(partition.getStart())
                  )
              );

              Assertions.assertTrue(
                  partition.getEnd() == null || keyComparator.compare(key, partition.getEnd()) < 0,
                  StringUtils.format(
                      "Key %s < partition %,d end %s",
                      keyReader.read(key),
                      partitionNumber,
                      partition.getEnd() == null ? null : keyReader.read(partition.getEnd())
                  )
              );
            }
        );

        outputSequences.add(
            FrameTestUtil.readRowsFromFrameChannel(
                duplicateOutputChannel(outputChannel),
                frameReader
            )
        );
      }

      final Sequence<List<Object>> expectedRows = Sequences.sort(
          FrameTestUtil.readRowsFromAdapter(adapter, signature, true),
          Comparator.comparing(
              row -> {
                final Object[] array = new Object[clusterByPartColumns.length];

                for (int i = 0; i < array.length; i++) {
                  array[i] = row.get(clusterByPartColumns[i]);
                }

                return createKey(clusterBy, array);
              },
              keyComparator
          )
      );

      FrameTestUtil.assertRowsEqual(expectedRows, Sequences.concat(outputSequences));

      return outputChannels;
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "maxRowsPerFrame = {0}, "
        + "maxBytesPerFrame = {1}, "
        + "numChannels = {2}, "
        + "maxActiveProcessors = {3}, "
        + "maxChannelsPerProcessor = {4}, "
        + "numThreads = {5}, "
        + "isComposedStorage = {6}")
    public void test_clusterByQualityLongAscRowNumberAsc_onePartition(int maxRowsPerFrame, int maxBytesPerFrame, int numChannels, int maxActiveProcessors, int maxChannelsPerProcessor, int numThreads, boolean isComposedStorage) throws Exception
    {
      initParameterizedCasesTest(maxRowsPerFrame, maxBytesPerFrame, numChannels, maxActiveProcessors, maxChannelsPerProcessor, numThreads, isComposedStorage);
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("qualityLong", KeyOrder.ASCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(clusterBy);
      verifySuperSorter(clusterBy, ClusterByPartitions.oneUniversalPartition());
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "maxRowsPerFrame = {0}, "
        + "maxBytesPerFrame = {1}, "
        + "numChannels = {2}, "
        + "maxActiveProcessors = {3}, "
        + "maxChannelsPerProcessor = {4}, "
        + "numThreads = {5}, "
        + "isComposedStorage = {6}")
    public void test_clusterByQualityLongAscRowNumberAsc_twoPartitionsOneEmpty(int maxRowsPerFrame, int maxBytesPerFrame, int numChannels, int maxActiveProcessors, int maxChannelsPerProcessor, int numThreads, boolean isComposedStorage) throws Exception
    {
      initParameterizedCasesTest(maxRowsPerFrame, maxBytesPerFrame, numChannels, maxActiveProcessors, maxChannelsPerProcessor, numThreads, isComposedStorage);
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("qualityLong", KeyOrder.ASCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final RowKey zeroZero = createKey(clusterBy, 0L, 0L);
      final OutputChannels outputChannels = verifySuperSorter(
          clusterBy,
          new ClusterByPartitions(
              ImmutableList.of(
                  new ClusterByPartition(null, zeroZero), // empty partition
                  new ClusterByPartition(zeroZero, null) // all data goes in here
              )
          )
      );

      // Verify that one of the partitions is actually empty.
      Assertions.assertEquals(
          0,
          countSequence(
              FrameTestUtil.readRowsFromFrameChannel(
                  Iterables.getOnlyElement(outputChannels.getChannelsForPartition(0)).getReadableChannel(),
                  frameReader
              )
          )
      );

      // Verify that the other partition has all data in it.
      Assertions.assertEquals(
          adapter.getNumRows(),
          countSequence(
              FrameTestUtil.readRowsFromFrameChannel(
                  Iterables.getOnlyElement(outputChannels.getChannelsForPartition(1)).getReadableChannel(),
                  frameReader
              )
          )
      );
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "maxRowsPerFrame = {0}, "
        + "maxBytesPerFrame = {1}, "
        + "numChannels = {2}, "
        + "maxActiveProcessors = {3}, "
        + "maxChannelsPerProcessor = {4}, "
        + "numThreads = {5}, "
        + "isComposedStorage = {6}")
    public void test_clusterByQualityDescRowNumberAsc_fourPartitions(int maxRowsPerFrame, int maxBytesPerFrame, int numChannels, int maxActiveProcessors, int maxChannelsPerProcessor, int numThreads, boolean isComposedStorage) throws Exception
    {
      initParameterizedCasesTest(maxRowsPerFrame, maxBytesPerFrame, numChannels, maxActiveProcessors, maxChannelsPerProcessor, numThreads, isComposedStorage);
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("quality", KeyOrder.DESCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final ClusterByPartitions partitions = new ClusterByPartitions(
          ImmutableList.of(
              new ClusterByPartition(
                  createKey(clusterBy, "travel", 8L),
                  createKey(clusterBy, "premium", 506L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, "premium", 506L),
                  createKey(clusterBy, "mezzanine", 204L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, "mezzanine", 204L),
                  createKey(clusterBy, "health", 900L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, "health", 900L),
                  null
              )
          )
      );

      Assertions.assertEquals(4, partitions.size());

      verifySuperSorter(clusterBy, partitions);
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "maxRowsPerFrame = {0}, "
        + "maxBytesPerFrame = {1}, "
        + "numChannels = {2}, "
        + "maxActiveProcessors = {3}, "
        + "maxChannelsPerProcessor = {4}, "
        + "numThreads = {5}, "
        + "isComposedStorage = {6}")
    public void test_clusterByTimeAscMarketAscRowNumberAsc_fourPartitions(int maxRowsPerFrame, int maxBytesPerFrame, int numChannels, int maxActiveProcessors, int maxChannelsPerProcessor, int numThreads, boolean isComposedStorage) throws Exception
    {
      initParameterizedCasesTest(maxRowsPerFrame, maxBytesPerFrame, numChannels, maxActiveProcessors, maxChannelsPerProcessor, numThreads, isComposedStorage);
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn(ColumnHolder.TIME_COLUMN_NAME, KeyOrder.ASCENDING),
              new KeyColumn("market", KeyOrder.ASCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final ClusterByPartitions partitions = new ClusterByPartitions(
          ImmutableList.of(
              new ClusterByPartition(
                  createKey(clusterBy, 1294790400000L, "spot", 0L),
                  createKey(clusterBy, 1296864000000L, "spot", 302L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1296864000000L, "spot", 302L),
                  createKey(clusterBy, 1298851200000L, "spot", 604L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1298851200000L, "spot", 604L),
                  createKey(clusterBy, 1300838400000L, "total_market", 906L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1300838400000L, "total_market", 906L),
                  null
              )
          )
      );

      Assertions.assertEquals(4, partitions.size());

      verifySuperSorter(clusterBy, partitions);
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "maxRowsPerFrame = {0}, "
        + "maxBytesPerFrame = {1}, "
        + "numChannels = {2}, "
        + "maxActiveProcessors = {3}, "
        + "maxChannelsPerProcessor = {4}, "
        + "numThreads = {5}, "
        + "isComposedStorage = {6}")
    public void test_clusterByPlacementishDescRowNumberAsc_fourPartitions(int maxRowsPerFrame, int maxBytesPerFrame, int numChannels, int maxActiveProcessors, int maxChannelsPerProcessor, int numThreads, boolean isComposedStorage) throws Exception
    {
      initParameterizedCasesTest(maxRowsPerFrame, maxBytesPerFrame, numChannels, maxActiveProcessors, maxChannelsPerProcessor, numThreads, isComposedStorage);
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("placementish", KeyOrder.DESCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final ClusterByPartitions partitions = new ClusterByPartitions(
          ImmutableList.of(
              new ClusterByPartition(
                  createKey(clusterBy, ImmutableList.of("preferred", "t"), 7L),
                  createKey(clusterBy, ImmutableList.of("p", "preferred"), 506L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, ImmutableList.of("p", "preferred"), 506L),
                  createKey(clusterBy, ImmutableList.of("m", "preferred"), 204L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, ImmutableList.of("m", "preferred"), 204L),
                  createKey(clusterBy, ImmutableList.of("h", "preferred"), 900L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, ImmutableList.of("h", "preferred"), 900L),
                  null
              )
          )
      );

      Assertions.assertEquals(4, partitions.size());

      verifySuperSorter(clusterBy, partitions);
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "maxRowsPerFrame = {0}, "
        + "maxBytesPerFrame = {1}, "
        + "numChannels = {2}, "
        + "maxActiveProcessors = {3}, "
        + "maxChannelsPerProcessor = {4}, "
        + "numThreads = {5}, "
        + "isComposedStorage = {6}")
    public void test_clusterByQualityLongDescRowNumberAsc_fourPartitions(int maxRowsPerFrame, int maxBytesPerFrame, int numChannels, int maxActiveProcessors, int maxChannelsPerProcessor, int numThreads, boolean isComposedStorage) throws Exception
    {
      initParameterizedCasesTest(maxRowsPerFrame, maxBytesPerFrame, numChannels, maxActiveProcessors, maxChannelsPerProcessor, numThreads, isComposedStorage);
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("qualityLong", KeyOrder.DESCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final ClusterByPartitions partitions = new ClusterByPartitions(
          ImmutableList.of(
              new ClusterByPartition(
                  createKey(clusterBy, 1800L, 8L),
                  createKey(clusterBy, 1600L, 506L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1600L, 506L),
                  createKey(clusterBy, 1400L, 204L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1400L, 204L),
                  createKey(clusterBy, 1300L, 900L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1300L, 900L),
                  null
              )
          )
      );

      Assertions.assertEquals(4, partitions.size());

      verifySuperSorter(clusterBy, partitions);
    }

    @MethodSource("constructorFeeder")
    @ParameterizedTest(name = "maxRowsPerFrame = {0}, "
        + "maxBytesPerFrame = {1}, "
        + "numChannels = {2}, "
        + "maxActiveProcessors = {3}, "
        + "maxChannelsPerProcessor = {4}, "
        + "numThreads = {5}, "
        + "isComposedStorage = {6}")
    public void test_clusterByQualityLongDescRowNumberAsc_fourPartitions_durableStorage(int maxRowsPerFrame, int maxBytesPerFrame, int numChannels, int maxActiveProcessors, int maxChannelsPerProcessor, int numThreads, boolean isComposedStorage) throws Exception
    {
      initParameterizedCasesTest(maxRowsPerFrame, maxBytesPerFrame, numChannels, maxActiveProcessors, maxChannelsPerProcessor, numThreads, isComposedStorage);
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new KeyColumn("qualityLong", KeyOrder.DESCENDING),
              new KeyColumn(FrameTestUtil.ROW_NUMBER_COLUMN, KeyOrder.ASCENDING)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final ClusterByPartitions partitions = new ClusterByPartitions(
          ImmutableList.of(
              new ClusterByPartition(
                  createKey(clusterBy, 1800L, 8L),
                  createKey(clusterBy, 1600L, 506L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1600L, 506L),
                  createKey(clusterBy, 1400L, 204L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1400L, 204L),
                  createKey(clusterBy, 1300L, 900L)
              ),
              new ClusterByPartition(
                  createKey(clusterBy, 1300L, 900L),
                  null
              )
          )
      );

      Assertions.assertEquals(4, partitions.size());

      verifySuperSorter(clusterBy, partitions);
    }

    private RowKey createKey(final ClusterBy clusterBy, final Object... objects)
    {
      final RowSignature keySignature = KeyTestUtils.createKeySignature(clusterBy.getColumns(), signature);
      return KeyTestUtils.createKey(keySignature, objects);
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

  private static List<ReadableFrameChannel> makeFileChannels(
      final Sequence<Frame> frames,
      final File tmpDir,
      final int numChannels
  ) throws IOException
  {
    final List<File> files = new ArrayList<>();
    final List<WritableFrameChannel> writableChannels = new ArrayList<>();

    for (int i = 0; i < numChannels; i++) {
      final File file = new File(tmpDir, StringUtils.format("channel-%d", i));
      files.add(file);
      writableChannels.add(
          new WritableFrameFileChannel(
              FrameFileWriter.open(
                  Channels.newChannel(Files.newOutputStream(file.toPath())),
                  null,
                  ByteTracker.unboundedTracker()
              )
          )
      );
    }

    frames.forEach(
        new Consumer<Frame>()
        {
          private int i;

          @Override
          public void accept(final Frame frame)
          {
            try {
              writableChannels.get(i % writableChannels.size()).write(frame);
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }

            i++;
          }
        }
    );

    final List<ReadableFrameChannel> retVal = new ArrayList<>();

    for (int i = 0; i < writableChannels.size(); i++) {
      WritableFrameChannel writableChannel = writableChannels.get(i);
      writableChannel.close();
      retVal.add(new ReadableFileFrameChannel(FrameFile.open(files.get(i), null)));
    }

    return retVal;
  }

  private static ReadableFrameChannel duplicateOutputChannel(final ReadableFrameChannel channel)
  {
    return new ReadableFileFrameChannel(((ReadableFileFrameChannel) channel).newFrameFileReference());
  }

  private static <T> long countSequence(final Sequence<T> sequence)
  {
    return sequence.accumulate(
        0L,
        (accumulated, in) -> accumulated + 1
    );
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
