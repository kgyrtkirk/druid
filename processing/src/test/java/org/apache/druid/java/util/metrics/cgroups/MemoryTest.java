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

package org.apache.druid.java.util.metrics.cgroups;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MemoryTest
{
  @TempDir
  public File temporaryFolder;
  private File procDir;
  private File cgroupDir;
  private CgroupDiscoverer discoverer;

  @BeforeEach
  public void setUp() throws Exception
  {
    cgroupDir = newFolder(temporaryFolder, "junit");
    procDir = newFolder(temporaryFolder, "junit");
    discoverer = new ProcCgroupDiscoverer(procDir.toPath());
    TestUtils.setUpCgroups(procDir, cgroupDir);
    final File memoryDir = new File(
        cgroupDir,
        "memory/system.slice/some.service"
    );

    FileUtils.mkdirp(memoryDir);
    TestUtils.copyResource("/memory.stat", new File(memoryDir, "memory.stat"));
    TestUtils.copyResource("/memory.numa_stat", new File(memoryDir, "memory.numa_stat"));
  }

  @Test
  public void testWontCrash()
  {
    final Memory memory = new Memory((cgroup) -> {
      throw new RuntimeException("shouldContinue");
    });
    final Memory.MemoryStat stat = memory.snapshot();
    Assertions.assertEquals(ImmutableMap.of(), stat.getNumaMemoryStats());
    Assertions.assertEquals(ImmutableMap.of(), stat.getMemoryStats());
  }

  @Test
  public void testSimpleSnapshot()
  {
    final Memory memory = new Memory(discoverer);
    final Memory.MemoryStat stat = memory.snapshot();
    final Map<String, Long> expectedMemoryStats = new HashMap<>();
    expectedMemoryStats.put("inactive_anon", 0L);
    expectedMemoryStats.put("total_pgfault", 13137L);
    expectedMemoryStats.put("total_unevictable", 0L);
    expectedMemoryStats.put("pgfault", 13137L);
    expectedMemoryStats.put("mapped_file", 1327104L);
    expectedMemoryStats.put("total_pgpgout", 5975L);
    expectedMemoryStats.put("total_active_anon", 1757184L);
    expectedMemoryStats.put("total_rss", 1818624L);
    expectedMemoryStats.put("rss", 1818624L);
    expectedMemoryStats.put("total_inactive_anon", 0L);
    expectedMemoryStats.put("active_file", 5873664L);
    expectedMemoryStats.put("total_swap", 0L);
    expectedMemoryStats.put("dirty", 0L);
    expectedMemoryStats.put("total_mapped_file", 1327104L);
    expectedMemoryStats.put("total_rss_huge", 0L);
    expectedMemoryStats.put("total_inactive_file", 2019328L);
    expectedMemoryStats.put("cache", 7892992L);
    expectedMemoryStats.put("rss_huge", 0L);
    expectedMemoryStats.put("shmem", 0L);
    expectedMemoryStats.put("swap", 0L);
    expectedMemoryStats.put("total_pgpgin", 8346L);
    expectedMemoryStats.put("unevictable", 0L);
    expectedMemoryStats.put("active_anon", 1757184L);
    expectedMemoryStats.put("total_dirty", 0L);
    expectedMemoryStats.put("total_active_file", 5873664L);
    expectedMemoryStats.put("hierarchical_memory_limit", 9223372036854771712L);
    expectedMemoryStats.put("total_cache", 7892992L);
    expectedMemoryStats.put("pgpgin", 8346L);
    expectedMemoryStats.put("pgmajfault", 120L);
    expectedMemoryStats.put("inactive_file", 2019328L);
    expectedMemoryStats.put("hierarchical_memsw_limit", 9223372036854771712L);
    expectedMemoryStats.put("writeback", 0L);
    expectedMemoryStats.put("total_shmem", 0L);
    expectedMemoryStats.put("pgpgout", 5975L);
    expectedMemoryStats.put("total_pgmajfault", 120L);
    expectedMemoryStats.put("total_writeback", 0L);
    Assertions.assertEquals(expectedMemoryStats, stat.getMemoryStats());

    final Map<Long, Map<String, Long>> expectedMemoryNumaStats = new HashMap<>();
    final Map<String, Long> expectedNumaNode0Stats = new HashMap<>();
    expectedNumaNode0Stats.put("anon", 432L);
    expectedNumaNode0Stats.put("total", 2359L);
    expectedNumaNode0Stats.put("hierarchical_total", 2359L);
    expectedNumaNode0Stats.put("file", 1927L);
    expectedNumaNode0Stats.put("unevictable", 0L);
    expectedNumaNode0Stats.put("hierarchical_file", 1927L);
    expectedNumaNode0Stats.put("hierarchical_anon", 432L);
    expectedNumaNode0Stats.put("hierarchical_unevictable", 0L);
    expectedMemoryNumaStats.put(0L, expectedNumaNode0Stats);
    Assertions.assertEquals(expectedMemoryNumaStats, stat.getNumaMemoryStats());
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
