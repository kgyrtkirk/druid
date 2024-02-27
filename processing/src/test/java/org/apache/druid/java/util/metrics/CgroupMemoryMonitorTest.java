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

package org.apache.druid.java.util.metrics;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.ProcCgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class CgroupMemoryMonitorTest
{
  @TempDir
  public File temporaryFolder;
  private File procDir;
  private File cgroupDir;
  private CgroupDiscoverer discoverer;

  @BeforeEach
  public void setUp() throws IOException
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
  public void testMonitor()
  {
    final CgroupMemoryMonitor monitor = new CgroupMemoryMonitor(discoverer, ImmutableMap.of(), "some_feed");
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    Assertions.assertTrue(monitor.doMonitor(emitter));
    final List<Event> actualEvents = emitter.getEvents();
    Assertions.assertEquals(44, actualEvents.size());
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
