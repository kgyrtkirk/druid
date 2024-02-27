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
import java.util.Map;

public class CgroupCpuMonitorTest
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
    final File cpuDir = new File(
        cgroupDir,
        "cpu,cpuacct/system.slice/some.service/f12ba7e0-fa16-462e-bb9d-652ccc27f0ee"
    );

    FileUtils.mkdirp(cpuDir);
    TestUtils.copyOrReplaceResource("/cpu.shares", new File(cpuDir, "cpu.shares"));
    TestUtils.copyOrReplaceResource("/cpu.cfs_quota_us", new File(cpuDir, "cpu.cfs_quota_us"));
    TestUtils.copyOrReplaceResource("/cpu.cfs_period_us", new File(cpuDir, "cpu.cfs_period_us"));
  }

  @Test
  public void testMonitor()
  {
    final CgroupCpuMonitor monitor = new CgroupCpuMonitor(discoverer, ImmutableMap.of(), "some_feed");
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    Assertions.assertTrue(monitor.doMonitor(emitter));
    final List<Event> actualEvents = emitter.getEvents();
    Assertions.assertEquals(2, actualEvents.size());
    final Map<String, Object> sharesEvent = actualEvents.get(0).toMap();
    final Map<String, Object> coresEvent = actualEvents.get(1).toMap();
    Assertions.assertEquals("cgroup/cpu/shares", sharesEvent.get("metric"));
    Assertions.assertEquals(1024L, sharesEvent.get("value"));
    Assertions.assertEquals("cgroup/cpu/cores_quota", coresEvent.get("metric"));
    Assertions.assertEquals(3.0D, coresEvent.get("value"));
  }

  @Test
  public void testQuotaCompute()
  {
    Assertions.assertEquals(-1, CgroupCpuMonitor.computeProcessorQuota(-1, 100000), 0);
    Assertions.assertEquals(0, CgroupCpuMonitor.computeProcessorQuota(0, 100000), 0);
    Assertions.assertEquals(-1, CgroupCpuMonitor.computeProcessorQuota(100000, 0), 0);
    Assertions.assertEquals(2.0D, CgroupCpuMonitor.computeProcessorQuota(200000, 100000), 0);
    Assertions.assertEquals(0.5D, CgroupCpuMonitor.computeProcessorQuota(50000, 100000), 0);
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
