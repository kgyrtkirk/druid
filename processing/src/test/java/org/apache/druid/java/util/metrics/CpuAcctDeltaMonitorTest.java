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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.metrics.cgroups.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class CpuAcctDeltaMonitorTest
{
  @TempDir
  public File temporaryFolder;
  private File procDir;
  private File cgroupDir;
  private File cpuacctDir;

  @BeforeEach
  public void setUp() throws IOException
  {
    cgroupDir = newFolder(temporaryFolder, "junit");
    procDir = newFolder(temporaryFolder, "junit");
    TestUtils.setUpCgroups(procDir, cgroupDir);
    cpuacctDir = new File(
        cgroupDir,
        "cpu,cpuacct/system.slice/some.service/f12ba7e0-fa16-462e-bb9d-652ccc27f0ee"
    );

    FileUtils.mkdirp(cpuacctDir);
    TestUtils.copyResource("/cpuacct.usage_all", new File(cpuacctDir, "cpuacct.usage_all"));
  }

  @Test
  public void testMonitorWontCrash()
  {
    final CpuAcctDeltaMonitor monitor = new CpuAcctDeltaMonitor(
        "some_feed",
        ImmutableMap.of(),
        cgroup -> {
          throw new RuntimeException("Should continue");
        }
    );
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);
    monitor.doMonitor(emitter);
    monitor.doMonitor(emitter);
    Assertions.assertTrue(emitter.getEvents().isEmpty());
  }

  @Test
  public void testSimpleMonitor() throws Exception
  {
    final File cpuacct = new File(cpuacctDir, "cpuacct.usage_all");
    try (final FileOutputStream fos = new FileOutputStream(cpuacct)) {
      fos.write(StringUtils.toUtf8("cpu user system\n"));
      for (int i = 0; i < 128; ++i) {
        fos.write(StringUtils.toUtf8(StringUtils.format("%d 0 0\n", i)));
      }
    }
    final CpuAcctDeltaMonitor monitor = new CpuAcctDeltaMonitor(
        "some_feed",
        ImmutableMap.of(),
        (cgroup) -> cpuacctDir.toPath()
    );
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    Assertions.assertFalse(monitor.doMonitor(emitter));
    // First should just cache
    Assertions.assertEquals(0, emitter.getEvents().size());
    Assertions.assertTrue(cpuacct.delete());
    TestUtils.copyResource("/cpuacct.usage_all", cpuacct);
    Assertions.assertTrue(monitor.doMonitor(emitter));
    Assertions.assertEquals(2 * 128 + 1, emitter.getEvents().size());
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
