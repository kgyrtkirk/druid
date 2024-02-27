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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ProcCgroupDiscovererTest
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
  }

  @Test
  public void testSimpleProc()
  {
    Assertions.assertEquals(
        new File(
            cgroupDir,
            "cpu,cpuacct/system.slice/some.service/f12ba7e0-fa16-462e-bb9d-652ccc27f0ee"
        ).toPath(),
        discoverer.discover("cpu")
    );
  }

  @Test
  public void testParse()
  {
    final ProcCgroupDiscoverer.ProcMountsEntry entry = ProcCgroupDiscoverer.ProcMountsEntry.parse(
        "/dev/md126 /ebs xfs rw,seclabel,noatime,attr2,inode64,sunit=1024,swidth=16384,noquota 0 0"
    );
    Assertions.assertEquals("/dev/md126", entry.dev);
    Assertions.assertEquals(Paths.get("/ebs"), entry.path);
    Assertions.assertEquals("xfs", entry.type);
    Assertions.assertEquals(ImmutableSet.of(
        "rw",
        "seclabel",
        "noatime",
        "attr2",
        "inode64",
        "sunit=1024",
        "swidth=16384",
        "noquota"
    ), entry.options);
  }

  @Test
  public void testNullCgroup()
  {
    assertThrows(NullPointerException.class, () -> {
      Assertions.assertNull(new ProcCgroupDiscoverer(procDir.toPath()).discover(null));
    });
  }

  @Test
  public void testFallBack() throws Exception
  {
    File temporaryFolder = new File();
    File cgroupDir = newFolder(temporaryFolder, "junit");
    File procDir = newFolder(temporaryFolder, "junit");
    TestUtils.setUpCgroups(procDir, cgroupDir);

    // Swap out the cgroup path with a default path
    FileUtils.deleteDirectory(new File(
        cgroupDir,
        "cpu,cpuacct/"
    ));

    Assertions.assertTrue(new File(
        cgroupDir,
        "cpu,cpuacct/"
    ).mkdir());

    Assertions.assertEquals(
        new File(
            cgroupDir,
            "cpu,cpuacct"
        ).toPath(),
        new ProcCgroupDiscoverer(procDir.toPath()).discover("cpu")
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
