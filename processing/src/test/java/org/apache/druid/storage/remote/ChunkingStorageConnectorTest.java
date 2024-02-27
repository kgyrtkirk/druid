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

package org.apache.druid.storage.remote;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.apache.druid.storage.StorageConnector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ChunkingStorageConnectorTest
{

  @TempDir
  public File temporaryFolder;

  private StorageConnector storageConnector;

  @BeforeEach
  public void setup() throws IOException
  {
    storageConnector = new TestStorageConnector(newFolder(temporaryFolder, "junit"));
  }

  @Test
  public void testRead() throws IOException
  {
    InputStream is = storageConnector.read("");
    byte[] dataBytes = IOUtils.toByteArray(is);
    Assertions.assertEquals(TestStorageConnector.DATA, new String(dataBytes, StandardCharsets.UTF_8));
  }

  @Test
  public void testReadRange() throws IOException
  {

    List<Integer> ranges = ImmutableList.of(
        TestStorageConnector.CHUNK_SIZE_BYTES,
        TestStorageConnector.CHUNK_SIZE_BYTES * 2,
        TestStorageConnector.CHUNK_SIZE_BYTES * 7,
        TestStorageConnector.CHUNK_SIZE_BYTES + 1,
        TestStorageConnector.CHUNK_SIZE_BYTES + 2,
        TestStorageConnector.CHUNK_SIZE_BYTES + 3
    );

    List<Integer> startPositions = ImmutableList.of(0, 25, 37, TestStorageConnector.DATA.length() - 10);

    for (int range : ranges) {
      for (int startPosition : startPositions) {
        int limitedRange = startPosition + range > TestStorageConnector.DATA.length()
                           ? TestStorageConnector.DATA.length() - startPosition
                           : range;
        InputStream is = storageConnector.readRange("", startPosition, limitedRange);
        byte[] dataBytes = IOUtils.toByteArray(is);
        Assertions.assertEquals(
            TestStorageConnector.DATA.substring(startPosition, startPosition + limitedRange),
            new String(dataBytes, StandardCharsets.UTF_8)
        );
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
