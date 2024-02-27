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

package org.apache.druid.java.util.common.io;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class NativeIOTest
{
  @TempDir
  public File tempFolder;

  @Test
  public void testChunkedCopy() throws Exception
  {
    File f = File.createTempFile("junit", null, tempFolder);
    byte[] bytes = new byte[]{(byte) 0x8, (byte) 0x9};

    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    NativeIO.chunkedCopy(bis, f);

    byte[] data = Files.readAllBytes(f.toPath());
    Assertions.assertTrue(Arrays.equals(bytes, data));
  }

  @Test
  public void testException() throws Exception
  {
    assertThrows(IOException.class, () -> {
      File dir = newFolder(tempFolder, "junit");
      NativeIO.chunkedCopy(null, dir);
    });
  }

  @Test
  public void testDisabledFadviseChunkedCopy() throws Exception
  {
    boolean possible = NativeIO.isFadvisePossible();

    NativeIO.setFadvisePossible(false);
    File f = File.createTempFile("junit", null, tempFolder);
    byte[] bytes = new byte[]{(byte) 0x8, (byte) 0x9};

    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    NativeIO.chunkedCopy(bis, f);

    byte[] data = Files.readAllBytes(f.toPath());

    NativeIO.setFadvisePossible(possible);
    Assertions.assertTrue(Arrays.equals(bytes, data));
  }

  @Test
  public void testDisabledSyncFileRangePossible() throws Exception
  {
    boolean possible = NativeIO.isSyncFileRangePossible();

    NativeIO.setSyncFileRangePossible(false);
    File f = File.createTempFile("junit", null, tempFolder);
    byte[] bytes = new byte[]{(byte) 0x8, (byte) 0x9};

    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    NativeIO.chunkedCopy(bis, f);

    byte[] data = Files.readAllBytes(f.toPath());

    NativeIO.setSyncFileRangePossible(possible);
    Assertions.assertTrue(Arrays.equals(bytes, data));
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
