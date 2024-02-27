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

package org.apache.druid.java.util.common;

import com.google.common.base.Predicates;
import org.hamcrest.CoreMatchers;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;

import static org.hamcrest.MatcherAssert.assertThat;

public class FileUtilsTest
{
  @TempDir
  public File temporaryFolder;

  @Test
  public void testMap() throws IOException
  {
    File dataFile = File.createTempFile("data", null, temporaryFolder);
    long buffersMemoryBefore = BufferUtils.totalMemoryUsedByDirectAndMappedBuffers();
    try (RandomAccessFile raf = new RandomAccessFile(dataFile, "rw")) {
      raf.write(42);
      raf.setLength(1 << 20); // 1 MiB
    }
    try (MappedByteBufferHandler mappedByteBufferHandler = FileUtils.map(dataFile)) {
      Assertions.assertEquals(42, mappedByteBufferHandler.get().get(0));
    }
    long buffersMemoryAfter = BufferUtils.totalMemoryUsedByDirectAndMappedBuffers();
    Assertions.assertEquals(buffersMemoryBefore, buffersMemoryAfter);
  }

  @Test
  public void testMapFileTooLarge() throws IOException
  {
    File dataFile = File.createTempFile("data", null, temporaryFolder);
    try (RandomAccessFile raf = new RandomAccessFile(dataFile, "rw")) {
      raf.write(42);
      raf.setLength(1 << 20); // 1 MiB
    }
    final IllegalArgumentException e = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> FileUtils.map(dataFile, 0, (long) Integer.MAX_VALUE + 1)
    );
    assertThat(e.getMessage(), CoreMatchers.containsString("Cannot map region larger than"));
  }

  @Test
  public void testMapRandomAccessFileTooLarge() throws IOException
  {
    File dataFile = File.createTempFile("data", null, temporaryFolder);
    try (RandomAccessFile raf = new RandomAccessFile(dataFile, "rw")) {
      raf.write(42);
      raf.setLength(1 << 20); // 1 MiB
    }
    try (RandomAccessFile raf = new RandomAccessFile(dataFile, "r")) {
      final IllegalArgumentException e = Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> FileUtils.map(raf, 0, (long) Integer.MAX_VALUE + 1)
      );
      assertThat(e.getMessage(), CoreMatchers.containsString("Cannot map region larger than"));
    }
  }

  @Test
  public void testWriteAtomically() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "junit");
    final File tmpFile = new File(tmpDir, "file1");
    FileUtils.writeAtomically(tmpFile, out -> {
      out.write(StringUtils.toUtf8("foo"));
      return null;
    });
    Assertions.assertEquals("foo", StringUtils.fromUtf8(Files.readAllBytes(tmpFile.toPath())));

    // Try writing again, throw error partway through.
    try {
      FileUtils.writeAtomically(tmpFile, out -> {
        out.write(StringUtils.toUtf8("bar"));
        out.flush();
        throw new ISE("OMG!");
      });
    }
    catch (IllegalStateException e) {
      // Suppress
    }
    Assertions.assertEquals("foo", StringUtils.fromUtf8(Files.readAllBytes(tmpFile.toPath())));

    FileUtils.writeAtomically(tmpFile, out -> {
      out.write(StringUtils.toUtf8("baz"));
      return null;
    });
    Assertions.assertEquals("baz", StringUtils.fromUtf8(Files.readAllBytes(tmpFile.toPath())));
  }

  @Test
  public void testCreateTempDir() throws IOException
  {
    final File tempDir = FileUtils.createTempDir();
    try {
      Assertions.assertEquals(
          new File(System.getProperty("java.io.tmpdir")).toPath(),
          tempDir.getParentFile().toPath()
      );
    }
    finally {
      Files.delete(tempDir.toPath());
    }
  }

  @Test
  public void testCreateTempDirInLocation() throws IOException
  {
    final File baseDir = newFolder(temporaryFolder, "junit");
    File tmp = FileUtils.createTempDirInLocation(baseDir.toPath(), null);
    Assertions.assertTrue(tmp.getName().startsWith("druid"));
    Assertions.assertEquals(
        baseDir.toPath(),
        tmp.getParentFile().toPath()
    );
  }

  @Test
  public void testCreateTempDirNonexistentBase()
  {
    final String oldJavaTmpDir = System.getProperty("java.io.tmpdir");
    final String nonExistentDir = oldJavaTmpDir + "nonexistent";

    try {
      System.setProperty("java.io.tmpdir", nonExistentDir);
      Throwable e = Assertions.assertThrows(IllegalStateException.class, () -> FileUtils.createTempDir());
      Assertions.assertEquals("Path [" + nonExistentDir + "] does not exist", e.getMessage());
    }
    finally {
      System.setProperty("java.io.tmpdir", oldJavaTmpDir);
    }
  }

  @Test
  public void testCreateTempDirUnwritableBase() throws IOException
  {
    final File baseDir = FileUtils.createTempDir();
    final String oldJavaTmpDir = System.getProperty("java.io.tmpdir");
    try {

      System.setProperty("java.io.tmpdir", baseDir.getPath());
      baseDir.setWritable(false);
      Throwable e = Assertions.assertThrows(IllegalStateException.class, () -> FileUtils.createTempDir());

      Assertions.assertEquals("Path [" + baseDir + "] is not writable, check permissions", e.getMessage());
    }
    finally {
      baseDir.setWritable(true);
      Files.delete(baseDir.toPath());
      System.setProperty("java.io.tmpdir", oldJavaTmpDir);
    }
  }

  @Test
  public void testMkdirp() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "junit");
    final File testDirectory = new File(tmpDir, "test");

    FileUtils.mkdirp(testDirectory);
    Assertions.assertTrue(testDirectory.isDirectory());

    FileUtils.mkdirp(testDirectory);
    Assertions.assertTrue(testDirectory.isDirectory());
  }

  @Test
  public void testMkdirpCannotCreateOverExistingFile() throws IOException
  {
    final File tmpFile = File.createTempFile("junit", null, temporaryFolder);

    Throwable t = Assertions.assertThrows(IOException.class, () -> FileUtils.mkdirp(tmpFile));
    assertThat(
        t,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Cannot create directory"))
    );
  }

  @Test
  public void testMkdirpCannotCreateInNonWritableDirectory() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "junit");
    final File testDirectory = new File(tmpDir, "test");
    tmpDir.setWritable(false);
    final IOException e = Assertions.assertThrows(IOException.class, () -> FileUtils.mkdirp(testDirectory));

    assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Cannot create directory"))
    );
    tmpDir.setWritable(true);

    // Now it should work.
    FileUtils.mkdirp(testDirectory);
    Assertions.assertTrue(testDirectory.isDirectory());
  }

  @Test
  public void testCopyLarge() throws IOException
  {
    final File dstDirectory = newFolder(temporaryFolder, "junit");
    final File dstFile = new File(dstDirectory, "dst");
    final String data = "test data to write";

    final long result = FileUtils.copyLarge(
        () -> new ByteArrayInputStream(StringUtils.toUtf8(data)),
        dstFile,
        new byte[1024],
        Predicates.alwaysFalse(),
        3,
        null
    );

    Assertions.assertEquals(data.length(), result);
    Assertions.assertEquals(data, StringUtils.fromUtf8(Files.readAllBytes(dstFile.toPath())));
  }

  @Test
  public void testLinkOrCopy1() throws IOException
  {
    // Will be a LINK.

    final File fromFile = File.createTempFile("junit", null, temporaryFolder);
    final File toDir = newFolder(temporaryFolder, "junit");
    final File toFile = new File(toDir, "toFile");

    Files.write(fromFile.toPath(), StringUtils.toUtf8("foo"));
    final FileUtils.LinkOrCopyResult linkOrCopyResult = FileUtils.linkOrCopy(fromFile, toFile);

    // Verify the new link.
    Assertions.assertEquals(FileUtils.LinkOrCopyResult.LINK, linkOrCopyResult);
    Assertions.assertEquals("foo", StringUtils.fromUtf8(Files.readAllBytes(toFile.toPath())));

    // Verify they are actually the same file.
    Files.write(fromFile.toPath(), StringUtils.toUtf8("bar"));
    Assertions.assertEquals("bar", StringUtils.fromUtf8(Files.readAllBytes(toFile.toPath())));
  }

  @Test
  public void testLinkOrCopy2() throws IOException
  {
    // Will be a COPY, because the destination file already exists and therefore Files.createLink fails.

    final File fromFile = File.createTempFile("junit", null, temporaryFolder);
    final File toFile = File.createTempFile("junit", null, temporaryFolder);

    Files.write(fromFile.toPath(), StringUtils.toUtf8("foo"));
    final FileUtils.LinkOrCopyResult linkOrCopyResult = FileUtils.linkOrCopy(fromFile, toFile);

    // Verify the new link.
    Assertions.assertEquals(FileUtils.LinkOrCopyResult.COPY, linkOrCopyResult);
    Assertions.assertEquals("foo", StringUtils.fromUtf8(Files.readAllBytes(toFile.toPath())));

    // Verify they are not the same file.
    Files.write(fromFile.toPath(), StringUtils.toUtf8("bar"));
    Assertions.assertEquals("foo", StringUtils.fromUtf8(Files.readAllBytes(toFile.toPath())));
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
