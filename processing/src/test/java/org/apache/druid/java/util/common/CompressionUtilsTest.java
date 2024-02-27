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
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;
import com.google.common.io.Files;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream;
import org.apache.druid.utils.CompressionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class CompressionUtilsTest
{
  private static final String CONTENT;
  private static final byte[] EXPECTED;
  private static final byte[] GZ_BYTES;

  static {
    final StringBuilder builder = new StringBuilder();
    try (InputStream stream = CompressionUtilsTest.class.getClassLoader().getResourceAsStream("white-rabbit.txt")) {
      final Iterator<String> it = new Scanner(
          new InputStreamReader(stream, StandardCharsets.UTF_8)
      ).useDelimiter(Pattern.quote(System.lineSeparator()));
      while (it.hasNext()) {
        builder.append(it.next());
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    CONTENT = builder.toString();
    EXPECTED = StringUtils.toUtf8(CONTENT);

    final ByteArrayOutputStream gzByteStream = new ByteArrayOutputStream(EXPECTED.length);
    try (GZIPOutputStream outputStream = new GZIPOutputStream(gzByteStream)) {
      try (ByteArrayInputStream in = new ByteArrayInputStream(EXPECTED)) {
        ByteStreams.copy(in, outputStream);
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    GZ_BYTES = gzByteStream.toByteArray();
  }

  @TempDir
  public File temporaryFolder;
  private File testDir;
  private File testFile;

  public static void assertGoodDataStream(InputStream stream) throws IOException
  {
    try (final ByteArrayOutputStream bos = new ByteArrayOutputStream(EXPECTED.length)) {
      ByteStreams.copy(stream, bos);
      Assertions.assertArrayEquals(EXPECTED, bos.toByteArray());
    }
  }

  @BeforeEach
  public void setUp() throws IOException
  {
    testDir = newFolder(temporaryFolder, "testDir");
    testFile = new File(testDir, "test.dat");
    try (OutputStream outputStream = new FileOutputStream(testFile)) {
      outputStream.write(StringUtils.toUtf8(CONTENT));
    }
    Assertions.assertTrue(testFile.getParentFile().equals(testDir));
  }

  @Test
  public void testGoodGzNameResolution()
  {
    Assertions.assertEquals("foo", CompressionUtils.getGzBaseName("foo.gz"));
  }

  @Test
  public void testBadGzName()
  {
    assertThrows(IAE.class, () -> {
      CompressionUtils.getGzBaseName("foo");
    });
  }


  @Test
  public void testBadShortGzName()
  {
    assertThrows(IAE.class, () -> {
      CompressionUtils.getGzBaseName(".gz");
    });
  }

  @Test
  public void testGoodZipCompressUncompress() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testGoodZipCompressUncompress");
    final File zipFile = new File(tmpDir, "compressionUtilTest.zip");

    CompressionUtils.zip(testDir, zipFile);
    final File newDir = new File(tmpDir, "newDir");
    newDir.mkdir();
    final FileUtils.FileCopyResult result = CompressionUtils.unzip(zipFile, newDir);

    verifyUnzip(newDir, result, ImmutableMap.of(testFile.getName(), StringUtils.toUtf8(CONTENT)));
  }


  @Test
  public void testGoodZipCompressUncompressWithLocalCopy() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testGoodZipCompressUncompressWithLocalCopy");
    final File zipFile = new File(tmpDir, "testGoodZipCompressUncompressWithLocalCopy.zip");
    CompressionUtils.zip(testDir, zipFile);
    final File newDir = new File(tmpDir, "newDir");
    newDir.mkdir();

    final FileUtils.FileCopyResult result = CompressionUtils.unzip(
        new ByteSource()
        {
          @Override
          public InputStream openStream() throws IOException
          {
            return new FileInputStream(zipFile);
          }
        },
        newDir,
        FileUtils.IS_EXCEPTION,
        true
    );

    verifyUnzip(newDir, result, ImmutableMap.of(testFile.getName(), StringUtils.toUtf8(CONTENT)));
  }

  @Test
  public void testGoodGZCompressUncompressToFile() throws Exception
  {
    final File tmpDir = newFolder(temporaryFolder, "testGoodGZCompressUncompressToFile");
    final File gzFile = new File(tmpDir, testFile.getName() + ".gz");
    Assertions.assertFalse(gzFile.exists());
    CompressionUtils.gzip(testFile, gzFile);
    Assertions.assertTrue(gzFile.exists());
    try (final InputStream inputStream = new GZIPInputStream(new FileInputStream(gzFile))) {
      assertGoodDataStream(inputStream);
    }
    testFile.delete();
    Assertions.assertFalse(testFile.exists());
    CompressionUtils.gunzip(gzFile, testFile);
    Assertions.assertTrue(testFile.exists());
    try (final InputStream inputStream = new FileInputStream(testFile)) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testGoodZipStream() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testGoodZipStream");
    final File zipFile = new File(tmpDir, "compressionUtilTest.zip");
    CompressionUtils.zip(testDir, new FileOutputStream(zipFile));
    final File newDir = new File(tmpDir, "newDir");
    newDir.mkdir();
    final FileUtils.FileCopyResult result = CompressionUtils.unzip(new FileInputStream(zipFile), newDir);
    verifyUnzip(newDir, result, ImmutableMap.of(testFile.getName(), StringUtils.toUtf8(CONTENT)));
  }

  private Map<String, byte[]> writeZipWithManyFiles(final File zipFile) throws IOException
  {
    final File srcDir = newFolder(temporaryFolder, "junit");

    final Map<String, byte[]> expectedFiles = new HashMap<>();

    for (int i = 0; i < 100; i++) {
      final String filePath = "file" + i;

      try (final FileOutputStream out = new FileOutputStream(new File(srcDir, filePath))) {
        out.write(i);
        expectedFiles.put(filePath, new byte[]{(byte) i});
      }
    }

    CompressionUtils.zip(srcDir, new FileOutputStream(zipFile));

    return expectedFiles;
  }

  @Test
  public void testZipWithManyFiles() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testZipWithManyFilesStream");
    final File zipFile = new File(tmpDir, "compressionUtilTest.zip");

    final Map<String, byte[]> expectedFiles = writeZipWithManyFiles(zipFile);

    final File unzipDir = new File(tmpDir, "unzipDir");
    unzipDir.mkdir();

    final FileUtils.FileCopyResult result = CompressionUtils.unzip(zipFile, unzipDir);
    verifyUnzip(unzipDir, result, expectedFiles);
  }

  @Test
  public void testZipWithManyFilesStreamWithLocalCopy() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testZipWithManyFilesStream");
    final File zipFile = new File(tmpDir, "compressionUtilTest.zip");

    final Map<String, byte[]> expectedFiles = writeZipWithManyFiles(zipFile);

    final File unzipDir = new File(tmpDir, "unzipDir");
    unzipDir.mkdir();

    final FileUtils.FileCopyResult result = CompressionUtils.unzip(
        new ByteSource()
        {
          @Override
          public InputStream openStream() throws IOException
          {
            return new FileInputStream(zipFile);
          }
        },
        unzipDir,
        FileUtils.IS_EXCEPTION,
        true
    );
    verifyUnzip(unzipDir, result, expectedFiles);
  }

  @Test
  public void testZipWithManyFilesStream() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testZipWithManyFilesStream");
    final File zipFile = new File(tmpDir, "compressionUtilTest.zip");

    final Map<String, byte[]> expectedFiles = writeZipWithManyFiles(zipFile);

    final File unzipDir = new File(tmpDir, "unzipDir");
    unzipDir.mkdir();

    try (final CountingInputStream zipIn = new CountingInputStream(new FileInputStream(zipFile))) {
      final FileUtils.FileCopyResult result = CompressionUtils.unzip(zipIn, unzipDir);

      verifyUnzip(unzipDir, result, expectedFiles);

      // Check that all bytes were read from the stream
      Assertions.assertEquals(zipFile.length(), zipIn.getCount());
    }
  }

  @Test
  public void testGoodGzipByteSource() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testGoodGzipByteSource");
    final File gzFile = new File(tmpDir, testFile.getName() + ".gz");
    Assertions.assertFalse(gzFile.exists());
    CompressionUtils.gzip(Files.asByteSource(testFile), Files.asByteSink(gzFile), Predicates.alwaysTrue());
    Assertions.assertTrue(gzFile.exists());
    try (final InputStream inputStream = CompressionUtils.decompress(new FileInputStream(gzFile), gzFile.getName())) {
      assertGoodDataStream(inputStream);
    }
    if (!testFile.delete()) {
      throw new IOE("Unable to delete file [%s]", testFile.getAbsolutePath());
    }
    Assertions.assertFalse(testFile.exists());
    CompressionUtils.gunzip(Files.asByteSource(gzFile), testFile);
    Assertions.assertTrue(testFile.exists());
    try (final InputStream inputStream = new FileInputStream(testFile)) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testDecompressBzip2() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testDecompressBzip2");
    final File bzFile = new File(tmpDir, testFile.getName() + ".bz2");
    Assertions.assertFalse(bzFile.exists());
    try (final OutputStream out = new BZip2CompressorOutputStream(new FileOutputStream(bzFile))) {
      ByteStreams.copy(new FileInputStream(testFile), out);
    }
    try (final InputStream inputStream = CompressionUtils.decompress(new FileInputStream(bzFile), bzFile.getName())) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testDecompressXz() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testDecompressXz");
    final File xzFile = new File(tmpDir, testFile.getName() + ".xz");
    Assertions.assertFalse(xzFile.exists());
    try (final OutputStream out = new XZCompressorOutputStream(new FileOutputStream(xzFile))) {
      ByteStreams.copy(new FileInputStream(testFile), out);
    }
    try (final InputStream inputStream = CompressionUtils.decompress(new FileInputStream(xzFile), xzFile.getName())) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testDecompressSnappy() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testDecompressSnappy");
    final File snappyFile = new File(tmpDir, testFile.getName() + ".sz");
    Assertions.assertFalse(snappyFile.exists());
    try (final OutputStream out = new FramedSnappyCompressorOutputStream(new FileOutputStream(snappyFile))) {
      ByteStreams.copy(new FileInputStream(testFile), out);
    }
    try (final InputStream inputStream = CompressionUtils.decompress(new FileInputStream(snappyFile), snappyFile.getName())) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testDecompressZstd() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testDecompressZstd");
    final File zstdFile = new File(tmpDir, testFile.getName() + ".zst");
    Assertions.assertFalse(zstdFile.exists());
    try (final OutputStream out = new ZstdCompressorOutputStream(new FileOutputStream(zstdFile))) {
      ByteStreams.copy(new FileInputStream(testFile), out);
    }
    try (final InputStream inputStream = CompressionUtils.decompress(new FileInputStream(zstdFile), zstdFile.getName())) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testDecompressZip() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testDecompressZip");
    final File zipFile = new File(tmpDir, testFile.getName() + ".zip");
    Assertions.assertFalse(zipFile.exists());
    try (final ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zipFile))) {
      out.putNextEntry(new ZipEntry("cool.file"));
      ByteStreams.copy(new FileInputStream(testFile), out);
      out.closeEntry();
    }
    try (final InputStream inputStream = CompressionUtils.decompress(new FileInputStream(zipFile), zipFile.getName())) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testDecompressZipWithManyFiles() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testDecompressZip");
    final File zipFile = new File(tmpDir, testFile.getName() + ".zip");
    writeZipWithManyFiles(zipFile);

    try (final InputStream inputStream = CompressionUtils.decompress(new FileInputStream(zipFile), zipFile.getName())) {
      // Should read the first file, which contains a single null byte.
      Assertions.assertArrayEquals(new byte[]{0}, ByteStreams.toByteArray(inputStream));
    }
  }

  @Test
  public void testGoodGZStream() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testGoodGZStream");
    final File gzFile = new File(tmpDir, testFile.getName() + ".gz");
    Assertions.assertFalse(gzFile.exists());
    CompressionUtils.gzip(new FileInputStream(testFile), new FileOutputStream(gzFile));
    Assertions.assertTrue(gzFile.exists());
    try (final InputStream inputStream = new GZIPInputStream(new FileInputStream(gzFile))) {
      assertGoodDataStream(inputStream);
    }
    if (!testFile.delete()) {
      throw new IOE("Unable to delete file [%s]", testFile.getAbsolutePath());
    }
    Assertions.assertFalse(testFile.exists());
    CompressionUtils.gunzip(new FileInputStream(gzFile), testFile);
    Assertions.assertTrue(testFile.exists());
    try (final InputStream inputStream = new FileInputStream(testFile)) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testEvilZip() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testEvilZip");

    final File evilResult = new File("/tmp/evil.txt");
    java.nio.file.Files.deleteIfExists(evilResult.toPath());

    File evilZip = new File(tmpDir, "evil.zip");
    java.nio.file.Files.deleteIfExists(evilZip.toPath());
    CompressionUtilsTest.makeEvilZip(evilZip);

    try {
      CompressionUtils.unzip(evilZip, tmpDir);
    }
    catch (ISE ise) {
      Assertions.assertTrue(ise.getMessage().contains("does not start with outDir"));
      Assertions.assertFalse(evilResult.exists(), "Zip exploit triggered, /tmp/evil.txt was written.");
      return;
    }
    Assertions.fail("Exception was not thrown for malicious zip file");
  }

  @Test
  public void testEvilZipInputStream() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testEvilZip");

    final File evilResult = new File("/tmp/evil.txt");
    java.nio.file.Files.deleteIfExists(evilResult.toPath());

    File evilZip = new File(tmpDir, "evil.zip");
    java.nio.file.Files.deleteIfExists(evilZip.toPath());
    CompressionUtilsTest.makeEvilZip(evilZip);

    try {
      CompressionUtils.unzip(new FileInputStream(evilZip), tmpDir);
    }
    catch (ISE ise) {
      Assertions.assertTrue(ise.getMessage().contains("does not start with outDir"));
      Assertions.assertFalse(evilResult.exists(), "Zip exploit triggered, /tmp/evil.txt was written.");
      return;
    }
    Assertions.fail("Exception was not thrown for malicious zip file");
  }

  @Test
  public void testEvilZipInputStreamWithLocalCopy() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testEvilZip");

    final File evilResult = new File("/tmp/evil.txt");
    java.nio.file.Files.deleteIfExists(evilResult.toPath());

    File evilZip = new File(tmpDir, "evil.zip");
    java.nio.file.Files.deleteIfExists(evilZip.toPath());
    CompressionUtilsTest.makeEvilZip(evilZip);

    try {
      CompressionUtils.unzip(
          new ByteSource()
          {
            @Override
            public InputStream openStream() throws IOException
            {
              return new FileInputStream(evilZip);
            }
          },
          tmpDir,
          FileUtils.IS_EXCEPTION,
          true
      );
    }
    catch (ISE ise) {
      Assertions.assertTrue(ise.getMessage().contains("does not start with outDir"));
      Assertions.assertFalse(evilResult.exists(), "Zip exploit triggered, /tmp/evil.txt was written.");
      return;
    }
    Assertions.fail("Exception was not thrown for malicious zip file");
  }

  @Test
  // Sanity check to make sure the test class works as expected
  public void testZeroRemainingInputStream() throws IOException
  {
    try (OutputStream outputStream = new FileOutputStream(testFile)) {
      Assertions.assertEquals(
          GZ_BYTES.length,
          ByteStreams.copy(
              new ZeroRemainingInputStream(new ByteArrayInputStream(GZ_BYTES)),
              outputStream
          )
      );
      Assertions.assertEquals(
          GZ_BYTES.length,
          ByteStreams.copy(
              new ZeroRemainingInputStream(new ByteArrayInputStream(GZ_BYTES)),
              outputStream
          )
      );
      Assertions.assertEquals(
          GZ_BYTES.length,
          ByteStreams.copy(
              new ZeroRemainingInputStream(new ByteArrayInputStream(GZ_BYTES)),
              outputStream
          )
      );
    }
    Assertions.assertEquals(GZ_BYTES.length * 3, testFile.length());
    try (InputStream inputStream = new ZeroRemainingInputStream(new FileInputStream(testFile))) {
      for (int i = 0; i < 3; ++i) {
        final byte[] bytes = new byte[GZ_BYTES.length];
        Assertions.assertEquals(bytes.length, inputStream.read(bytes));
        Assertions.assertArrayEquals(
            GZ_BYTES,
            bytes,
            StringUtils.format("Failed on range %d", i)
        );
      }
    }
  }

  // If this ever passes, er... fails to fail... then the bug is fixed
  @Test
  // http://bugs.java.com/bugdatabase/view_bug.do?bug_id=7036144
  public void testGunzipBug() throws IOException
  {
    assertThrows(AssertionError.class, () -> {
      final ByteArrayOutputStream tripleGzByteStream = new ByteArrayOutputStream(GZ_BYTES.length * 3);
      tripleGzByteStream.write(GZ_BYTES);
      tripleGzByteStream.write(GZ_BYTES);
      tripleGzByteStream.write(GZ_BYTES);
      try (final InputStream inputStream = new GZIPInputStream(
          new ZeroRemainingInputStream(
              new ByteArrayInputStream(
                  tripleGzByteStream.toByteArray()
              )
          )
      )) {
        try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream(EXPECTED.length * 3)) {
          Assertions.assertEquals(
              EXPECTED.length * 3,
              ByteStreams.copy(inputStream, outputStream),
              "Read terminated too soon (bug 7036144)"
          );
          final byte[] found = outputStream.toByteArray();
          Assertions.assertEquals(EXPECTED.length * 3, found.length);
          Assertions.assertArrayEquals(EXPECTED, Arrays.copyOfRange(found, EXPECTED.length * 0, EXPECTED.length * 1));
          Assertions.assertArrayEquals(EXPECTED, Arrays.copyOfRange(found, EXPECTED.length * 1, EXPECTED.length * 2));
          Assertions.assertArrayEquals(EXPECTED, Arrays.copyOfRange(found, EXPECTED.length * 2, EXPECTED.length * 3));
        }
      }
    });
  }

  @Test
  // http://bugs.java.com/bugdatabase/view_bug.do?bug_id=7036144
  public void testGunzipBugworkarround() throws IOException
  {
    testFile.delete();
    Assertions.assertFalse(testFile.exists());

    final ByteArrayOutputStream tripleGzByteStream = new ByteArrayOutputStream(GZ_BYTES.length * 3);
    tripleGzByteStream.write(GZ_BYTES);
    tripleGzByteStream.write(GZ_BYTES);
    tripleGzByteStream.write(GZ_BYTES);

    final ByteSource inputStreamFactory = new ByteSource()
    {
      @Override
      public InputStream openStream()
      {
        return new ZeroRemainingInputStream(new ByteArrayInputStream(tripleGzByteStream.toByteArray()));
      }
    };

    Assertions.assertEquals((long) (EXPECTED.length * 3), CompressionUtils.gunzip(inputStreamFactory, testFile).size());

    try (final InputStream inputStream = new FileInputStream(testFile)) {
      try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream(EXPECTED.length * 3)) {
        Assertions.assertEquals(
            EXPECTED.length * 3,
            ByteStreams.copy(inputStream, outputStream),
            "Read terminated too soon (7036144)"
        );
        final byte[] found = outputStream.toByteArray();
        Assertions.assertEquals(EXPECTED.length * 3, found.length);
        Assertions.assertArrayEquals(EXPECTED, Arrays.copyOfRange(found, EXPECTED.length * 0, EXPECTED.length * 1));
        Assertions.assertArrayEquals(EXPECTED, Arrays.copyOfRange(found, EXPECTED.length * 1, EXPECTED.length * 2));
        Assertions.assertArrayEquals(EXPECTED, Arrays.copyOfRange(found, EXPECTED.length * 2, EXPECTED.length * 3));
      }
    }
  }

  @Test
  // http://bugs.java.com/bugdatabase/view_bug.do?bug_id=7036144
  public void testGunzipBugStreamWorkarround() throws IOException
  {

    final ByteArrayOutputStream tripleGzByteStream = new ByteArrayOutputStream(GZ_BYTES.length * 3);
    tripleGzByteStream.write(GZ_BYTES);
    tripleGzByteStream.write(GZ_BYTES);
    tripleGzByteStream.write(GZ_BYTES);

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream(EXPECTED.length * 3)) {
      Assertions.assertEquals(
          EXPECTED.length * 3,
          CompressionUtils.gunzip(
              new ZeroRemainingInputStream(
                  new ByteArrayInputStream(tripleGzByteStream.toByteArray())
              ), bos
          )
      );
      final byte[] found = bos.toByteArray();
      Assertions.assertEquals(EXPECTED.length * 3, found.length);
      Assertions.assertArrayEquals(EXPECTED, Arrays.copyOfRange(found, EXPECTED.length * 0, EXPECTED.length * 1));
      Assertions.assertArrayEquals(EXPECTED, Arrays.copyOfRange(found, EXPECTED.length * 1, EXPECTED.length * 2));
      Assertions.assertArrayEquals(EXPECTED, Arrays.copyOfRange(found, EXPECTED.length * 2, EXPECTED.length * 3));
    }
  }

  @Test
  public void testZipName() throws IOException
  {
    final File tmpDir = newFolder(temporaryFolder, "testZipName");
    final File zipDir = new File(tmpDir, "zipDir");
    zipDir.mkdir();
    final File file = new File(tmpDir, "zipDir.zip");
    final Path unzipPath = Paths.get(zipDir.getPath(), "test.dat");
    file.delete();
    Assertions.assertFalse(file.exists());
    Assertions.assertFalse(unzipPath.toFile().exists());
    CompressionUtils.zip(testDir, file);
    Assertions.assertTrue(file.exists());
    CompressionUtils.unzip(file, zipDir);
    Assertions.assertTrue(unzipPath.toFile().exists());
    try (final FileInputStream inputStream = new FileInputStream(unzipPath.toFile())) {
      assertGoodDataStream(inputStream);
    }
  }

  @Test
  public void testNewFileDoesntCreateFile()
  {
    final File tmpFile = new File(testDir, "fofooofodshfudhfwdjkfwf.dat");
    Assertions.assertFalse(tmpFile.exists());
  }

  @Test
  public void testGoodGzipName()
  {
    Assertions.assertEquals("foo", CompressionUtils.getGzBaseName("foo.gz"));
  }

  @Test
  public void testGoodGzipNameWithPath()
  {
    Assertions.assertEquals("foo", CompressionUtils.getGzBaseName("/tar/ball/baz/bock/foo.gz"));
  }

  @Test
  public void testBadShortName()
  {
    assertThrows(IAE.class, () -> {
      CompressionUtils.getGzBaseName(".gz");
    });
  }

  @Test
  public void testBadName()
  {
    assertThrows(IAE.class, () -> {
      CompressionUtils.getGzBaseName("BANANAS");
    });
  }

  @Test
  public void testBadNameWithPath()
  {
    assertThrows(IAE.class, () -> {
      CompressionUtils.getGzBaseName("/foo/big/.gz");
    });
  }

  @Test
  public void testGoodGzipWithException() throws Exception
  {
    final AtomicLong flushes = new AtomicLong(0);
    final File tmpDir = newFolder(temporaryFolder, "testGoodGzipByteSource");
    final File gzFile = new File(tmpDir, testFile.getName() + ".gz");
    Assertions.assertFalse(gzFile.exists());
    CompressionUtils.gzip(
        Files.asByteSource(testFile), new ByteSink()
        {
          @Override
          public OutputStream openStream() throws IOException
          {
            return new FilterOutputStream(new FileOutputStream(gzFile))
            {
              @Override
              public void flush() throws IOException
              {
                if (flushes.getAndIncrement() > 0) {
                  super.flush();
                } else {
                  throw new IOException("Haven't flushed enough");
                }
              }
            };
          }
        }, Predicates.alwaysTrue()
    );
    Assertions.assertTrue(gzFile.exists());
    try (final InputStream inputStream = CompressionUtils.decompress(new FileInputStream(gzFile), "file.gz")) {
      assertGoodDataStream(inputStream);
    }
    if (!testFile.delete()) {
      throw new IOE("Unable to delete file [%s]", testFile.getAbsolutePath());
    }
    Assertions.assertFalse(testFile.exists());
    CompressionUtils.gunzip(Files.asByteSource(gzFile), testFile);
    Assertions.assertTrue(testFile.exists());
    try (final InputStream inputStream = new FileInputStream(testFile)) {
      assertGoodDataStream(inputStream);
    }
    Assertions.assertEquals(4, flushes.get()); // 2 for suppressed closes, 2 for manual calls to shake out errors
  }

  @Test
  public void testStreamErrorGzip() throws Exception
  {
    assertThrows(IOException.class, () -> {
      final File tmpDir = newFolder(temporaryFolder, "testGoodGzipByteSource");
      final File gzFile = new File(tmpDir, testFile.getName() + ".gz");
      Assertions.assertFalse(gzFile.exists());
      final AtomicLong flushes = new AtomicLong(0L);
      CompressionUtils.gzip(
          new FileInputStream(testFile), new FileOutputStream(gzFile)
      {
        @Override
        public void flush() throws IOException
        {
          if (flushes.getAndIncrement() > 0) {
            super.flush();
          } else {
            throw new IOException("Test exception");
          }
        }
      }
      );
    });
  }

  @Test
  public void testStreamErrorGunzip() throws Exception
  {
    assertThrows(IOException.class, () -> {
      final File tmpDir = newFolder(temporaryFolder, "testGoodGzipByteSource");
      final File gzFile = new File(tmpDir, testFile.getName() + ".gz");
      Assertions.assertFalse(gzFile.exists());
      CompressionUtils.gzip(Files.asByteSource(testFile), Files.asByteSink(gzFile), Predicates.alwaysTrue());
      Assertions.assertTrue(gzFile.exists());
      try (final InputStream inputStream = CompressionUtils.decompress(new FileInputStream(gzFile), "file.gz")) {
        assertGoodDataStream(inputStream);
      }
      if (testFile.exists() && !testFile.delete()) {
        throw new RE("Unable to delete file [%s]", testFile.getAbsolutePath());
      }
      Assertions.assertFalse(testFile.exists());
      final AtomicLong flushes = new AtomicLong(0L);
      CompressionUtils.gunzip(
          new FileInputStream(gzFile), new FilterOutputStream(
          new FileOutputStream(testFile)
          {
            @Override
            public void flush() throws IOException
            {
              if (flushes.getAndIncrement() > 0) {
                super.flush();
              } else {
                throw new IOException("Test exception");
              }
            }
          }
      )
      );
    });
  }

  private void verifyUnzip(
      final File unzipDir,
      final FileUtils.FileCopyResult result,
      final Map<String, byte[]> expectedFiles
  ) throws IOException
  {
    final List<String> filePaths = expectedFiles.keySet().stream().sorted().collect(Collectors.toList());

    // Check the FileCopyResult
    Assertions.assertEquals(expectedFiles.values().stream().mapToLong(arr -> arr.length).sum(), result.size());
    Assertions.assertEquals(
        filePaths.stream().map(filePath -> new File(unzipDir, filePath)).collect(Collectors.toList()),
        result.getFiles().stream().sorted().collect(Collectors.toList())
    );

    // Check the actual file list
    Assertions.assertEquals(
        filePaths,
        Arrays.stream(unzipDir.listFiles()).map(File::getName).sorted().collect(Collectors.toList())
    );

    // Check actual file contents
    for (Map.Entry<String, byte[]> entry : expectedFiles.entrySet()) {
      try (final FileInputStream in = new FileInputStream(new File(unzipDir, entry.getKey()))) {
        final byte[] bytes = ByteStreams.toByteArray(in);
        Assertions.assertArrayEquals(entry.getValue(), bytes);
      }
    }
  }

  private static class ZeroRemainingInputStream extends FilterInputStream
  {
    private final AtomicInteger pos = new AtomicInteger(0);

    protected ZeroRemainingInputStream(InputStream in)
    {
      super(in);
    }

    @Override
    public synchronized void reset() throws IOException
    {
      super.reset();
      pos.set(0);
    }

    @Override
    public int read(byte[] b) throws IOException
    {
      final int len = Math.min(b.length, GZ_BYTES.length - pos.get() % GZ_BYTES.length);
      pos.addAndGet(len);
      return read(b, 0, len);
    }

    @Override
    public int read() throws IOException
    {
      pos.incrementAndGet();
      return super.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException
    {
      final int l = Math.min(len, GZ_BYTES.length - pos.get() % GZ_BYTES.length);
      pos.addAndGet(l);
      return super.read(b, off, l);
    }

    @Override
    public int available()
    {
      return 0;
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

  // Helper method for unit tests (for checking that we fixed https://snyk.io/research/zip-slip-vulnerability)
  public static void makeEvilZip(File outputFile) throws IOException
  {
    ZipOutputStream zipOutputStream = new ZipOutputStream(new FileOutputStream(outputFile));
    ZipEntry zipEntry = new ZipEntry("../../../../../../../../../../../../../../../tmp/evil.txt");
    zipOutputStream.putNextEntry(zipEntry);
    byte[] output = StringUtils.toUtf8("evil text");
    zipOutputStream.write(output);
    zipOutputStream.closeEntry();
    zipOutputStream.close();
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
