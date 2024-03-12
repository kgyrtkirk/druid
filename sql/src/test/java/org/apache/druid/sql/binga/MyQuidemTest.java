/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.druid.sql.binga;

import com.google.common.io.PatternFilenameFilter;
import net.hydromatic.quidem.CommandHandler;
import net.hydromatic.quidem.Quidem;
import org.apache.calcite.test.DiffTestCase;
import org.apache.calcite.util.Closer;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.Util;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.FilenameFilter;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test that runs every Quidem file as a test.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class MyQuidemTest {

  private static final Pattern PATTERN = Pattern.compile("\\.iq$");

  protected static Collection<String> data(String first) {
    // inUrl = "file:/home/fred/calcite/core/target/test-classes/sql/agg.iq"
    final URL inUrl = MyQuidemTest.class.getResource("/" + n2u(first));
    final File firstFile = Sources.of(requireNonNull(inUrl, "inUrl")).file();
    final int commonPrefixLength = firstFile.getAbsolutePath().length() - first.length();
    final File dir = firstFile.getParentFile();
    final List<String> paths = new ArrayList<>();
    final FilenameFilter filter = new PatternFilenameFilter(".*\\.iq$");
    for (File f : Util.first(dir.listFiles(filter), new File[0])) {
      paths.add(f.getAbsolutePath().substring(commonPrefixLength));
    }
    return paths;
  }

  /** Returns a file, replacing one directory with another.
   *
   * <p>For example, {@code replaceDir("/abc/str/astro.txt", "str", "xyz")}
   * returns "{@code "/abc/xyz/astro.txt}".
   * Note that the file name "astro.txt" does not become "axyzo.txt".
   */
  private static File replaceDir(File file, String target, String replacement) {
    return new File(
        file.getAbsolutePath().replace(n2u('/' + target + '/'),
            n2u('/' + replacement + '/')));
  }

  /** Creates a command handler. */
  protected CommandHandler createCommandHandler() {
    return Quidem.EMPTY_COMMAND_HANDLER;
  }

  /** Creates a connection factory. */
  protected Quidem.ConnectionFactory createConnectionFactory() {
    return new MyConnectionFactory();
  }

  /** Converts a path from native to Unix. On Windows, converts
   * back-slashes to forward-slashes; on Linux, does nothing. */
  private static String n2u(String s) {
    return File.separatorChar == '\\'
        ? s.replace('\\', '/')
        : s;
  }

  @ParameterizedTest
  @MethodSource("getPath")
  public void test(String path) throws Exception {
    final File inFile;
    final File outFile;
    final File f = new File(path);
    if (f.isAbsolute()) {
      // e.g. path = "/tmp/foo.iq"
      inFile = f;
      outFile = new File(path + ".out");
    } else {
      // e.g. path = "sql/agg.iq"
      // inUrl = "file:/home/fred/calcite/core/build/resources/test/sql/agg.iq"
      // inFile = "/home/fred/calcite/core/build/resources/test/sql/agg.iq"
      // outDir = "/home/fred/calcite/core/build/quidem/test/sql"
      // outFile = "/home/fred/calcite/core/build/quidem/test/sql/agg.iq"
      final URL inUrl = MyQuidemTest.class.getResource("/" + n2u(path));
      inFile = Sources.of(requireNonNull(inUrl, "inUrl")).file();
      outFile = replaceDir(inFile, "resources", "quidem");
    }
    Util.discard(outFile.getParentFile().mkdirs());
    try (Reader reader = Util.reader(inFile);
         Writer writer = Util.printWriter(outFile);
         Closer closer = new Closer()) {
      final Quidem.Config config = Quidem.configBuilder()
          .withReader(reader)
          .withWriter(writer)
          .withConnectionFactory(createConnectionFactory())
          .withCommandHandler(createCommandHandler())
          .build();
      new Quidem(config).execute();
    }
    final String diff = DiffTestCase.diff(inFile, outFile);
    if (!diff.isEmpty()) {
      fail("Files differ: " + outFile + " " + inFile + "\n"
          + diff);
    }
  }

  /** Factory method for {@link MyQuidemTest#test(String)} parameters. */
  protected abstract Collection<String> getPath();
}
