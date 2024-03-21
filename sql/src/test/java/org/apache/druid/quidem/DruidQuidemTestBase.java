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
package org.apache.druid.quidem;

import net.hydromatic.quidem.CommandHandler;
import net.hydromatic.quidem.Quidem;
import org.apache.calcite.test.DiffTestCase;
import org.apache.calcite.util.Closer;
import org.apache.calcite.util.Util;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import static org.junit.jupiter.api.Assertions.fail;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DruidQuidemTestBase
{
  private static final String IQ_SUFFIX = ".iq";

  /** Creates a command handler. */
  protected CommandHandler createCommandHandler()
  {
    return Quidem.EMPTY_COMMAND_HANDLER;
  }

  @ParameterizedTest
  @MethodSource("getFileNames")
  public void test(String testFileName) throws Exception
  {
    File inFile = new File(getTestRoot(), testFileName);

    final File outFile = new File(inFile.getParentFile(), inFile.getName() + ".out");
    Util.discard(outFile.getParentFile().mkdirs());
    try (Reader reader = Util.reader(inFile);
        Writer writer = Util.printWriter(outFile);
        Closer closer = new Closer()) {
      final Quidem.Config config = Quidem.configBuilder()
          .withReader(reader)
          .withWriter(writer)
          .withConnectionFactory(new DruidQuidemConnectionFactory())
          .withCommandHandler(new DruidQuidemCommandHandler())
          .build();
      new Quidem(config).execute();
    }
    final String diff = DiffTestCase.diff(inFile, outFile);
    if (!diff.isEmpty()) {
      fail(
          "Files differ: " + outFile + " " + inFile + "\n"
              + diff
      );
    }
  }

  protected final List<String> getFileNames() throws IOException
  {
    List<String> ret = new ArrayList<String>();
    for (File f : getTestRoot().listFiles()) {
      if (f.isDirectory()) {
        continue;
      }
      if (!f.getName().endsWith(IQ_SUFFIX)) {
        continue;
      }
      ret.add(f.getName());
    }
    Collections.sort(ret);
    return ret;

  }

  protected abstract File getTestRoot();
}
