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
import java.io.Reader;
import java.io.Writer;
import java.util.Collection;
import static org.junit.jupiter.api.Assertions.fail;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DruidQuidemTestBase
{
  /** Creates a command handler. */
  protected CommandHandler createCommandHandler()
  {
    return Quidem.EMPTY_COMMAND_HANDLER;
  }

  @ParameterizedTest
  @MethodSource("getPath")
  public void test(File inFile) throws Exception
  {
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

  /** Factory method for {@link DruidQuidemTestBase#test(String)} parameters. */
  protected abstract Collection<String> getPath();
}
