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

package org.apache.druid.quidem;

import org.apache.curator.shaded.com.google.common.collect.Iterables;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class IQSplitter
{

  public static void main(String[] args) throws Exception
  {
    new IQSplitter().run(args);

  }

  public void run(String[] args) throws Exception
  {
    File in = ProjectPathUtils
        .getPathFromProjectRoot("quidem-ut/src/test/quidem/org.apache.druid.quidem.QTest/qaArray_sql.dart.iq");
    String id = "qaD";
    File outDir = new File(in.getParentFile(), id);
    outDir.mkdirs();

    String pattern = "# TESTCASE:";

    List<String> lines = Files.readAllLines(in.toPath());

    List<String> preamble = new ArrayList<String>();
    List<String> testCase = new ArrayList<String>();
    int idx = 0;

    for (String string : lines) {
      if (string.startsWith("#-----")) {
        continue;
      }
      if (string.startsWith(pattern)) {
        if (idx == 0) {
          preamble = new ArrayList<>(testCase);
        } else {

          extracted(id, outDir, preamble, testCase, idx);
        }
        testCase.clear();
        idx++;
      }
      testCase.add(string);
    }
    extracted(id, outDir, preamble, testCase, idx);
    System.out.println("idx" + idx);
  }

  private void extracted(String id, File outDir, List<String> preamble, List<String> testCase, int idx)
      throws IOException
  {
    String name = String.format("%s.%05d.iq", id, idx);
    File outFile = new File(outDir, name);
    Iterable<String> caseLines = Iterables.concat(preamble, testCase);
    Files.write(outFile.toPath(), caseLines);
  }
}
