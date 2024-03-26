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

package org.apache.druid.sql.calcite;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import org.apache.calcite.util.Util;
import org.apache.druid.quidem.DruidIQTestInfo;
import org.apache.druid.quidem.DruidQuidemTestBase;
import org.apache.druid.quidem.DruidQuidemTestBase.DruidQuidemRunner;
import org.apache.druid.sql.calcite.QueryTestRunner.QueryRunStep;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class QTestCase
{

  private StringBuffer sb;
  private String testName;
  private DruidIQTestInfo testInfo;
  private boolean overwrite=true;

  public QTestCase(DruidIQTestInfo testInfo)
  {
    this.testInfo = testInfo;
    sb = new StringBuffer();

    println("!use druid");
    println("!set outputformat mysql");
  }

  public void println(String str)
  {
    sb.append(str);
    sb.append("\n");
  }

  public QueryRunStep toRunner()
  {
    return new QueryRunStep(null)
    {

      @Override
      public void run()
      {
        if (overwrite) {
          writeCaseTo(testInfo.getIQFile());
        } else {
          isValidTestCaseFile(testInfo.getIQFile());
        }

        try {
          DruidQuidemRunner rr;
          rr = new DruidQuidemTestBase.DruidQuidemRunner(overwrite);
          rr.run(testInfo.getIQFile());
        }
        catch (Exception e) {
          throw new RuntimeException("Error running quidem test", e);
        }
      }
    };
  }

  protected void isValidTestCaseFile(File iqFile)
  {
    if(iqFile.exists()) {
      throw new IllegalStateException("testcase doesn't exists; run with (-Dquidem.overwrite) : "+iqFile);
    }
    try {
      String header= makeHeader();
      String testCaseFirstLine = Files.asCharSource(iqFile,Charsets.UTF_8).readFirstLine();
      if(!header.equals(testCaseFirstLine)) {
        throw new IllegalStateException(
            "backing quidem testcase doesn't match test - run with (-Dquidem.overwrite) : " + iqFile
        );
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String makeHeader()
  {
    HashCode hash = Hashing.crc32().hashBytes(sb.toString().getBytes());
    return String.format("# %s case-crc:%s", testInfo.testName, hash);

  }

  public void writeCaseTo(File file)
  {
    Util.discard(file.getParentFile().mkdirs());
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(makeHeader().getBytes(Charsets.UTF_8));
      fos.write('\n');
      fos.write(sb.toString().getBytes(Charsets.UTF_8));
    }
    catch (IOException e) {
      throw new RuntimeException("Error writing testcase to: " + file, e);
    }
  }

}
