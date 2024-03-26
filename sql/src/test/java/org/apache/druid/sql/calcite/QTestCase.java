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

import com.google.common.io.CharSource;
import org.apache.druid.sql.calcite.QueryTestRunner.QueryRunStep;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class QTestCase
{

  private StringBuffer sb;
  private String testName;

  public QTestCase(String testName)
  {
    this.testName = testName;
    sb = new StringBuffer();

    println("!use druid");
    println("!set outputformat mysql");
  }

  public void println(String str)
  {
    sb.append(str);
    sb.append("\n");
  }

  public QueryRunStep tuRunner()
  {
    return new QueryRunStep(null)
    {
      @Override
      public void run()
      {

      }
    };
  }

  public void close()
  {
    String contents = sb.toString();


    CopyOption aa = StandardCopyOption.REPLACE_EXISTING;
    Path aa1 = new File(testName).toPath();
    try {
      Files.copy(
          CharSource.wrap(contents).asByteSource(StandardCharsets.UTF_8).openStream(),
          aa1
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
