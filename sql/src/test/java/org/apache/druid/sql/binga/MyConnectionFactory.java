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

package org.apache.druid.sql.binga;

import net.hydromatic.quidem.Quidem.ConnectionFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import java.sql.Connection;

public class MyConnectionFactory implements ConnectionFactory
{

  SqlTestFrameworkConfig.ClassRule  frameworkClassRule ;
  SqlTestFrameworkConfig.MethodRule  frameworkMethodRule;
  private SqlTestFramework frameWork;
  private BaseCalciteQueryTest testHost;

  public MyConnectionFactory()
  {
    CalciteTestBase.setupCalciteProperties();

    frameworkClassRule = new SqlTestFrameworkConfig.ClassRule();
    testHost = new BaseCalciteQueryTest();
    frameworkMethodRule = frameworkClassRule.methodRule(testHost);

    frameWork = frameworkMethodRule.get(frameworkMethodRule.defaultConfig());

  }

  @Override
  public Connection connect(String name, boolean reference) throws Exception
  {
    if (name.equals("druid")) {
      return new MyConnection(testHost,frameWork);
    }
    return null;
  }

}
