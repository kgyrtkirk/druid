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
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

public class Qui2 extends MyQuidemTest
{

  @Override
  protected Collection<String> getPath()
  {
    ArrayList<String> ret = new ArrayList<>();
    ret.add(new File("qui/a.iq").getAbsolutePath());
    return ret;// data(new File("qui/a.iq").getAbsolutePath());
  }

  @Override
  protected ConnectionFactory createConnectionFactory()
  {
    return new MyConnectionFactory();
//    return super.createConnectionFactory();
  }

  public static void main(String[] args) throws Exception
  {
    for (String arg : args) {
      new Qui2().test(arg);
    }
  }

}
