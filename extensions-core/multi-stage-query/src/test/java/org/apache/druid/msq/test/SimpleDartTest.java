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

package org.apache.druid.msq.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.QueryTestBuilder;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig;
import org.junit.jupiter.api.Test;
import java.util.UUID;

@SqlTestFrameworkConfig.ComponentSupplier(DartComponentSupplier.class)
public class SimpleDartTest extends BaseCalciteQueryTest
{
  @Override
  protected QueryTestBuilder testBuilder()
  {
    return new QueryTestBuilder(new CalciteTestConfig(true))
        .queryContext(ImmutableMap.<String, Object>builder().put("asd", UUID.randomUUID().toString()).build())
        .skipVectorize(true)
        .verifyNativeQueries(new VerifyMSQSupportedNativeQueriesPredicate());
  }

  @Test
  public void testSelect1()
  {
    testBuilder()
        .sql("SELECT 1")
        .expectedResults(
            ImmutableList.of(new Object[] {1})
        )
        .run();
  }

  @Test
  public void testSelectFromFoo()
  {
    testBuilder()
        .sql("SELECT 2 from foo order by dim1")
        .expectedResults(
            ImmutableList.of(
                new Object[] {2},
                new Object[] {2}
            )
        )
        .run();
  }

}
