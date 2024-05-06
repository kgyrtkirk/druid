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

import org.apache.druid.sql.calcite.DisableUnless.DisableUnlessRule;
import org.apache.druid.sql.calcite.NotYetSupported.NotYetSupportedProcessor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

@ExtendWith(NotYetSupportedProcessor.class)
public class DecoupledPlanningCalciteJoinQueryTest extends CalciteJoinQueryTest
{
  @RegisterExtension
  public DisableUnlessRule sqlCompatOnly = DisableUnless.SQL_COMPATIBLE;

  @RegisterExtension
  static DecoupledExtension decoupledExtension = new DecoupledExtension();

  @Override
  protected QueryTestBuilder testBuilder()
  {
    return decoupledExtension.testBuilder();
  }

  @Test
  public void validateTestClass()
  {
    // technical testcase needed by the extension temporarily
  }

  @MethodSource("provideQueryContexts")
  @ParameterizedTest(name = "{0}")
  @DecoupledTestConfig
  public void ensureDecoupledTestConfigAnnotationWorks(Map<String, Object> queryContext)
  {
    assertNotNull(queryFrameworkRule.getAnnotation(DecoupledTestConfig.class));
    assertNotNull(queryContext);
  }
}
