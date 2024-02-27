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

package org.apache.druid.server.coordinator.rules;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.DruidServer;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;

public class ForeverLoadRuleTest
{
  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();

  @Test
  public void testSerde() throws Exception
  {
    ForeverLoadRule rule = new ForeverLoadRule(null, null);

    ObjectMapper jsonMapper = new DefaultObjectMapper();
    Rule reread = jsonMapper.readValue(jsonMapper.writeValueAsString(rule), Rule.class);

    Assertions.assertEquals(rule.getTieredReplicants(), ((ForeverLoadRule) reread).getTieredReplicants());
    Assertions.assertEquals(ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS), rule.getTieredReplicants());
  }

  @Test
  public void testCreatingNegativeTieredReplicants()
  {
    assertThat(
        Assertions.assertThrows(DruidException.class, () ->
            new ForeverLoadRule(
                ImmutableMap.of(DruidServer.DEFAULT_TIER, -1),
                null
            )
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains(
            "Invalid number of replicas for tier [_default_tier]. Value [-1] must be positive."
        )
    );
  }

  @Test
  public void testEmptyTieredReplicants() throws Exception
  {
    ForeverLoadRule rule = new ForeverLoadRule(ImmutableMap.of(), false);

    LoadRule reread = (LoadRule) OBJECT_MAPPER.readValue(OBJECT_MAPPER.writeValueAsString(rule), Rule.class);
    Assertions.assertEquals(ImmutableMap.of(), reread.getTieredReplicants());
  }

  @Test
  public void testNullReplicantValue()
  {
    // Immutable map does not allow null values
    Map<String, Integer> tieredReplicants = new HashMap<>();
    tieredReplicants.put("tier", null);

    assertThat(
        Assertions.assertThrows(DruidException.class, () ->
            new ForeverLoadRule(
                tieredReplicants,
                true
            )
        ),
        DruidExceptionMatcher.invalidInput().expectMessageContains(
            "Invalid number of replicas for tier [tier]. Value must not be null."
        )
    );
  }

  @Test
  public void testShouldCreateDefaultTier() throws Exception
  {
    String inputJson = "    {\n"
                       + "     \"type\": \"loadForever\"\n"
                       + "  }";
    ForeverLoadRule inputForeverLoadRule = OBJECT_MAPPER.readValue(inputJson, ForeverLoadRule.class);
    Assertions.assertEquals(ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS), inputForeverLoadRule.getTieredReplicants());
  }

  @Test
  public void testUseDefaultTierAsTrueShouldCreateDefaultTier() throws Exception
  {
    String inputJson = "    {\n"
                       + "     \"type\": \"loadForever\"\n,"
                       + "     \"useDefaultTierForNull\": \"true\"\n"
                       + "  }";
    ForeverLoadRule inputForeverLoadRule = OBJECT_MAPPER.readValue(inputJson, ForeverLoadRule.class);
    Assertions.assertEquals(ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS), inputForeverLoadRule.getTieredReplicants());
  }

  @Test
  public void testUseDefaultTierAsFalseShouldCreateEmptyMap() throws Exception
  {
    String inputJson = "    {\n"
                       + "     \"type\": \"loadForever\"\n,"
                       + "     \"useDefaultTierForNull\": \"false\"\n"
                       + "  }";
    ForeverLoadRule inputForeverLoadRule = OBJECT_MAPPER.readValue(inputJson, ForeverLoadRule.class);
    Assertions.assertEquals(ImmutableMap.of(), inputForeverLoadRule.getTieredReplicants());
  }
}
