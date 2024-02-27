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

package org.apache.druid.security.pac4j;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;

public class OIDCConfigTest
{
  @Test
  public void testSerde() throws Exception
  {
    ObjectMapper jsonMapper = new ObjectMapper();

    String jsonStr = "{\n"
                     + "  \"clientID\": \"testid\",\n"
                     + "  \"clientSecret\": \"testsecret\",\n"
                     + "  \"discoveryURI\": \"testdiscoveryuri\",\n"
                     + "  \"scope\": \"testscope\"\n"
                     + "}\n";

    OIDCConfig conf = jsonMapper.readValue(
        jsonMapper.writeValueAsString(jsonMapper.readValue(jsonStr, OIDCConfig.class)),
        OIDCConfig.class
    );
    Assert.assertEquals("testid", conf.getClientID());
    Assert.assertEquals("testsecret", conf.getClientSecret().getPassword());
    Assert.assertEquals("testdiscoveryuri", conf.getDiscoveryURI());
    Assert.assertEquals("name", conf.getOidcClaim());
    Assert.assertEquals("testscope", conf.getScope());
  }

  @Test
  public void testSerdeWithoutDefaults() throws Exception
  {
    ObjectMapper jsonMapper = new ObjectMapper();

    String jsonStr = "{\n"
                     + "  \"clientID\": \"testid\",\n"
                     + "  \"clientSecret\": \"testsecret\",\n"
                     + "  \"discoveryURI\": \"testdiscoveryuri\",\n"
                     + "  \"oidcClaim\": \"email\",\n"
                     + "  \"scope\": \"testscope\"\n"
                     + "}\n";

    OIDCConfig conf = jsonMapper.readValue(
        jsonMapper.writeValueAsString(jsonMapper.readValue(jsonStr, OIDCConfig.class)),
        OIDCConfig.class
    );

    Assert.assertEquals("testid", conf.getClientID());
    Assert.assertEquals("testsecret", conf.getClientSecret().getPassword());
    Assert.assertEquals("testdiscoveryuri", conf.getDiscoveryURI());
    Assert.assertEquals("email", conf.getOidcClaim());
    Assert.assertEquals("testscope", conf.getScope());
  }
}
