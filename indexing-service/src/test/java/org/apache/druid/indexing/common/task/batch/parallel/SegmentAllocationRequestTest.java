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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.junit.Assert;

import java.io.IOException;

public class SegmentAllocationRequestTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(SegmentAllocationRequest.class)
                  .usingGetClass()
                  .withNonnullFields("timestamp", "sequenceName")
                  .verify();
  }

  @Test
  public void testSerde() throws IOException
  {
    ObjectMapper objectMapper = new DefaultObjectMapper();
    SegmentAllocationRequest request = new SegmentAllocationRequest(
        DateTimes.nowUtc(),
        "sequenceName",
        "prevSegmentId"
    );
    byte[] json = objectMapper.writeValueAsBytes(request);
    SegmentAllocationRequest fromJson = objectMapper.readValue(json, SegmentAllocationRequest.class);
    Assert.assertEquals(request, fromJson);
  }
}
