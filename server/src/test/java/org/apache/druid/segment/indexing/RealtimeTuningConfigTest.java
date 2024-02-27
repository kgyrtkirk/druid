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

package org.apache.druid.segment.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.hamcrest.CoreMatchers;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;

public class RealtimeTuningConfigTest
{
  @Test
  public void testErrorMessageIsMeaningfulWhenUnableToCreateTemporaryDirectory()
  {
    String propertyName = "java.io.tmpdir";
    String originalValue = System.getProperty(propertyName);
    String nonExistedDirectory = "/tmp/" + UUID.randomUUID();
    try {
      System.setProperty(propertyName, nonExistedDirectory);
      RealtimeTuningConfig.makeDefaultTuningConfig(null);
    }
    catch (IllegalStateException e) {
      assertThat(
          e.getMessage(),
          CoreMatchers.startsWith("java.io.tmpdir (" + nonExistedDirectory + ") does not exist")
      );
    }
    finally {
      System.setProperty(propertyName, originalValue);
    }
  }

  @Test
  public void testSpecificBasePersistDirectory()
  {
    final RealtimeTuningConfig tuningConfig = RealtimeTuningConfig.makeDefaultTuningConfig(
        new File("/tmp/nonexistent")
    );
    Assertions.assertEquals(new File("/tmp/nonexistent"), tuningConfig.getBasePersistDirectory());
  }

  @Test
  public void testSerdeWithDefaults() throws Exception
  {
    String jsonStr = "{\"type\":\"realtime\"}";

    ObjectMapper mapper = TestHelper.makeJsonMapper();
    RealtimeTuningConfig config = (RealtimeTuningConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                TuningConfig.class
            )
        ),
        TuningConfig.class
    );

    Assertions.assertEquals(new OnheapIncrementalIndex.Spec(), config.getAppendableIndexSpec());
    Assertions.assertEquals(Duration.standardMinutes(15).getMillis(), config.getHandoffConditionTimeout());
    Assertions.assertEquals(0, config.getAlertTimeout());
    Assertions.assertEquals(IndexSpec.DEFAULT, config.getIndexSpec());
    Assertions.assertEquals(IndexSpec.DEFAULT, config.getIndexSpecForIntermediatePersists());
    Assertions.assertEquals(new Period("PT10M"), config.getIntermediatePersistPeriod());
    Assertions.assertEquals(new NumberedShardSpec(0, 1), config.getShardSpec());
    Assertions.assertEquals(0, config.getMaxPendingPersists());
    Assertions.assertEquals(150000, config.getMaxRowsInMemory());
    Assertions.assertEquals(0, config.getMergeThreadPriority());
    Assertions.assertEquals(0, config.getPersistThreadPriority());
    Assertions.assertEquals(new Period("PT10M"), config.getWindowPeriod());
    Assertions.assertFalse(config.isReportParseExceptions());
  }

  @Test
  public void testSerdeWithNonDefaults() throws Exception
  {
    String jsonStr = "{\n"
                     + "  \"type\": \"realtime\",\n"
                     + "  \"maxRowsInMemory\": 100,\n"
                     + "  \"intermediatePersistPeriod\": \"PT1H\",\n"
                     + "  \"windowPeriod\": \"PT1H\",\n"
                     + "  \"maxPendingPersists\": 100,\n"
                     + "  \"persistThreadPriority\": 100,\n"
                     + "  \"mergeThreadPriority\": 100,\n"
                     + "  \"reportParseExceptions\": true,\n"
                     + "  \"handoffConditionTimeout\": 100,\n"
                     + "  \"alertTimeout\": 70,\n"
                     + "  \"indexSpec\": { \"metricCompression\" : \"NONE\" },\n"
                     + "  \"indexSpecForIntermediatePersists\": { \"dimensionCompression\" : \"uncompressed\" },\n"
                     + "  \"appendableIndexSpec\": { \"type\" : \"onheap\" }\n"
                     + "}";

    ObjectMapper mapper = TestHelper.makeJsonMapper();
    RealtimeTuningConfig config = (RealtimeTuningConfig) mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(
                jsonStr,
                TuningConfig.class
            )
        ),
        TuningConfig.class
    );

    Assertions.assertEquals(new OnheapIncrementalIndex.Spec(), config.getAppendableIndexSpec());
    Assertions.assertEquals(100, config.getHandoffConditionTimeout());
    Assertions.assertEquals(70, config.getAlertTimeout());
    Assertions.assertEquals(new Period("PT1H"), config.getIntermediatePersistPeriod());
    Assertions.assertEquals(new NumberedShardSpec(0, 1), config.getShardSpec());
    Assertions.assertEquals(100, config.getMaxPendingPersists());
    Assertions.assertEquals(100, config.getMaxRowsInMemory());
    Assertions.assertEquals(100, config.getMergeThreadPriority());
    Assertions.assertEquals(100, config.getPersistThreadPriority());
    Assertions.assertEquals(new Period("PT1H"), config.getWindowPeriod());
    Assertions.assertEquals(true, config.isReportParseExceptions());
    Assertions.assertEquals(
        IndexSpec.builder().withMetricCompression(CompressionStrategy.NONE).build(),
        config.getIndexSpec()
    );
    Assertions.assertEquals(
        IndexSpec.builder().withDimensionCompression(CompressionStrategy.UNCOMPRESSED).build(),
        config.getIndexSpecForIntermediatePersists()
    );

  }
}
