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

package org.apache.druid.segment.incremental;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.testing.junit.LoggerCaptureRule;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ParseExceptionHandlerTest
{

  @Rule
  public LoggerCaptureRule logger = new LoggerCaptureRule(ParseExceptionHandler.class);

  @Test
  public void testMetricWhenAllConfigurationsAreTurnedOff()
  {
    final ParseException parseException = new ParseException(null, "test");
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        false,
        Integer.MAX_VALUE,
        0
    );

    IntStream.range(0, 100).forEach(i -> {
      parseExceptionHandler.handle(parseException);
      Assertions.assertEquals(i + 1, rowIngestionMeters.getUnparseable());
    });
  }

  @Test
  public void testLogParseExceptions()
  {
    final ParseException parseException = new ParseException(null, "test");
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        true,
        Integer.MAX_VALUE,
        0
    );
    parseExceptionHandler.handle(parseException);

    List<LogEvent> logEvents = logger.getLogEvents();
    Assertions.assertEquals(1, logEvents.size());
    String logMessage = logEvents.get(0).getMessage().getFormattedMessage();
    Assertions.assertTrue(logMessage.contains("Encountered parse exception"));
  }

  @Test
  public void testGetSavedParseExceptionsReturnNullWhenMaxSavedParseExceptionsIsZero()
  {
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        new SimpleRowIngestionMeters(),
        false,
        Integer.MAX_VALUE,
        0
    );
    Assertions.assertNull(parseExceptionHandler.getSavedParseExceptionReports());
  }

  @Test
  public void testMaxAllowedParseExceptionsThrowExceptionWhenItHitsMax()
  {
    Throwable exception = assertThrows(RuntimeException.class, () -> {
      final ParseException parseException = new ParseException(null, "test");
      final int maxAllowedParseExceptions = 3;
      final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
      final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
          rowIngestionMeters,
          false,
          maxAllowedParseExceptions,
          0
      );

      IntStream.range(0, maxAllowedParseExceptions).forEach(i -> parseExceptionHandler.handle(parseException));
      Assertions.assertEquals(3, rowIngestionMeters.getUnparseable());
      try {
        parseExceptionHandler.handle(parseException);
      }
      catch (RuntimeException e) {
        Assertions.assertEquals(4, rowIngestionMeters.getUnparseable());
        throw e;
      }
    });
    assertTrue(exception.getMessage().contains("Max parse exceptions[3] exceeded"));
  }

  @Test
  public void testGetSavedParseExceptionsReturnMostRecentParseExceptions()
  {
    final int maxSavedParseExceptions = 3;
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        false,
        Integer.MAX_VALUE,
        maxSavedParseExceptions
    );
    Assertions.assertNotNull(parseExceptionHandler.getSavedParseExceptionReports());
    int exceptionCounter = 0;
    for (; exceptionCounter < maxSavedParseExceptions; exceptionCounter++) {
      parseExceptionHandler.handle(new ParseException(null, StringUtils.format("test %d", exceptionCounter)));
    }
    Assertions.assertEquals(3, rowIngestionMeters.getUnparseable());
    Assertions.assertEquals(maxSavedParseExceptions, parseExceptionHandler.getSavedParseExceptionReports().size());
    for (int i = 0; i < maxSavedParseExceptions; i++) {
      Assertions.assertEquals(
          StringUtils.format("test %d", i),
          parseExceptionHandler.getSavedParseExceptionReports().get(i).getDetails().get(0)
      );
    }
    for (; exceptionCounter < 5; exceptionCounter++) {
      parseExceptionHandler.handle(new ParseException(null, StringUtils.format("test %d", exceptionCounter)));
    }
    Assertions.assertEquals(5, rowIngestionMeters.getUnparseable());

    Assertions.assertEquals(maxSavedParseExceptions, parseExceptionHandler.getSavedParseExceptionReports().size());
    for (int i = 0; i < maxSavedParseExceptions; i++) {
      Assertions.assertEquals(
          StringUtils.format("test %d", i + 2),
          parseExceptionHandler.getSavedParseExceptionReports().get(i).getDetails().get(0)
      );
    }
  }

  @Test
  public void testParseExceptionReportEquals()
  {
    EqualsVerifier.forClass(ParseExceptionReport.class)
                  .withNonnullFields("errorType", "details", "timeOfExceptionMillis")
                  .usingGetClass()
                  .verify();
  }
}
