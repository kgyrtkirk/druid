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

package org.apache.druid.segment.realtime.firehose;

import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.server.DruidNode;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class ServiceAnnouncingChatHandlerProviderTest extends EasyMockSupport
{
  private static class TestChatHandler implements ChatHandler
  {
  }

  private static final String TEST_SERVICE_NAME = "test-service-name";
  private static final String TEST_HOST = "test-host";
  private static final int TEST_PORT = 1234;

  private ServiceAnnouncingChatHandlerProvider chatHandlerProvider;

  @Mock
  private DruidNode node;

  @Mock
  private ServiceAnnouncer serviceAnnouncer;

  @BeforeEach
  public void setUp()
  {
    chatHandlerProvider = new ServiceAnnouncingChatHandlerProvider(node, serviceAnnouncer);
  }

  @Test
  public void testRegistrationDefault()
  {
    testRegistrationWithAnnounce(false);
  }

  @Test
  public void testRegistrationWithAnnounce()
  {
    testRegistrationWithAnnounce(true);
  }

  @Test
  public void testRegistrationWithoutAnnounce()
  {
    ChatHandler testChatHandler = new TestChatHandler();

    Assertions.assertFalse(chatHandlerProvider.get(TEST_SERVICE_NAME).isPresent(), "bad initial state");

    chatHandlerProvider.register(TEST_SERVICE_NAME, testChatHandler, false);
    Assertions.assertTrue(chatHandlerProvider.get(TEST_SERVICE_NAME).isPresent(), "chatHandler did not register");
    Assertions.assertEquals(testChatHandler, chatHandlerProvider.get(TEST_SERVICE_NAME).get());

    chatHandlerProvider.unregister(TEST_SERVICE_NAME);
    Assertions.assertFalse(chatHandlerProvider.get(TEST_SERVICE_NAME).isPresent(), "chatHandler did not deregister");
  }

  private void testRegistrationWithAnnounce(boolean useThreeArgConstructor)
  {
    ChatHandler testChatHandler = new TestChatHandler();
    Capture<DruidNode> captured = Capture.newInstance();

    EasyMock.expect(node.getHost()).andReturn(TEST_HOST);
    EasyMock.expect(node.isBindOnHost()).andReturn(false);
    EasyMock.expect(node.getPlaintextPort()).andReturn(TEST_PORT);
    EasyMock.expect(node.isEnablePlaintextPort()).andReturn(true);
    EasyMock.expect(node.isEnableTlsPort()).andReturn(false);
    EasyMock.expect(node.getTlsPort()).andReturn(-1);
    serviceAnnouncer.announce(EasyMock.capture(captured));
    replayAll();

    Assertions.assertFalse(chatHandlerProvider.get(TEST_SERVICE_NAME).isPresent(), "bad initial state");

    if (useThreeArgConstructor) {
      chatHandlerProvider.register(TEST_SERVICE_NAME, testChatHandler, true);
    } else {
      chatHandlerProvider.register(TEST_SERVICE_NAME, testChatHandler);
    }
    verifyAll();

    DruidNode param = captured.getValues().get(0);
    Assertions.assertEquals(TEST_SERVICE_NAME, param.getServiceName());
    Assertions.assertEquals(TEST_HOST, param.getHost());
    Assertions.assertEquals(TEST_PORT, param.getPlaintextPort());
    Assertions.assertEquals(-1, param.getTlsPort());
    Assertions.assertEquals(null, param.getHostAndTlsPort());
    Assertions.assertTrue(chatHandlerProvider.get(TEST_SERVICE_NAME).isPresent(), "chatHandler did not register");
    Assertions.assertEquals(testChatHandler, chatHandlerProvider.get(TEST_SERVICE_NAME).get());

    captured.reset();
    resetAll();
    EasyMock.expect(node.getHost()).andReturn(TEST_HOST);
    EasyMock.expect(node.isBindOnHost()).andReturn(false);
    EasyMock.expect(node.getPlaintextPort()).andReturn(TEST_PORT);
    EasyMock.expect(node.isEnablePlaintextPort()).andReturn(true);
    EasyMock.expect(node.getTlsPort()).andReturn(-1);
    EasyMock.expect(node.isEnableTlsPort()).andReturn(false);
    serviceAnnouncer.unannounce(EasyMock.capture(captured));
    replayAll();

    chatHandlerProvider.unregister(TEST_SERVICE_NAME);
    verifyAll();

    param = captured.getValues().get(0);
    Assertions.assertEquals(TEST_SERVICE_NAME, param.getServiceName());
    Assertions.assertEquals(TEST_HOST, param.getHost());
    Assertions.assertEquals(TEST_PORT, param.getPlaintextPort());
    Assertions.assertEquals(-1, param.getTlsPort());
    Assertions.assertEquals(null, param.getHostAndTlsPort());
    Assertions.assertFalse(chatHandlerProvider.get(TEST_SERVICE_NAME).isPresent(), "chatHandler did not deregister");
  }
}
