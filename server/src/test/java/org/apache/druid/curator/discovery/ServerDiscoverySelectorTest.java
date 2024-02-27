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

package org.apache.druid.curator.discovery;

import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.apache.druid.client.selector.Server;
import org.apache.druid.java.util.common.StringUtils;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;

public class ServerDiscoverySelectorTest
{

  private ServiceProvider serviceProvider;
  private ServerDiscoverySelector serverDiscoverySelector;
  private ServiceInstance instance;
  private static final int PORT = 8080;
  private static final int SSL_PORT = 8280;
  private static final String ADDRESS = "localhost";

  @BeforeEach
  public void setUp()
  {
    serviceProvider = EasyMock.createMock(ServiceProvider.class);
    instance = EasyMock.createMock(ServiceInstance.class);
    serverDiscoverySelector = new ServerDiscoverySelector(serviceProvider, "test");
  }

  @Test
  public void testPick() throws Exception
  {
    EasyMock.expect(serviceProvider.getInstance()).andReturn(instance).anyTimes();
    EasyMock.expect(instance.getAddress()).andReturn(ADDRESS).anyTimes();
    EasyMock.expect(instance.getPort()).andReturn(PORT).anyTimes();
    EasyMock.expect(instance.getSslPort()).andReturn(-1).anyTimes();
    EasyMock.replay(instance, serviceProvider);
    Server server = serverDiscoverySelector.pick();
    Assertions.assertEquals(PORT, server.getPort());
    Assertions.assertEquals(ADDRESS, server.getAddress());
    Assertions.assertTrue(server.getHost().contains(Integer.toString(PORT)));
    Assertions.assertTrue(server.getHost().contains(ADDRESS));
    Assertions.assertEquals("http", server.getScheme());
    EasyMock.verify(instance, serviceProvider);
    final URI uri = new URI(
        server.getScheme(),
        null,
        server.getAddress(),
        server.getPort(),
        "/druid/indexer/v1/action",
        null,
        null
    );
    Assertions.assertEquals(PORT, uri.getPort());
    Assertions.assertEquals(ADDRESS, uri.getHost());
    Assertions.assertEquals("http", uri.getScheme());
  }

  @Test
  public void testPickWithNullSslPort() throws Exception
  {
    EasyMock.expect(serviceProvider.getInstance()).andReturn(instance).anyTimes();
    EasyMock.expect(instance.getAddress()).andReturn(ADDRESS).anyTimes();
    EasyMock.expect(instance.getPort()).andReturn(PORT).anyTimes();
    EasyMock.expect(instance.getSslPort()).andReturn(null).anyTimes();
    EasyMock.replay(instance, serviceProvider);
    Server server = serverDiscoverySelector.pick();
    Assertions.assertEquals(PORT, server.getPort());
    Assertions.assertEquals(ADDRESS, server.getAddress());
    Assertions.assertTrue(server.getHost().contains(Integer.toString(PORT)));
    Assertions.assertTrue(server.getHost().contains(ADDRESS));
    Assertions.assertEquals("http", server.getScheme());
    EasyMock.verify(instance, serviceProvider);
    final URI uri = new URI(
        server.getScheme(),
        null,
        server.getAddress(),
        server.getPort(),
        "/druid/indexer/v1/action",
        null,
        null
    );
    Assertions.assertEquals(PORT, uri.getPort());
    Assertions.assertEquals(ADDRESS, uri.getHost());
    Assertions.assertEquals("http", uri.getScheme());
  }

  @Test
  public void testPickWithSslPort() throws Exception
  {
    EasyMock.expect(serviceProvider.getInstance()).andReturn(instance).anyTimes();
    EasyMock.expect(instance.getAddress()).andReturn(ADDRESS).anyTimes();
    EasyMock.expect(instance.getPort()).andReturn(PORT).anyTimes();
    EasyMock.expect(instance.getSslPort()).andReturn(SSL_PORT).anyTimes();
    EasyMock.replay(instance, serviceProvider);
    Server server = serverDiscoverySelector.pick();
    Assertions.assertEquals(SSL_PORT, server.getPort());
    Assertions.assertEquals(ADDRESS, server.getAddress());
    Assertions.assertTrue(server.getHost().contains(Integer.toString(SSL_PORT)));
    Assertions.assertTrue(server.getHost().contains(ADDRESS));
    Assertions.assertEquals("https", server.getScheme());
    EasyMock.verify(instance, serviceProvider);
    final URI uri = new URI(
        server.getScheme(),
        null,
        server.getAddress(),
        server.getPort(),
        "/druid/indexer/v1/action",
        null,
        null
    );
    Assertions.assertEquals(SSL_PORT, uri.getPort());
    Assertions.assertEquals(ADDRESS, uri.getHost());
    Assertions.assertEquals("https", uri.getScheme());
  }

  @Test
  public void testPickIPv6() throws Exception
  {
    final String address = "2001:0db8:0000:0000:0000:ff00:0042:8329";
    EasyMock.expect(serviceProvider.getInstance()).andReturn(instance).anyTimes();
    EasyMock.expect(instance.getAddress()).andReturn(address).anyTimes();
    EasyMock.expect(instance.getPort()).andReturn(PORT).anyTimes();
    EasyMock.expect(instance.getSslPort()).andReturn(-1).anyTimes();
    EasyMock.replay(instance, serviceProvider);
    Server server = serverDiscoverySelector.pick();
    Assertions.assertEquals(PORT, server.getPort());
    Assertions.assertEquals(address, server.getAddress());
    Assertions.assertTrue(server.getHost().contains(Integer.toString(PORT)));
    Assertions.assertTrue(server.getHost().contains(address));
    Assertions.assertEquals("http", server.getScheme());
    EasyMock.verify(instance, serviceProvider);
    final URI uri = new URI(
        server.getScheme(),
        null,
        server.getAddress(),
        server.getPort(),
        "/druid/indexer/v1/action",
        null,
        null
    );
    Assertions.assertEquals(PORT, uri.getPort());
    Assertions.assertEquals(StringUtils.format("[%s]", address), uri.getHost());
    Assertions.assertEquals("http", uri.getScheme());
  }


  @Test
  public void testPickIPv6Bracket() throws Exception
  {
    final String address = "[2001:0db8:0000:0000:0000:ff00:0042:8329]";
    EasyMock.expect(serviceProvider.getInstance()).andReturn(instance).anyTimes();
    EasyMock.expect(instance.getAddress()).andReturn(address).anyTimes();
    EasyMock.expect(instance.getPort()).andReturn(PORT).anyTimes();
    EasyMock.expect(instance.getSslPort()).andReturn(-1).anyTimes();
    EasyMock.replay(instance, serviceProvider);
    Server server = serverDiscoverySelector.pick();
    Assertions.assertEquals(PORT, server.getPort());
    Assertions.assertEquals(address, server.getAddress());
    Assertions.assertTrue(server.getHost().contains(Integer.toString(PORT)));
    Assertions.assertTrue(server.getHost().contains(address));
    Assertions.assertEquals("http", server.getScheme());
    EasyMock.verify(instance, serviceProvider);
    final URI uri = new URI(
        server.getScheme(),
        null,
        server.getAddress(),
        server.getPort(),
        "/druid/indexer/v1/action",
        null,
        null
    );
    Assertions.assertEquals(PORT, uri.getPort());
    Assertions.assertEquals(address, uri.getHost());
    Assertions.assertEquals("http", uri.getScheme());
  }

  @Test
  public void testPickWithNullInstance() throws Exception
  {
    EasyMock.expect(serviceProvider.getInstance()).andReturn(null).anyTimes();
    EasyMock.replay(serviceProvider);
    Server server = serverDiscoverySelector.pick();
    Assertions.assertNull(server);
    EasyMock.verify(serviceProvider);
  }

  @Test
  public void testPickWithException() throws Exception
  {
    EasyMock.expect(serviceProvider.getInstance()).andThrow(new Exception()).anyTimes();
    EasyMock.replay(serviceProvider);
    Server server = serverDiscoverySelector.pick();
    Assertions.assertNull(server);
    EasyMock.verify(serviceProvider);
  }

  @Test
  public void testStart() throws Exception
  {
    serviceProvider.start();
    EasyMock.replay(serviceProvider);
    serverDiscoverySelector.start();
    EasyMock.verify(serviceProvider);
  }

  @Test
  public void testStop() throws IOException
  {
    serviceProvider.close();
    EasyMock.replay(serviceProvider);
    serverDiscoverySelector.stop();
    EasyMock.verify(serviceProvider);
  }
}
