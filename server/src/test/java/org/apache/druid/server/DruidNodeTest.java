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

package org.apache.druid.server;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class DruidNodeTest
{
  private final ObjectMapper mapper;

  public DruidNodeTest()
  {
    mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(Integer.class, Integer.MAX_VALUE)
            .addValue(String.class, "DUMMY")
    );
  }

  @Test
  public void testDefaultsAndSanity()
  {
    final String service = "test/service";

    DruidNode node;

    node = new DruidNode(service, null, false, null, null, true, false);
    Assertions.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assertions.assertEquals(-1, node.getPlaintextPort());
    // Hosts which report only ipv6 will have getDefaultHost() report something like fe80::6e40:8ff:fe93:9230
    // but getHostAndPort() reports [fe80::6e40:8ff:fe93:9230]
    Assertions.assertEquals(HostAndPort.fromString(DruidNode.getDefaultHost()).toString(), node.getHostAndPort());

    node = new DruidNode(service, "2001:db8:85a3::8a2e:370:7334", false, -1, null, true, false);
    Assertions.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assertions.assertEquals(-1, node.getPlaintextPort());
    Assertions.assertEquals("[2001:db8:85a3::8a2e:370:7334]", node.getHostAndPort());

    node = new DruidNode(service, "abc:123", false, null, null, true, false);
    Assertions.assertEquals("abc", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals("abc:123", node.getHostAndPort());

    node = new DruidNode(service, "2001:db8:85a3::8a2e:370:7334", false, null, null, true, false);
    Assertions.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assertions.assertTrue(8080 <= node.getPlaintextPort());

    node = new DruidNode(service, "[2001:db8:85a3::8a2e:370:7334]", false, null, null, true, false);
    Assertions.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assertions.assertTrue(8080 <= node.getPlaintextPort());

    node = new DruidNode(service, "abc", false, null, null, true, false);
    Assertions.assertEquals("abc", node.getHost());
    Assertions.assertTrue(8080 <= node.getPlaintextPort());

    node = new DruidNode(service, "abc", false, 123, null, true, false);
    Assertions.assertEquals("abc", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals("abc:123", node.getHostAndPort());

    node = new DruidNode(service, "abc:123", false, 123, null, true, false);
    Assertions.assertEquals("abc", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals("abc:123", node.getHostAndPort());

    node = new DruidNode(service, "[2001:db8:85a3::8a2e:370:7334]:123", false, null, null, true, false);
    Assertions.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals("[2001:db8:85a3::8a2e:370:7334]:123", node.getHostAndPort());

    node = new DruidNode(service, "2001:db8:85a3::8a2e:370:7334", false, 123, null, true, false);
    Assertions.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals("[2001:db8:85a3::8a2e:370:7334]:123", node.getHostAndPort());

    node = new DruidNode(service, "[2001:db8:85a3::8a2e:370:7334]", false, 123, null, true, false);
    Assertions.assertEquals("2001:db8:85a3::8a2e:370:7334", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals("[2001:db8:85a3::8a2e:370:7334]:123", node.getHostAndPort());

    node = new DruidNode(service, null, false, 123, null, true, false);
    Assertions.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());

    node = new DruidNode(service, null, false, 123, 123, true, false);
    Assertions.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(-1, node.getTlsPort());

    node = new DruidNode(service, "host", false, 123, 123, true, false);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(-1, node.getTlsPort());

    node = new DruidNode(service, "host:123", false, null, 123, true, false);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(-1, node.getTlsPort());

    node = new DruidNode("test", "host:123", false, null, 214, true, true);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(214, node.getTlsPort());

    node = new DruidNode("test", "host", false, 123, 214, true, true);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(214, node.getTlsPort());

    node = new DruidNode("test", "host:123", false, 123, 214, true, true);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(214, node.getTlsPort());

    node = new DruidNode("test", null, false, 123, 214, true, true);
    Assertions.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(214, node.getTlsPort());

    node = new DruidNode("test", "host:123", false, null, 214, false, true);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(-1, node.getPlaintextPort());
    Assertions.assertEquals(214, node.getTlsPort());

    node = new DruidNode("test", "host:123", false, null, 123, false, true);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(-1, node.getPlaintextPort());
    Assertions.assertEquals(123, node.getTlsPort());

    node = new DruidNode("test", null, false, null, 123, false, true);
    Assertions.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assertions.assertEquals(-1, node.getPlaintextPort());
    Assertions.assertEquals(123, node.getTlsPort());

    node = new DruidNode("test", null, false, -1, 123, false, true);
    Assertions.assertEquals(DruidNode.getDefaultHost(), node.getHost());
    Assertions.assertEquals(-1, node.getPlaintextPort());
    Assertions.assertEquals(123, node.getTlsPort());

    node = new DruidNode("test", "host", false, -1, 123, false, true);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(-1, node.getPlaintextPort());
    Assertions.assertEquals(123, node.getTlsPort());

    node = new DruidNode("test", "host", false, -1, 123, true, false);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(-1, node.getPlaintextPort());
    Assertions.assertEquals(-1, node.getTlsPort());

    node = new DruidNode("test", "host:123", false, 123, null, true, false);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(-1, node.getTlsPort());

    node = new DruidNode("test", "host:123", false, null, 123, true, false);
    Assertions.assertEquals("host", node.getHost());
    Assertions.assertEquals(123, node.getPlaintextPort());
    Assertions.assertEquals(-1, node.getTlsPort());
  }

  @Test
  public void testConflictingPorts()
  {
    assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test/service", "abc:123", false, 456, null, true, false);
    });
  }

  @Test
  public void testAtLeastTlsOrPlainTextIsSet()
  {
    assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test", "host:123", false, null, 123, false, false);
    });
  }

  @Test
  public void testSamePlainTextAndTlsPort()
  {
    assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test", "host:123", false, null, 123, true, true);
    });
  }

  @Test
  public void testSamePlainTextAndTlsPort1()
  {
    assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test", "host", false, 123, 123, true, true);
    });
  }

  @Test
  public void testNullTlsPort()
  {
    assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test", "host:123", false, null, null, true, true);
    });
  }

  @Test
  public void testNullPlainTextAndTlsPort1()
  {
    assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test", "host", false, null, null, true, true);
    });
  }

  @Test
  public void testNullTlsPort1()
  {
    assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test", "host:123", false, 123, null, true, true);
    });
  }

  @Test
  public void testNullPlainTextAndTlsPort()
  {
    assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test", null, false, null, null, true, true);
    });
  }

  @Test
  public void testConflictingPlainTextPort()
  {
    assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test", "host:123", false, 321, null, true, true);
    });
  }

  @Test
  public void testInvalidIPv6WithPort()
  {
    assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test/service", "[abc:fff]:123", false, 456, null, true, false);
    });
  }

  @Test
  public void testInvalidIPv6()
  {
    assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test/service", "abc:fff", false, 456, null, true, false);
    });
  }

  @Test
  public void testConflictingPortsNonsense()
  {
    assertThrows(IllegalArgumentException.class, () -> {
      new DruidNode("test/service", "[2001:db8:85a3::8a2e:370:7334]:123", false, 456, null, true, false);
    });
  }

  @Test
  public void testEquals()
  {
    final String serviceName = "serviceName";
    final String host = "some.host";
    final int port = 9898;
    Assertions.assertEquals(new DruidNode(serviceName, host, false, port, null, true, false), new DruidNode(serviceName, host, false, port, null, true, false));
    Assertions.assertNotEquals(new DruidNode(serviceName, host, false, port, null, true, false), new DruidNode(serviceName, host, false, -1, null, true, false));
    Assertions.assertNotEquals(new DruidNode(serviceName, host, false, port, null, true, false), new DruidNode(serviceName, "other.host", false, port, null, true, false));
    Assertions.assertNotEquals(new DruidNode(serviceName, host, false, port, null, true, false), new DruidNode("otherServiceName", host, false, port, null, true, false));
  }

  @Test
  public void testHashCode()
  {

    final String serviceName = "serviceName";
    final String host = "some.host";
    final int port = 9898;
    Assertions.assertEquals(new DruidNode(serviceName, host, false, port, null, true, false).hashCode(), new DruidNode(serviceName, host, false, port, null, true, false).hashCode());
    // Potential hash collision if hashCode method ever changes
    Assertions.assertNotEquals(new DruidNode(serviceName, host, false, port, null, true, false).hashCode(), new DruidNode(serviceName, host, false, -1, null, true, false).hashCode());
    Assertions.assertNotEquals(new DruidNode(serviceName, host, false, port, null, true, false).hashCode(), new DruidNode(serviceName, "other.host", false, port, null, true, false).hashCode());
    Assertions.assertNotEquals(new DruidNode(serviceName, host, false, port, null, true, false).hashCode(), new DruidNode("otherServiceName", host, false, port, null, true, false).hashCode());
  }


  @Test
  public void testSerde1() throws Exception
  {
    DruidNode actual = mapper.readValue(
        mapper.writeValueAsString(new DruidNode("service", "host", true, 1234, null, 5678, true, true)),
        DruidNode.class
    );
    Assertions.assertEquals("service", actual.getServiceName());
    Assertions.assertEquals("host", actual.getHost());
    Assertions.assertTrue(actual.isBindOnHost());
    Assertions.assertTrue(actual.isEnablePlaintextPort());
    Assertions.assertTrue(actual.isEnableTlsPort());
    Assertions.assertEquals(1234, actual.getPlaintextPort());
    Assertions.assertEquals(5678, actual.getTlsPort());
  }

  @Test
  public void testSerde2() throws Exception
  {
    DruidNode actual = mapper.readValue(
        mapper.writeValueAsString(new DruidNode("service", "host", false, 1234, null, 5678, null, false)),
        DruidNode.class
    );
    Assertions.assertEquals("service", actual.getServiceName());
    Assertions.assertEquals("host", actual.getHost());
    Assertions.assertFalse(actual.isBindOnHost());
    Assertions.assertTrue(actual.isEnablePlaintextPort());
    Assertions.assertFalse(actual.isEnableTlsPort());
    Assertions.assertEquals(1234, actual.getPlaintextPort());
    Assertions.assertEquals(-1, actual.getTlsPort());
  }

  @Test
  public void testSerde3() throws Exception
  {
    DruidNode actual = mapper.readValue(
        mapper.writeValueAsString(new DruidNode("service", "host", true, 1234, null, 5678, false, true)),
        DruidNode.class
    );
    Assertions.assertEquals("service", actual.getServiceName());
    Assertions.assertEquals("host", actual.getHost());
    Assertions.assertTrue(actual.isBindOnHost());
    Assertions.assertFalse(actual.isEnablePlaintextPort());
    Assertions.assertTrue(actual.isEnableTlsPort());
    Assertions.assertEquals(-1, actual.getPlaintextPort());
    Assertions.assertEquals(5678, actual.getTlsPort());
  }

  @Test
  public void testDeserialization1() throws Exception
  {
    String json = "{\n"
                  + "  \"service\":\"service\",\n"
                  + "  \"host\":\"host\",\n"
                  + "  \"bindOnHost\":true,\n"
                  + "  \"plaintextPort\":1234,\n"
                  + "  \"tlsPort\":5678,\n"
                  + "  \"enablePlaintextPort\":true,\n"
                  + "  \"enableTlsPort\":true\n"
                  + "}\n";


    DruidNode actual = mapper.readValue(json, DruidNode.class);
    Assertions.assertEquals(new DruidNode("service", "host", true, 1234, null, 5678, true, true), actual);

    Assertions.assertEquals("https", actual.getServiceScheme());
    Assertions.assertEquals("host:1234", actual.getHostAndPort());
    Assertions.assertEquals("host:5678", actual.getHostAndTlsPort());
    Assertions.assertEquals("host:5678", actual.getHostAndPortToUse());
  }

  @Test
  public void testDeserialization2() throws Exception
  {
    String json = "{\n"
                  + "  \"service\":\"service\",\n"
                  + "  \"host\":\"host\",\n"
                  + "  \"plaintextPort\":1234,\n"
                  + "  \"tlsPort\":5678,\n"
                  + "  \"enablePlaintextPort\":true"
                  + "}\n";


    DruidNode actual = mapper.readValue(json, DruidNode.class);
    Assertions.assertEquals(new DruidNode("service", "host", false, 1234, null, 5678, true, false), actual);

    Assertions.assertEquals("http", actual.getServiceScheme());
    Assertions.assertEquals("host:1234", actual.getHostAndPort());
    Assertions.assertNull(actual.getHostAndTlsPort());
    Assertions.assertEquals("host:1234", actual.getHostAndPortToUse());
  }

  @Test
  public void testDeserialization3() throws Exception
  {
    String json = "{\n"
                  + "  \"service\":\"service\",\n"
                  + "  \"host\":\"host\",\n"
                  + "  \"plaintextPort\":1234,\n"
                  + "  \"tlsPort\":5678"
                  + "}\n";


    DruidNode actual = mapper.readValue(json, DruidNode.class);
    Assertions.assertEquals(new DruidNode("service", "host", false, 1234, null, 5678, null, false), actual);

    Assertions.assertEquals("http", actual.getServiceScheme());
    Assertions.assertEquals("host:1234", actual.getHostAndPort());
    Assertions.assertNull(actual.getHostAndTlsPort());
    Assertions.assertEquals("host:1234", actual.getHostAndPortToUse());
  }

  @Test
  public void testDeserialization4() throws Exception
  {
    String json = "{\n"
                  + "  \"service\":\"service\",\n"
                  + "  \"host\":\"host\",\n"
                  + "  \"port\":1234,\n"
                  + "  \"tlsPort\":5678"
                  + "}\n";


    DruidNode actual = mapper.readValue(json, DruidNode.class);
    Assertions.assertEquals(new DruidNode("service", "host", false, null, 1234, 5678, null, false), actual);

    Assertions.assertEquals("http", actual.getServiceScheme());
    Assertions.assertEquals("host:1234", actual.getHostAndPort());
    Assertions.assertNull(actual.getHostAndTlsPort());
    Assertions.assertEquals("host:1234", actual.getHostAndPortToUse());
  }

}
