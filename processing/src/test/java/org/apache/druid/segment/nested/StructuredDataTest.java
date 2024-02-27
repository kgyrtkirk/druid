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

package org.apache.druid.segment.nested;

import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StructuredDataTest
{
  @Test
  public void testCompareTo()
  {
    StructuredData sd0 = new StructuredData(null);
    StructuredData sd1 = new StructuredData("hello");
    StructuredData sd2 = new StructuredData("world");
    StructuredData sd3 = new StructuredData(1L);
    StructuredData sd4 = new StructuredData(2L);
    StructuredData sd5 = new StructuredData(1.1);
    StructuredData sd6 = new StructuredData(3.3);
    StructuredData sd7 = new StructuredData(ImmutableMap.of("x", 1, "y", "hello"));
    StructuredData sd8 = new StructuredData(ImmutableMap.of("x", 1, "y", "hello"));
    StructuredData sd9 = new StructuredData(ImmutableMap.of("x", 12, "y", "world"));

    // equals
    Assertions.assertEquals(0, sd0.compareTo(new StructuredData(null)));
    Assertions.assertEquals(0, sd1.compareTo(new StructuredData("hello")));
    Assertions.assertEquals(0, sd3.compareTo(new StructuredData(1L)));
    Assertions.assertEquals(0, sd6.compareTo(new StructuredData(3.3)));
    Assertions.assertEquals(0, sd7.compareTo(sd8));
    Assertions.assertEquals(0, sd8.compareTo(sd7));

    // null comparison
    Assertions.assertEquals(-1, sd0.compareTo(sd1));
    Assertions.assertEquals(1, sd1.compareTo(sd0));

    // string comparison
    Assertions.assertTrue(0 > sd1.compareTo(sd2));
    Assertions.assertTrue(0 < sd2.compareTo(sd1));

    // long comparison
    Assertions.assertEquals(-1, sd3.compareTo(sd4));
    Assertions.assertEquals(1, sd4.compareTo(sd3));

    // double comparison
    Assertions.assertEquals(-1, sd5.compareTo(sd6));
    Assertions.assertEquals(1, sd6.compareTo(sd5));

    // number comparison
    Assertions.assertEquals(-1, sd3.compareTo(sd5));
    Assertions.assertEquals(1, sd5.compareTo(sd3));

    // object hash comparison
    Assertions.assertEquals(1, sd7.compareTo(sd9));
    Assertions.assertEquals(-1, sd9.compareTo(sd7));

    // test transitive
    Assertions.assertEquals(-1, sd1.compareTo(sd3));
    Assertions.assertEquals(1, sd3.compareTo(sd1));
    Assertions.assertEquals(-1, sd2.compareTo(sd3));
    Assertions.assertEquals(1, sd3.compareTo(sd2));

    Assertions.assertEquals(-1, sd1.compareTo(sd9));
    Assertions.assertEquals(1, sd9.compareTo(sd1));
    Assertions.assertEquals(-1, sd2.compareTo(sd9));
    Assertions.assertEquals(1, sd9.compareTo(sd2));

    Assertions.assertEquals(-1, sd3.compareTo(sd9));
    Assertions.assertEquals(1, sd9.compareTo(sd3));
    Assertions.assertEquals(-1, sd4.compareTo(sd9));
    Assertions.assertEquals(1, sd9.compareTo(sd4));
    Assertions.assertEquals(-1, sd5.compareTo(sd9));
    Assertions.assertEquals(1, sd9.compareTo(sd5));
    Assertions.assertEquals(-1, sd6.compareTo(sd9));
    Assertions.assertEquals(1, sd9.compareTo(sd6));


  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(StructuredData.class).withIgnoredFields("hashInitialized", "hashValue", "hash").usingGetClass().verify();
  }
}
