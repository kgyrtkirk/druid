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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 */
public class SegmentChangeRequestLoadTest
{
  @Test
  public void testV1Serialization() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    final Interval interval = Intervals.of("2011-10-01/2011-10-02");
    final ImmutableMap<String, Object> loadSpec = ImmutableMap.of("something", "or_other");

    DataSegment segment = new DataSegment(
        "something",
        interval,
        "1",
        loadSpec,
        Arrays.asList("dim1", "dim2"),
        Arrays.asList("met1", "met2"),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        1
    );

    final SegmentChangeRequestLoad segmentDrop = new SegmentChangeRequestLoad(segment);

    Map<String, Object> objectMap = mapper.readValue(
        mapper.writeValueAsString(segmentDrop), JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
    );

    Assert.assertEquals(11, objectMap.size());
    Assert.assertEquals("load", objectMap.get("action"));
    Assert.assertEquals("something", objectMap.get("dataSource"));
    Assert.assertEquals(interval.toString(), objectMap.get("interval"));
    Assert.assertEquals("1", objectMap.get("version"));
    Assert.assertEquals(loadSpec, objectMap.get("loadSpec"));
    Assert.assertEquals("dim1,dim2", objectMap.get("dimensions"));
    Assert.assertEquals("met1,met2", objectMap.get("metrics"));
    Assert.assertEquals(ImmutableMap.of("type", "none"), objectMap.get("shardSpec"));
    Assert.assertEquals(IndexIO.CURRENT_VERSION_ID, objectMap.get("binaryVersion"));
    Assert.assertEquals(1, objectMap.get("size"));
  }

  @Test
  public void testV1Serialization1() throws Exception
  {
    ObjectMapper mapper = new DefaultObjectMapper();

    final Interval interval = Intervals.of("2011-10-01/2011-10-02");
    final ImmutableMap<String, Object> loadSpec = ImmutableMap.of("something", "or_other");

    DataSegment segment = makeSegment(interval, loadSpec);

    SegmentChangeRequestLoad srl = new SegmentChangeRequestLoad(segment);

    String str = mapper.writeValueAsString(srl);
    SegmentChangeRequestLoad r = mapper.readValue(str, SegmentChangeRequestLoad.class);
    assertEquals(srl, r);
  }

  private DataSegment makeSegment(final Interval interval, final ImmutableMap<String, Object> loadSpec)
  {
    DataSegment segment = new DataSegment(
        "something",
        interval,
        "1",
        loadSpec,
        Arrays.asList("dim1", "dim2"),
        Arrays.asList("met1", "met2"),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        1
    );
    return segment;
  }

  @Test
  public void main() throws IOException, ParseException
  {
    ObjectMapper mapper = new ObjectMapper();
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd-MM-yyyy");
    Date date = simpleDateFormat.parse("20-12-1984");
    Student.Name1 name = new Student.Name1();
    name.first = "Jane";
    name.last = "Doe";
    Student student = new Student(name, 1);
    String jsonString = mapper
        .writerWithDefaultPrettyPrinter()
        .writeValueAsString(student);
    System.out.println(jsonString);

    mapper.readValue(jsonString, Student.class);
  }

  static class Student
  {
    public int id;
    @JsonUnwrapped
    public Name1 name;

    @JsonCreator
    Student(
        @JsonUnwrapped
        Name1 name,
        @JsonProperty("id")
        int id

        )
    {
      this.id = id;
      this.name = name;
    }

    public static class Name1
    {
      public String first;
      public String last;
    }
  }

}
