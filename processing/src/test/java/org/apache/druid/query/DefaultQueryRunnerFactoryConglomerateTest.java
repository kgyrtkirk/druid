///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package org.apache.druid.query;
//
//import com.google.common.collect.ImmutableMap;
//import org.apache.druid.java.util.common.DateTimes;
//import org.junit.Assert;
//import org.junit.Test;
//
//public class DefaultQueryRunnerFactoryConglomerateTest
//{
//
//  @Test
//  public void testCompareNullTimestamp()
//  {
//    DefaultQueryRunnerFactoryConglomerate a=new DefaultQueryRunnerFactoryConglomerate(
//        ImmutableMap.builder()
//        .put()
//
//        )
//    final Result<Object> nullTimestamp = new Result<>(null, null);
//    final Result<Object> nullTimestamp2 = new Result<>(null, null);
//    final Result<Object> nonNullTimestamp = new Result<>(DateTimes.nowUtc(), null);
//
//    Assert.assertEquals(0, nullTimestamp.compareTo(nullTimestamp2));
//    Assert.assertEquals(1, nullTimestamp.compareTo(nonNullTimestamp));
//  }
//}
