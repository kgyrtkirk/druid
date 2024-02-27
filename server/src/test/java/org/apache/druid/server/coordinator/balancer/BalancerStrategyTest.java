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

package org.apache.druid.server.coordinator.balancer;

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class BalancerStrategyTest
{
  private BalancerStrategy balancerStrategy;
  private DataSegment proposedDataSegment;
  private List<ServerHolder> serverHolders;

  public static Iterable<Object[]> data()
  {
    return Arrays.asList(
        new Object[][]{
            {new CostBalancerStrategy(Execs.directExecutor())},
            {new RandomBalancerStrategy()}
        }
    );
  }

  public void initBalancerStrategyTest(BalancerStrategy balancerStrategy)
  {
    this.balancerStrategy = balancerStrategy;
  }

  @BeforeEach
  public void setUp()
  {
    this.proposedDataSegment = new DataSegment(
        "datasource1",
        Intervals.utc(0, 1),
        "",
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        11L
    );
  }


  @MethodSource("data")
  @ParameterizedTest(name = "{index}: BalancerStrategy:{0}")
  public void findNewSegmentHomeReplicatorNotEnoughSpace(BalancerStrategy balancerStrategy)
  {
    initBalancerStrategyTest(balancerStrategy);
    final ServerHolder serverHolder = new ServerHolder(
        new DruidServer("server1", "host1", null, 10L, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0).addDataSegment(proposedDataSegment).toImmutableDruidServer(),
        new TestLoadQueuePeon());
    Assertions.assertFalse(
        balancerStrategy.findServersToLoadSegment(
            proposedDataSegment,
            Collections.singletonList(serverHolder)
        ).hasNext()
    );
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}: BalancerStrategy:{0}")
  @Timeout(value = 5000L, unit = TimeUnit.MILLISECONDS)
  public void findNewSegmentHomeReplicatorNotEnoughNodesForReplication(BalancerStrategy balancerStrategy)
  {
    initBalancerStrategyTest(balancerStrategy);
    final ServerHolder serverHolder1 = new ServerHolder(
        new DruidServer("server1", "host1", null, 1000L, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0)
            .addDataSegment(proposedDataSegment).toImmutableDruidServer(),
        new TestLoadQueuePeon());

    final ServerHolder serverHolder2 = new ServerHolder(
        new DruidServer("server2", "host2", null, 1000L, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0)
            .addDataSegment(proposedDataSegment).toImmutableDruidServer(),
        new TestLoadQueuePeon());

    serverHolders = new ArrayList<>();
    serverHolders.add(serverHolder1);
    serverHolders.add(serverHolder2);

    // since there is not enough nodes to load 3 replicas of segment
    Assertions.assertFalse(balancerStrategy.findServersToLoadSegment(proposedDataSegment, serverHolders).hasNext());
  }

  @MethodSource("data")
  @ParameterizedTest(name = "{index}: BalancerStrategy:{0}")
  public void findNewSegmentHomeReplicatorEnoughSpace(BalancerStrategy balancerStrategy)
  {
    initBalancerStrategyTest(balancerStrategy);
    final ServerHolder serverHolder = new ServerHolder(
        new DruidServer("server1", "host1", null, 1000L, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0).toImmutableDruidServer(),
        new TestLoadQueuePeon());
    serverHolders = new ArrayList<>();
    serverHolders.add(serverHolder);
    final ServerHolder foundServerHolder = balancerStrategy
        .findServersToLoadSegment(proposedDataSegment, serverHolders).next();
    // since there is enough space on server it should be selected
    Assertions.assertEquals(serverHolder, foundServerHolder);
  }
}
