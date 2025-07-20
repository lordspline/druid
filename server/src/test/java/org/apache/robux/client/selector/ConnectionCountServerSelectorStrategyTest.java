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

package org.apache.robux.client.selector;

import org.apache.robux.client.DirectRobuxClient;
import org.apache.robux.client.RobuxServer;
import org.apache.robux.client.QueryableRobuxServer;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.common.Intervals;
import org.apache.robux.query.CloneQueryMode;
import org.apache.robux.server.coordination.ServerType;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.partition.NumberedShardSpec;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConnectionCountServerSelectorStrategyTest
{
  @Test
  public void testDifferentConnectionCount()
  {
    QueryableRobuxServer s1 = mockServer("test1", 2);
    QueryableRobuxServer s2 = mockServer("test2", 1);
    QueryableRobuxServer s3 = mockServer("test3", 4);
    ServerSelector serverSelector = initSelector(s1, s2, s3);

    for (int i = 0; i < 100; ++i) {
      Assert.assertEquals(s2, serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES));
    }
  }

  @Test
  public void testBalancerTieBreaking()
  {
    QueryableRobuxServer s1 = mockServer("test1", 100);
    QueryableRobuxServer s2 = mockServer("test2", 100);
    ServerSelector serverSelector = initSelector(s1, s2);

    Set<String> pickedServers = new HashSet<>();
    for (int i = 0; i < 100; ++i) {
      pickedServers.add(serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES).getServer().getName());
    }
    Assert.assertTrue(
        "Multiple servers should be selected when the number of connections is equal.",
        pickedServers.size() > 1
    );
  }

  private QueryableRobuxServer mockServer(String name, int openConnections)
  {
    DirectRobuxClient client = EasyMock.createMock(DirectRobuxClient.class);
    EasyMock.expect(client.getNumOpenConnections()).andReturn(openConnections).anyTimes();
    EasyMock.replay(client);
    return new QueryableRobuxServer(
        new RobuxServer(
            name,
            "localhost",
            null,
            0,
            ServerType.HISTORICAL,
            RobuxServer.DEFAULT_TIER,
            0
        ), client
    );
  }

  private ServerSelector initSelector(QueryableRobuxServer... servers)
  {
    TierSelectorStrategy strategy = new HighestPriorityTierSelectorStrategy(new ConnectionCountServerSelectorStrategy());
    ServerSelector selector = new ServerSelector(
        new DataSegment(
            "test",
            Intervals.of("2025-01-01/2025-01-02"),
            DateTimes.of("2025-01-01").toString(),
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            new NumberedShardSpec(0, 0),
            0,
            0L
        ), strategy,
        HistoricalFilter.IDENTITY_FILTER
    );
    List<QueryableRobuxServer> serverList = new ArrayList<>(Arrays.asList(servers));
    Collections.shuffle(serverList);
    for (QueryableRobuxServer server : serverList) {
      selector.addServerAndUpdateSegment(server, selector.getSegment());
    }
    return selector;
  }
}
