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

import com.google.common.collect.ImmutableList;
import org.apache.robux.client.DirectRobuxClient;
import org.apache.robux.client.RobuxServer;
import org.apache.robux.client.QueryableRobuxServer;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.common.Intervals;
import org.apache.robux.query.CloneQueryMode;
import org.apache.robux.query.Query;
import org.apache.robux.server.coordination.RobuxServerMetadata;
import org.apache.robux.server.coordination.ServerType;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class TierSelectorStrategyTest
{

  @Test
  public void testHighestPriorityTierSelectorStrategyRealtime()
  {
    DirectRobuxClient client = EasyMock.createMock(DirectRobuxClient.class);
    QueryableRobuxServer lowPriority = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.REALTIME, RobuxServer.DEFAULT_TIER, 0),
        client
    );
    QueryableRobuxServer highPriority = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.REALTIME, RobuxServer.DEFAULT_TIER, 1),
        client
    );

    testTierSelectorStrategy(
        new HighestPriorityTierSelectorStrategy(new ConnectionCountServerSelectorStrategy()),
        highPriority, lowPriority
    );
  }

  @Test
  public void testHighestPriorityTierSelectorStrategy()
  {
    DirectRobuxClient client = EasyMock.createMock(DirectRobuxClient.class);
    QueryableRobuxServer lowPriority = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, RobuxServer.DEFAULT_TIER, 0),
        client
    );
    QueryableRobuxServer highPriority = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, RobuxServer.DEFAULT_TIER, 1),
        client
    );

    testTierSelectorStrategy(
        new HighestPriorityTierSelectorStrategy(new ConnectionCountServerSelectorStrategy()),
        highPriority, lowPriority
    );
  }

  @Test
  public void testLowestPriorityTierSelectorStrategy()
  {
    DirectRobuxClient client = EasyMock.createMock(DirectRobuxClient.class);
    QueryableRobuxServer lowPriority = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, RobuxServer.DEFAULT_TIER, 0),
        client
    );
    QueryableRobuxServer highPriority = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, RobuxServer.DEFAULT_TIER, 1),
        client
    );

    testTierSelectorStrategy(
        new LowestPriorityTierSelectorStrategy(new ConnectionCountServerSelectorStrategy()),
        lowPriority, highPriority
    );
  }

  @Test
  public void testCustomPriorityTierSelectorStrategy()
  {
    DirectRobuxClient client = EasyMock.createMock(DirectRobuxClient.class);
    QueryableRobuxServer lowPriority = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, RobuxServer.DEFAULT_TIER, -1),
        client
    );
    QueryableRobuxServer mediumPriority = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, RobuxServer.DEFAULT_TIER, 0),
        client
    );
    QueryableRobuxServer highPriority = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, RobuxServer.DEFAULT_TIER, 1),
        client
    );

    testTierSelectorStrategy(
        new CustomTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new CustomTierSelectorStrategyConfig()
            {
              @Override
              public List<Integer> getPriorities()
              {
                return Arrays.asList(2, 0, -1, 1);
              }
            }
        ),
        mediumPriority, lowPriority, highPriority
    );
  }

  @Test
  public void testEmptyCustomPriorityTierSelectorStrategy()
  {
    DirectRobuxClient client = EasyMock.createMock(DirectRobuxClient.class);
    QueryableRobuxServer lowPriority = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, RobuxServer.DEFAULT_TIER, -1),
        client
    );
    QueryableRobuxServer mediumPriority = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, RobuxServer.DEFAULT_TIER, 0),
        client
    );
    QueryableRobuxServer highPriority = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, RobuxServer.DEFAULT_TIER, 1),
        client
    );

    testTierSelectorStrategy(
        new CustomTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new CustomTierSelectorStrategyConfig()
            {
              @Override
              public List<Integer> getPriorities()
              {
                return new ArrayList<>();
              }
            }
        ),
        highPriority, mediumPriority, lowPriority
    );
  }

  @Test
  public void testIncompleteCustomPriorityTierSelectorStrategy()
  {
    DirectRobuxClient client = EasyMock.createMock(DirectRobuxClient.class);
    QueryableRobuxServer p0 = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, RobuxServer.DEFAULT_TIER, -1),
        client
    );
    QueryableRobuxServer p1 = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, RobuxServer.DEFAULT_TIER, 0),
        client
    );
    QueryableRobuxServer p2 = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, RobuxServer.DEFAULT_TIER, 1),
        client
    );
    QueryableRobuxServer p3 = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, RobuxServer.DEFAULT_TIER, 2),
        client
    );
    QueryableRobuxServer p4 = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, RobuxServer.DEFAULT_TIER, 3),
        client
    );
    TierSelectorStrategy tierSelectorStrategy = new CustomTierSelectorStrategy(
        new ConnectionCountServerSelectorStrategy(),
        new CustomTierSelectorStrategyConfig()
        {
          @Override
          public List<Integer> getPriorities()
          {
            return Arrays.asList(2, 0, -1);
          }
        }
    );
    testTierSelectorStrategy(
        tierSelectorStrategy,
        p3, p1, p0, p4, p2
    );
  }

  private void testTierSelectorStrategy(
      TierSelectorStrategy tierSelectorStrategy,
      QueryableRobuxServer... expectedSelection
  )
  {
    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            Intervals.of("2013-01-01/2013-01-02"),
            DateTimes.of("2013-01-01").toString(),
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        tierSelectorStrategy,
        HistoricalFilter.IDENTITY_FILTER
    );

    List<QueryableRobuxServer> servers = new ArrayList<>(Arrays.asList(expectedSelection));

    List<RobuxServerMetadata> expectedCandidates = new ArrayList<>();
    for (QueryableRobuxServer server : servers) {
      expectedCandidates.add(server.getServer().getMetadata());
    }
    Collections.shuffle(servers);
    for (QueryableRobuxServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    Assert.assertEquals(expectedSelection[0], serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES));
    Assert.assertEquals(expectedSelection[0], serverSelector.pick(EasyMock.createMock(Query.class), CloneQueryMode.EXCLUDECLONES));
    Assert.assertEquals(expectedCandidates, serverSelector.getCandidates(-1, CloneQueryMode.EXCLUDECLONES));
    Assert.assertEquals(expectedCandidates.subList(0, 2), serverSelector.getCandidates(2, CloneQueryMode.EXCLUDECLONES));
  }

  @Test
  public void testServerSelectorStrategyDefaults()
  {
    DirectRobuxClient client = EasyMock.createMock(DirectRobuxClient.class);
    QueryableRobuxServer p0 = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, RobuxServer.DEFAULT_TIER, -1),
        client
    );
    Set<QueryableRobuxServer> servers = new HashSet<>();
    servers.add(p0);
    RandomServerSelectorStrategy strategy = new RandomServerSelectorStrategy();
    Assert.assertEquals(strategy.pick(servers, EasyMock.createMock(DataSegment.class)), p0);
    Assert.assertEquals(
        strategy.pick(
            EasyMock.createMock(Query.class),
            servers,
            EasyMock.createMock(DataSegment.class)
        ), p0
    );
    ServerSelectorStrategy defaultDeprecatedServerSelectorStrategy = new ServerSelectorStrategy()
    {
      @Override
      public <T> List<QueryableRobuxServer> pick(
          @Nullable Query<T> query, Set<QueryableRobuxServer> servers, DataSegment segment,
          int numServersToPick
      )
      {
        return strategy.pick(servers, segment, numServersToPick);
      }
    };
    Assert.assertEquals(
        defaultDeprecatedServerSelectorStrategy.pick(servers, EasyMock.createMock(DataSegment.class)),
        p0
    );
    Assert.assertEquals(
        defaultDeprecatedServerSelectorStrategy.pick(servers, EasyMock.createMock(DataSegment.class), 1)
                                               .get(0), p0
    );
  }

  /**
   * Tests the PreferredTierSelectorStrategy with various configurations and expected selections.
   * It verifies
   * 1. The preferred tier is respected when picking a server.
   * 2. When getting all servers, the preferred tier is ignored, and the returned list is sorted by priority.
   * 3. When getting a limited number of candidates, it returns the top N servers with the preferred tier first.
   */
  private void testPreferredTierSelectorStrategy(
      PreferredTierSelectorStrategy tierSelectorStrategy,
      QueryableRobuxServer... expectedSelection
  )
  {
    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            Intervals.of("2013-01-01/2013-01-02"),
            DateTimes.of("2013-01-01").toString(),
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        tierSelectorStrategy,
        HistoricalFilter.IDENTITY_FILTER
    );

    List<QueryableRobuxServer> servers = new ArrayList<>(Arrays.asList(expectedSelection));

    List<RobuxServerMetadata> expectedCandidates = new ArrayList<>();
    for (QueryableRobuxServer server : servers) {
      expectedCandidates.add(server.getServer().getMetadata());
    }
    for (QueryableRobuxServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    // Verify that the preferred tier is respected when picking a server
    Assert.assertEquals(expectedSelection[0], serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES));
    Assert.assertEquals(expectedSelection[0], serverSelector.pick(EasyMock.createMock(Query.class), CloneQueryMode.EXCLUDECLONES));

    // Verify that when getting all severs, the preferred tier is ignored, the returned list is sorted by priority
    List<RobuxServerMetadata> allServers = new ArrayList<>(expectedCandidates);
    allServers.sort((o1, o2) -> tierSelectorStrategy.getComparator().compare(o1.getPriority(), o2.getPriority()));
    // verify the priority only because values with same priority may return in different order
    Assert.assertEquals(
        allServers.stream().map(RobuxServerMetadata::getPriority).collect(Collectors.toList()),
        serverSelector.getCandidates(-1, CloneQueryMode.EXCLUDECLONES).stream().map(RobuxServerMetadata::getPriority).collect(Collectors.toList())
    );

    // Verify that when getting a limited number of candidates, returns the top N servers with preferred tier first
    Assert.assertEquals(expectedCandidates.subList(0, 2), serverSelector.getCandidates(2, CloneQueryMode.EXCLUDECLONES));
  }

  @Test
  public void testPreferredTierSelectorStrategyHighestPriority()
  {
    DirectRobuxClient client = EasyMock.createMock(DirectRobuxClient.class);

    // Two servers that have same tier and priority
    QueryableRobuxServer preferredTierLowPriority = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, "preferred", 0),
        client
    );
    QueryableRobuxServer preferredTierHighPriority = new QueryableRobuxServer(
        new RobuxServer("test2", "localhost", null, 0, ServerType.HISTORICAL, "preferred", 1),
        client
    );

    QueryableRobuxServer preferredTierHighPriority2 = new QueryableRobuxServer(
        new RobuxServer("test3", "localhost", null, 0, ServerType.HISTORICAL, "preferred", 1),
        client
    );

    QueryableRobuxServer nonPreferredTierHighestPriority = new QueryableRobuxServer(
        new RobuxServer("test4", "localhost", null, 0, ServerType.HISTORICAL, "non-preferred", 2),
        client
    );

    PreferredTierSelectorStrategy tierSelectorStrategy = new PreferredTierSelectorStrategy(
        // Use a customized strategy that return the 2nd server
        new ServerSelectorStrategy()
        {
          @Override
          public List<QueryableRobuxServer> pick(Set<QueryableRobuxServer> servers, DataSegment segment, int numServersToPick)
          {
            if (servers.size() <= numServersToPick) {
              return ImmutableList.copyOf(servers);
            }
            List<QueryableRobuxServer> list = new ArrayList<>(servers);
            if (numServersToPick == 1) {
              // return the server whose name is greater
              return list.stream()
                         .sorted((o1, o2) -> o1.getServer().getName().compareTo(o2.getServer().getName()))
                         .skip(1)
                         .limit(1)
                         .collect(Collectors.toList());
            } else {
              return list.stream().limit(numServersToPick).collect(Collectors.toList());
            }
          }
        },
        new PreferredTierSelectorStrategyConfig("preferred", "highest")
    );

    final ServerSelector serverSelector = new ServerSelector(
        new DataSegment(
            "test",
            Intervals.of("2013-01-01/2013-01-02"),
            DateTimes.of("2013-01-01").toString(),
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            NoneShardSpec.instance(),
            0,
            0L
        ),
        tierSelectorStrategy,
        HistoricalFilter.IDENTITY_FILTER
    );

    List<QueryableRobuxServer> servers = new ArrayList<>(Arrays.asList(
        preferredTierLowPriority,
        preferredTierHighPriority,
        preferredTierHighPriority2,
        nonPreferredTierHighestPriority
    ));

    List<RobuxServerMetadata> expectedCandidates = new ArrayList<>();
    for (QueryableRobuxServer server : servers) {
      expectedCandidates.add(server.getServer().getMetadata());
    }
    for (QueryableRobuxServer server : servers) {
      serverSelector.addServerAndUpdateSegment(server, serverSelector.getSegment());
    }

    // Verify that the 2nd server is selected
    Assert.assertEquals(preferredTierHighPriority2, serverSelector.pick(null, CloneQueryMode.EXCLUDECLONES));
    Assert.assertEquals(preferredTierHighPriority2, serverSelector.pick(EasyMock.createMock(Query.class), CloneQueryMode.EXCLUDECLONES));

    // Verify that when getting all severs, the preferred tier is ignored, the returned list is sorted by priority
    List<RobuxServerMetadata> allServers = new ArrayList<>(expectedCandidates);
    allServers.sort((o1, o2) -> tierSelectorStrategy.getComparator().compare(o1.getPriority(), o2.getPriority()));
    // verify the priority only because values with same priority may return in different order
    Assert.assertEquals(
        allServers.stream().map(RobuxServerMetadata::getPriority).collect(Collectors.toList()),
        serverSelector.getCandidates(-1, CloneQueryMode.EXCLUDECLONES).stream().map(RobuxServerMetadata::getPriority).collect(Collectors.toList())
    );

    // Verify that when getting 2 candidates, returns the top N servers with preferred tier first
    Assert.assertEquals(
        Arrays.asList(
            preferredTierHighPriority.getServer().getMetadata(),
            preferredTierHighPriority2.getServer().getMetadata()
        ),

        serverSelector.getCandidates(2, CloneQueryMode.EXCLUDECLONES)
                      .stream()
                      // sort the name to make sure the test is stable
                      .sorted((o1, o2) -> o1.getName().compareTo(o2.getName()))
                      .collect(Collectors.toList())
    );
  }

  @Test
  public void testPreferredTierSelectorStrategyLowestPriority()
  {
    DirectRobuxClient client = EasyMock.createMock(DirectRobuxClient.class);
    QueryableRobuxServer preferredTierLowPriority = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, "preferred", 0),
        client
    );
    QueryableRobuxServer preferredTierHighPriority = new QueryableRobuxServer(
        new RobuxServer("test2", "localhost", null, 0, ServerType.HISTORICAL, "preferred", 1),
        client
    );
    QueryableRobuxServer nonPreferredTierLowestPriority = new QueryableRobuxServer(
        new RobuxServer("test3", "localhost", null, 0, ServerType.HISTORICAL, "non-preferred", -1),
        client
    );

    testPreferredTierSelectorStrategy(
        new PreferredTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new PreferredTierSelectorStrategyConfig("preferred", "lowest")
        ),
        preferredTierLowPriority, preferredTierHighPriority, nonPreferredTierLowestPriority
    );
  }

  @Test
  public void testPreferredTierSelectorStrategyWithFallback()
  {
    DirectRobuxClient client = EasyMock.createMock(DirectRobuxClient.class);
    // Create only non-preferred tier servers with different priorities
    QueryableRobuxServer nonPreferredTierLowPriority = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, "non-preferred", 0),
        client
    );
    QueryableRobuxServer nonPreferredTierMediumPriority = new QueryableRobuxServer(
        new RobuxServer("test2", "localhost", null, 0, ServerType.HISTORICAL, "non-preferred", 1),
        client
    );
    QueryableRobuxServer nonPreferredTierHighPriority = new QueryableRobuxServer(
        new RobuxServer("test3", "localhost", null, 0, ServerType.HISTORICAL, "non-preferred", 2),
        client
    );

    // Since no preferred tier servers are available, it should fall back to other servers
    // based on highest priority
    testPreferredTierSelectorStrategy(
        new PreferredTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new PreferredTierSelectorStrategyConfig("preferred", "highest")
        ),
        nonPreferredTierHighPriority, nonPreferredTierMediumPriority, nonPreferredTierLowPriority
    );
  }

  @Test
  public void testPreferredTierSelectorStrategyMixedServers()
  {
    DirectRobuxClient client = EasyMock.createMock(DirectRobuxClient.class);
    QueryableRobuxServer preferredTierLowPriority = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, "preferred", 0),
        client
    );
    QueryableRobuxServer preferredTierHighPriority = new QueryableRobuxServer(
        new RobuxServer("test2", "localhost", null, 0, ServerType.HISTORICAL, "preferred", 1),
        client
    );
    QueryableRobuxServer anotherTierHighPriority = new QueryableRobuxServer(
        new RobuxServer("test3", "localhost", null, 0, ServerType.HISTORICAL, "tier1", 3),
        client
    );
    QueryableRobuxServer yetAnotherTierMediumPriority = new QueryableRobuxServer(
        new RobuxServer("test4", "localhost", null, 0, ServerType.HISTORICAL, "tier2", 2),
        client
    );

    // Should return preferred tier servers first, sorted by priority
    testPreferredTierSelectorStrategy(
        new PreferredTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            new PreferredTierSelectorStrategyConfig("preferred", "highest")
        ),
        preferredTierHighPriority, preferredTierLowPriority, anotherTierHighPriority, yetAnotherTierMediumPriority
    );
  }

  @Test
  public void testPreferredTierSelectorStrategyDefaultPriority()
  {
    DirectRobuxClient client = EasyMock.createMock(DirectRobuxClient.class);

    QueryableRobuxServer preferredTierLowPriority = new QueryableRobuxServer(
        new RobuxServer("test1", "localhost", null, 0, ServerType.HISTORICAL, "preferred", 0),
        client
    );
    QueryableRobuxServer preferredTierHighPriority = new QueryableRobuxServer(
        new RobuxServer("test2", "localhost", null, 0, ServerType.HISTORICAL, "preferred", 1),
        client
    );
    QueryableRobuxServer nonPreferredTierHighestPriority = new QueryableRobuxServer(
        new RobuxServer("test3", "localhost", null, 0, ServerType.HISTORICAL, "non-preferred", 2),
        client
    );

    testPreferredTierSelectorStrategy(
        new PreferredTierSelectorStrategy(
            new ConnectionCountServerSelectorStrategy(),
            // Using null for priority should default to highest priority
            new PreferredTierSelectorStrategyConfig("preferred", null)
        ),
        preferredTierHighPriority, preferredTierLowPriority, nonPreferredTierHighestPriority
    );
  }
}
