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

package org.apache.robux.server.lookup.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.discovery.RobuxNodeDiscovery;
import org.apache.robux.discovery.RobuxNodeDiscoveryProvider;
import org.apache.robux.discovery.LookupNodeService;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.http.HostAndPortWithScheme;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class LookupNodeDiscoveryTest
{
  private RobuxNodeDiscoveryProvider robuxNodeDiscoveryProvider;
  private RobuxNodeDiscovery robuxNodeDiscovery;
  private LookupNodeDiscovery lookupNodeDiscovery;

  @Before
  public void setup()
  {
    robuxNodeDiscoveryProvider = EasyMock.createStrictMock(RobuxNodeDiscoveryProvider.class);

    robuxNodeDiscovery = EasyMock.createStrictMock(RobuxNodeDiscovery.class);

    EasyMock.expect(robuxNodeDiscoveryProvider.getForService(LookupNodeService.DISCOVERY_SERVICE_KEY))
            .andReturn(robuxNodeDiscovery);

    DiscoveryRobuxNode node1 = new DiscoveryRobuxNode(
        new RobuxNode("s1", "h1", false, 8080, null, true, false),
        NodeRole.HISTORICAL,
        ImmutableMap.of(
            LookupNodeService.DISCOVERY_SERVICE_KEY, new LookupNodeService("tier1"))
    );

    DiscoveryRobuxNode node2 = new DiscoveryRobuxNode(
        new RobuxNode("s2", "h2", false, 8080, null, true, false),
        NodeRole.PEON,
        ImmutableMap.of(
            LookupNodeService.DISCOVERY_SERVICE_KEY, new LookupNodeService("tier1"))
    );

    DiscoveryRobuxNode node3 = new DiscoveryRobuxNode(
        new RobuxNode("s3", "h3", false, 8080, null, true, false),
        NodeRole.PEON,
        ImmutableMap.of(
            LookupNodeService.DISCOVERY_SERVICE_KEY, new LookupNodeService("tier2"))
    );

    EasyMock.expect(robuxNodeDiscovery.getAllNodes())
            .andReturn(ImmutableSet.of(node1, node2, node3))
            .anyTimes();

    EasyMock.replay(robuxNodeDiscoveryProvider, robuxNodeDiscovery);

    lookupNodeDiscovery = new LookupNodeDiscovery(robuxNodeDiscoveryProvider);
  }

  @Test
  public void testGetNodesInTier()
  {
    Assert.assertEquals(
        ImmutableList.of(
            HostAndPortWithScheme.fromParts("http", "h1", 8080),
            HostAndPortWithScheme.fromParts("http", "h2", 8080)
        ),
        ImmutableList.copyOf(lookupNodeDiscovery.getNodesInTier("tier1"))
    );

    Assert.assertEquals(
        ImmutableList.of(
            HostAndPortWithScheme.fromParts("http", "h3", 8080)
        ),
        ImmutableList.copyOf(lookupNodeDiscovery.getNodesInTier("tier2"))
    );

    Assert.assertEquals(
        ImmutableList.of(),
        ImmutableList.copyOf(lookupNodeDiscovery.getNodesInTier("tier3"))
    );

    EasyMock.verify(robuxNodeDiscoveryProvider, robuxNodeDiscovery);
  }

  @Test
  public void testGetAllTiers()
  {
    Assert.assertEquals(
        ImmutableSet.of("tier1", "tier2"),
        lookupNodeDiscovery.getAllTiers()
    );

    EasyMock.verify(robuxNodeDiscoveryProvider, robuxNodeDiscovery);
  }
}
