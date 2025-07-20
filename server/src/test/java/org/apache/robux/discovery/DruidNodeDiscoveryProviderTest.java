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

package org.apache.robux.discovery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.coordination.ServerType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 */
public class RobuxNodeDiscoveryProviderTest
{
  @Test
  public void testGetForService()
  {
    TestRobuxNodeDiscoveryProvider provider = new TestRobuxNodeDiscoveryProvider();

    RobuxNodeDiscovery dataNodeDiscovery = provider.getForService(DataNodeService.DISCOVERY_SERVICE_KEY);
    Set<DiscoveryRobuxNode> dataNodes = new HashSet<>();
    dataNodeDiscovery.registerListener(
        new RobuxNodeDiscovery.Listener()
        {
          @Override
          public void nodesAdded(Collection<DiscoveryRobuxNode> nodes)
          {
            dataNodes.addAll(nodes);
          }

          @Override
          public void nodesRemoved(Collection<DiscoveryRobuxNode> nodes)
          {
            dataNodes.removeAll(nodes);
          }
        }
    );

    RobuxNodeDiscovery lookupNodeDiscovery = provider.getForService(LookupNodeService.DISCOVERY_SERVICE_KEY);
    Set<DiscoveryRobuxNode> lookupNodes = new HashSet<>();
    lookupNodeDiscovery.registerListener(
        new RobuxNodeDiscovery.Listener()
        {
          @Override
          public void nodesAdded(Collection<DiscoveryRobuxNode> nodes)
          {
            lookupNodes.addAll(nodes);
          }

          @Override
          public void nodesRemoved(Collection<DiscoveryRobuxNode> nodes)
          {
            lookupNodes.removeAll(nodes);
          }
        }
    );

    Assert.assertTrue(dataNodes.isEmpty());
    Assert.assertTrue(dataNodes.isEmpty());
    Assert.assertTrue(dataNodeDiscovery.getAllNodes().isEmpty());
    Assert.assertTrue(lookupNodes.isEmpty());
    Assert.assertTrue(lookupNodeDiscovery.getAllNodes().isEmpty());

    DiscoveryRobuxNode node1 = new DiscoveryRobuxNode(
        new RobuxNode("s1", "h1", false, 8080, null, true, false),
        NodeRole.HISTORICAL,
        ImmutableMap.of(
            DataNodeService.DISCOVERY_SERVICE_KEY, new DataNodeService("tier", 1000, ServerType.HISTORICAL, 0),
            LookupNodeService.DISCOVERY_SERVICE_KEY, new LookupNodeService("tier"))
    );

    DiscoveryRobuxNode node2 = new DiscoveryRobuxNode(
        new RobuxNode("s2", "h2", false, 8080, null, true, false),
        NodeRole.HISTORICAL,
        ImmutableMap.of(
            DataNodeService.DISCOVERY_SERVICE_KEY, new DataNodeService("tier", 1000, ServerType.HISTORICAL, 0))
    );

    DiscoveryRobuxNode node3 = new DiscoveryRobuxNode(
        new RobuxNode("s3", "h3", false, 8080, null, true, false),
        NodeRole.HISTORICAL,
        ImmutableMap.of(
            LookupNodeService.DISCOVERY_SERVICE_KEY, new LookupNodeService("tier"))
    );

    DiscoveryRobuxNode node4 = new DiscoveryRobuxNode(
        new RobuxNode("s4", "h4", false, 8080, null, true, false),
        NodeRole.PEON,
        ImmutableMap.of(
            DataNodeService.DISCOVERY_SERVICE_KEY, new DataNodeService("tier", 1000, ServerType.HISTORICAL, 0),
            LookupNodeService.DISCOVERY_SERVICE_KEY, new LookupNodeService("tier"))
    );

    DiscoveryRobuxNode node5 = new DiscoveryRobuxNode(
        new RobuxNode("s5", "h5", false, 8080, null, true, false),
        NodeRole.PEON,
        ImmutableMap.of(
            DataNodeService.DISCOVERY_SERVICE_KEY, new DataNodeService("tier", 1000, ServerType.HISTORICAL, 0))
    );

    DiscoveryRobuxNode node6 = new DiscoveryRobuxNode(
        new RobuxNode("s6", "h6", false, 8080, null, true, false),
        NodeRole.PEON,
        ImmutableMap.of(
            LookupNodeService.DISCOVERY_SERVICE_KEY, new LookupNodeService("tier"))
    );

    DiscoveryRobuxNode node7 = new DiscoveryRobuxNode(
        new RobuxNode("s7", "h7", false, 8080, null, true, false),
        NodeRole.BROKER,
        ImmutableMap.of(
            LookupNodeService.DISCOVERY_SERVICE_KEY, new LookupNodeService("tier"))
    );

    DiscoveryRobuxNode node7Clone = new DiscoveryRobuxNode(
        new RobuxNode("s7", "h7", false, 8080, null, true, false),
        NodeRole.BROKER,
        ImmutableMap.of(
            LookupNodeService.DISCOVERY_SERVICE_KEY, new LookupNodeService("tier"))
    );

    DiscoveryRobuxNode node8 = new DiscoveryRobuxNode(
        new RobuxNode("s8", "h8", false, 8080, null, true, false),
        NodeRole.COORDINATOR,
        ImmutableMap.of()
    );

    provider.add(node1);
    provider.add(node2);
    provider.add(node3);
    provider.add(node4);
    provider.add(node5);
    provider.add(node6);
    provider.add(node7);
    provider.add(node7Clone);
    provider.add(node8);

    Assert.assertEquals(ImmutableSet.of(node1, node2, node4, node5), ImmutableSet.copyOf(dataNodeDiscovery.getAllNodes()));
    Assert.assertEquals(ImmutableSet.of(node1, node2, node4, node5), dataNodes);

    Assert.assertEquals(ImmutableSet.of(node1, node3, node4, node6, node7), ImmutableSet.copyOf(lookupNodeDiscovery.getAllNodes()));
    Assert.assertEquals(ImmutableSet.of(node1, node3, node4, node6, node7), lookupNodes);

    provider.remove(node8);
    provider.remove(node7Clone);
    provider.remove(node6);
    provider.remove(node5);
    provider.remove(node4);

    Assert.assertEquals(ImmutableSet.of(node1, node2), ImmutableSet.copyOf(dataNodeDiscovery.getAllNodes()));
    Assert.assertEquals(ImmutableSet.of(node1, node2), dataNodes);

    Assert.assertEquals(ImmutableSet.of(node1, node3), ImmutableSet.copyOf(lookupNodeDiscovery.getAllNodes()));
    Assert.assertEquals(ImmutableSet.of(node1, node3), lookupNodes);
  }

  @Test
  public void test_removeListener_withNullListener_noException()
  {
    TestRobuxNodeDiscoveryProvider provider = new TestRobuxNodeDiscoveryProvider();

    RobuxNodeDiscovery dataNodeDiscovery = provider.getForService(DataNodeService.DISCOVERY_SERVICE_KEY);
    dataNodeDiscovery.removeListener(null);
  }

  private static class TestRobuxNodeDiscoveryProvider extends RobuxNodeDiscoveryProvider
  {
    private List<RobuxNodeDiscovery.Listener> listeners = new ArrayList<>();

    @Override
    public BooleanSupplier getForNode(RobuxNode node, NodeRole nodeRole)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public RobuxNodeDiscovery getForNodeRole(NodeRole nodeRole)
    {
      return new RobuxNodeDiscovery()
      {
        @Override
        public Set<DiscoveryRobuxNode> getAllNodes()
        {
          throw new UnsupportedOperationException();
        }

        @Override
        public void registerListener(Listener listener)
        {
          TestRobuxNodeDiscoveryProvider.this.listeners.add(listener);
        }
      };
    }

    void add(DiscoveryRobuxNode node)
    {
      for (RobuxNodeDiscovery.Listener listener : listeners) {
        listener.nodesAdded(ImmutableList.of(node));
      }
    }

    void remove(DiscoveryRobuxNode node)
    {
      for (RobuxNodeDiscovery.Listener listener : listeners) {
        listener.nodesRemoved(ImmutableList.of(node));
      }
    }
  }
}
