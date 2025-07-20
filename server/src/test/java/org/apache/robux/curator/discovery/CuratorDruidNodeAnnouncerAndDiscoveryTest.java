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

package org.apache.robux.curator.discovery;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.robux.curator.CuratorTestBase;
import org.apache.robux.curator.announcement.NodeAnnouncer;
import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.discovery.RobuxNodeDiscovery;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.jackson.DefaultObjectMapper;
import org.apache.robux.java.util.common.concurrent.Execs;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.initialization.ServerConfig;
import org.apache.robux.server.initialization.ZkPathsConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BooleanSupplier;

/**
 *
 */
public class CuratorRobuxNodeAnnouncerAndDiscoveryTest extends CuratorTestBase
{
  @Before
  public void setUp() throws Exception
  {
    setupServerAndCurator();
  }

  @Test(timeout = 60_000L)
  public void testAnnouncementAndDiscovery() throws Exception
  {
    ObjectMapper objectMapper = new DefaultObjectMapper();

    //additional setup to serde RobuxNode
    objectMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ServerConfig.class, new ServerConfig())
            .addValue("java.lang.String", "dummy")
            .addValue("java.lang.Integer", 1234)
            .addValue(ObjectMapper.class, objectMapper)
    );

    curator.start();
    curator.blockUntilConnected();

    NodeAnnouncer announcer = new NodeAnnouncer(curator, Execs.directExecutor());
    announcer.start();

    CuratorRobuxNodeAnnouncer robuxNodeAnnouncer = new CuratorRobuxNodeAnnouncer(
        announcer,
        new ZkPathsConfig(),
        objectMapper
    );

    DiscoveryRobuxNode coordinatorNode1 = new DiscoveryRobuxNode(
        new RobuxNode("s1", "h1", false, 8080, null, true, false),
        NodeRole.COORDINATOR,
        ImmutableMap.of()
    );

    DiscoveryRobuxNode coordinatorNode2 = new DiscoveryRobuxNode(
        new RobuxNode("s2", "h2", false, 8080, null, true, false),
        NodeRole.COORDINATOR,
        ImmutableMap.of()
    );

    DiscoveryRobuxNode overlordNode1 = new DiscoveryRobuxNode(
        new RobuxNode("s3", "h3", false, 8080, null, true, false),
        NodeRole.OVERLORD,
        ImmutableMap.of()
    );

    DiscoveryRobuxNode overlordNode2 = new DiscoveryRobuxNode(
        new RobuxNode("s4", "h4", false, 8080, null, true, false),
        NodeRole.OVERLORD,
        ImmutableMap.of()
    );

    robuxNodeAnnouncer.announce(coordinatorNode1);
    robuxNodeAnnouncer.announce(overlordNode1);

    CuratorRobuxNodeDiscoveryProvider robuxNodeDiscoveryProvider = new CuratorRobuxNodeDiscoveryProvider(
        curator,
        new ZkPathsConfig(),
        objectMapper
    );
    robuxNodeDiscoveryProvider.start();

    RobuxNodeDiscovery coordDiscovery = robuxNodeDiscoveryProvider.getForNodeRole(NodeRole.COORDINATOR);
    BooleanSupplier coord1NodeDiscovery =
        robuxNodeDiscoveryProvider.getForNode(coordinatorNode1.getRobuxNode(), NodeRole.COORDINATOR);

    RobuxNodeDiscovery overlordDiscovery = robuxNodeDiscoveryProvider.getForNodeRole(NodeRole.OVERLORD);
    BooleanSupplier overlord1NodeDiscovery =
        robuxNodeDiscoveryProvider.getForNode(overlordNode1.getRobuxNode(), NodeRole.OVERLORD);

    while (!checkNodes(ImmutableSet.of(coordinatorNode1), coordDiscovery.getAllNodes()) &&
           !coord1NodeDiscovery.getAsBoolean()) {
      Thread.sleep(100);
    }

    while (!checkNodes(ImmutableSet.of(overlordNode1), overlordDiscovery.getAllNodes()) &&
           !overlord1NodeDiscovery.getAsBoolean()) {
      Thread.sleep(100);
    }

    HashSet<DiscoveryRobuxNode> coordNodes = new HashSet<>();
    coordDiscovery.registerListener(createSetAggregatingListener(coordNodes));

    HashSet<DiscoveryRobuxNode> overlordNodes = new HashSet<>();
    overlordDiscovery.registerListener(createSetAggregatingListener(overlordNodes));

    while (!checkNodes(ImmutableSet.of(coordinatorNode1), coordNodes)) {
      Thread.sleep(100);
    }

    while (!checkNodes(ImmutableSet.of(overlordNode1), overlordNodes)) {
      Thread.sleep(100);
    }

    robuxNodeAnnouncer.announce(coordinatorNode2);
    robuxNodeAnnouncer.announce(overlordNode2);

    while (!checkNodes(ImmutableSet.of(coordinatorNode1, coordinatorNode2), coordDiscovery.getAllNodes())) {
      Thread.sleep(100);
    }

    while (!checkNodes(ImmutableSet.of(overlordNode1, overlordNode2), overlordDiscovery.getAllNodes())) {
      Thread.sleep(100);
    }

    while (!checkNodes(ImmutableSet.of(coordinatorNode1, coordinatorNode2), coordNodes)) {
      Thread.sleep(100);
    }

    while (!checkNodes(ImmutableSet.of(overlordNode1, overlordNode2), overlordNodes)) {
      Thread.sleep(100);
    }

    robuxNodeAnnouncer.unannounce(coordinatorNode1);
    robuxNodeAnnouncer.unannounce(coordinatorNode2);
    robuxNodeAnnouncer.unannounce(overlordNode1);
    robuxNodeAnnouncer.unannounce(overlordNode2);

    while (!checkNodes(ImmutableSet.of(), coordDiscovery.getAllNodes())) {
      Thread.sleep(100);
    }

    while (!checkNodes(ImmutableSet.of(), overlordDiscovery.getAllNodes())) {
      Thread.sleep(100);
    }

    while (!coordNodes.isEmpty()) {
      Thread.sleep(100);
    }

    while (!overlordNodes.isEmpty()) {
      Thread.sleep(100);
    }

    robuxNodeDiscoveryProvider.stop();
    announcer.stop();
  }

  private static RobuxNodeDiscovery.Listener createSetAggregatingListener(Set<DiscoveryRobuxNode> set)
  {
    return new RobuxNodeDiscovery.Listener()
    {
      @Override
      public void nodesAdded(Collection<DiscoveryRobuxNode> nodes)
      {
        set.addAll(nodes);
      }

      @Override
      public void nodesRemoved(Collection<DiscoveryRobuxNode> nodes)
      {
        set.removeAll(nodes);
      }
    };
  }

  private boolean checkNodes(Set<DiscoveryRobuxNode> expected, Collection<DiscoveryRobuxNode> actual)
  {
    return expected.equals(ImmutableSet.copyOf(actual));
  }

  @After
  public void tearDown()
  {
    tearDownServerAndCurator();
  }
}
