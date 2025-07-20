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

package org.apache.robux.k8s.discovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kubernetes.client.util.Config;
import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.discovery.RobuxNodeDiscovery;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.jackson.DefaultObjectMapper;
import org.apache.robux.server.RobuxNode;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.function.BooleanSupplier;

/**
 * This is not a UT, but very helpful when making changes to ensure things work with real K8S Api Server.
 * It is ignored in the build but checked in the reporitory for running manually by devs.
 */
@Ignore("Needs K8S API Server")
public class K8sAnnouncerAndDiscoveryIntTest
{
  private final DiscoveryRobuxNode testNode = new DiscoveryRobuxNode(
      new RobuxNode("robux/router", "test-host", true, 80, null, true, false),
      NodeRole.ROUTER,
      null
  );

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private final PodInfo podInfo = new PodInfo("busybox", "default");

  private final K8sDiscoveryConfig discoveryConfig = new K8sDiscoveryConfig("robux-cluster", null, null, null, null, null, null, null);

  @Test(timeout = 30000L)
  public void testAnnouncementAndDiscoveryWorkflow() throws Exception
  {
    K8sApiClient k8sApiClient = new DefaultK8sApiClient(Config.defaultClient(), new DefaultObjectMapper());

    K8sRobuxNodeDiscoveryProvider discoveryProvider = new K8sRobuxNodeDiscoveryProvider(
        podInfo,
        discoveryConfig,
        k8sApiClient
    );
    discoveryProvider.start();

    BooleanSupplier nodeInquirer = discoveryProvider.getForNode(testNode.getRobuxNode(), NodeRole.ROUTER);
    Assert.assertFalse(nodeInquirer.getAsBoolean());

    RobuxNodeDiscovery discovery = discoveryProvider.getForNodeRole(NodeRole.ROUTER);

    CountDownLatch nodeViewInitialized = new CountDownLatch(1);
    CountDownLatch nodeAppeared = new CountDownLatch(1);
    CountDownLatch nodeDisappeared = new CountDownLatch(1);

    discovery.registerListener(
        new RobuxNodeDiscovery.Listener()
        {
          @Override
          public void nodesAdded(Collection<DiscoveryRobuxNode> nodes)
          {
            Iterator<DiscoveryRobuxNode> iter = nodes.iterator();
            if (iter.hasNext() && testNode.getRobuxNode().getHostAndPort().equals(iter.next().getRobuxNode().getHostAndPort())) {
              nodeAppeared.countDown();
            }
          }

          @Override
          public void nodesRemoved(Collection<DiscoveryRobuxNode> nodes)
          {
            Iterator<DiscoveryRobuxNode> iter = nodes.iterator();
            if (iter.hasNext() && testNode.getRobuxNode().getHostAndPort().equals(iter.next().getRobuxNode().getHostAndPort())) {
              nodeDisappeared.countDown();
            }
          }

          @Override
          public void nodeViewInitialized()
          {
            nodeViewInitialized.countDown();
          }

          @Override
          public void nodeViewInitializedTimedOut()
          {
            nodeViewInitialized();
          }
        }
    );

    nodeViewInitialized.await();

    K8sRobuxNodeAnnouncer announcer = new K8sRobuxNodeAnnouncer(podInfo, discoveryConfig, k8sApiClient, jsonMapper);
    announcer.announce(testNode);

    nodeAppeared.await();

    Assert.assertTrue(nodeInquirer.getAsBoolean());

    announcer.unannounce(testNode);

    nodeDisappeared.await();

    Assert.assertFalse(nodeInquirer.getAsBoolean());

    discoveryProvider.stop();
  }
}
