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

import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.discovery.RobuxLeaderSelector;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.server.RobuxNode;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class K8sRobuxLeaderSelectorTest
{
  private final DiscoveryRobuxNode testNode1 = new DiscoveryRobuxNode(
      new RobuxNode("robux/router", "test-host1", true, 80, null, true, false),
      NodeRole.ROUTER,
      null
  );

  private final K8sDiscoveryConfig discoveryConfig = new K8sDiscoveryConfig("robux-cluster", null, null,
                                                                            "default", "default", Duration.millis(10_000), Duration.millis(7_000), Duration.millis(3_000));

  private final String lockResourceName = "robux-leader-election";

  @Test(timeout = 5_000)
  public void testLeaderElection_HappyPath() throws Exception
  {
    K8sRobuxLeaderSelector leaderSelector = new K8sRobuxLeaderSelector(
        testNode1.getRobuxNode(),
        lockResourceName,
        discoveryConfig.getCoordinatorLeaderElectionConfigMapNamespace(),
        discoveryConfig,
        new K8sLeaderElectorFactory()
        {
          @Override
          public K8sLeaderElector create(String candidateId, String namespace, String lockResourceName)
          {
            return new K8sLeaderElector()
            {
              @Override
              public String getCurrentLeader()
              {
                return testNode1.getRobuxNode().getHostAndPortToUse();
              }

              @Override
              public void run(Runnable startLeadingHook, Runnable stopLeadingHook)
              {
                startLeadingHook.run();
                try {
                  Thread.sleep(Long.MAX_VALUE);
                }
                catch (InterruptedException ex) {
                  stopLeadingHook.run();
                }
              }
            };
          }
        }
    );

    Assert.assertEquals(testNode1.getRobuxNode().getHostAndPortToUse(), leaderSelector.getCurrentLeader());

    CountDownLatch becomeLeaderLatch = new CountDownLatch(1);
    CountDownLatch stopBeingLeaderLatch = new CountDownLatch(1);

    leaderSelector.registerListener(
        new RobuxLeaderSelector.Listener()
        {
          @Override
          public void becomeLeader()
          {
            becomeLeaderLatch.countDown();
          }

          @Override
          public void stopBeingLeader()
          {
            stopBeingLeaderLatch.countDown();
          }
        }
    );

    becomeLeaderLatch.await();
    leaderSelector.unregisterListener();
    stopBeingLeaderLatch.await();
  }

  @Test(timeout = 5_000)
  public void testLeaderElection_LeaderElectorExits() throws Exception
  {
    K8sRobuxLeaderSelector leaderSelector = new K8sRobuxLeaderSelector(
        testNode1.getRobuxNode(),
        lockResourceName,
        discoveryConfig.getCoordinatorLeaderElectionConfigMapNamespace(),
        discoveryConfig,
        new K8sLeaderElectorFactory()
        {
          @Override
          public K8sLeaderElector create(String candidateId, String namespace, String lockResourceName)
          {
            return new K8sLeaderElector()
            {
              private boolean isFirstTime = true;

              @Override
              public String getCurrentLeader()
              {
                return testNode1.getRobuxNode().getHostAndPortToUse();
              }

              @Override
              public void run(Runnable startLeadingHook, Runnable stopLeadingHook)
              {
                startLeadingHook.run();

                if (isFirstTime) {
                  isFirstTime = false;
                  stopLeadingHook.run();
                } else {
                  try {
                    Thread.sleep(Long.MAX_VALUE);
                  }
                  catch (InterruptedException ex) {
                    stopLeadingHook.run();
                  }
                }
              }
            };
          }
        }
    );

    Assert.assertEquals(testNode1.getRobuxNode().getHostAndPortToUse(), leaderSelector.getCurrentLeader());

    CountDownLatch becomeLeaderLatch = new CountDownLatch(2);
    CountDownLatch stopBeingLeaderLatch = new CountDownLatch(2);

    leaderSelector.registerListener(
        new RobuxLeaderSelector.Listener()
        {
          @Override
          public void becomeLeader()
          {
            becomeLeaderLatch.countDown();
          }

          @Override
          public void stopBeingLeader()
          {
            stopBeingLeaderLatch.countDown();
          }
        }
    );

    becomeLeaderLatch.await();
    leaderSelector.unregisterListener();
    stopBeingLeaderLatch.await();
  }
}
