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

package org.apache.robux.testsEx.leadership;

import com.google.inject.Inject;
import org.apache.robux.cli.CliCustomNodeRole;
import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.discovery.RobuxNodeDiscovery;
import org.apache.robux.discovery.RobuxNodeDiscoveryProvider;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.java.util.http.client.HttpClient;
import org.apache.robux.testing.IntegrationTestingConfig;
import org.apache.robux.testing.guice.TestClient;
import org.apache.robux.testing.utils.SqlTestQueryHelper;
import org.apache.robux.testsEx.categories.HighAvailability;
import org.apache.robux.testsEx.cluster.RobuxClusterClient;
import org.apache.robux.testsEx.config.RobuxTestRunner;
import org.apache.robux.testsEx.config.Initializer;
import org.apache.robux.testsEx.indexer.AbstractIndexerTest;
import org.apache.robux.testsEx.utils.RobuxClusterAdminClient;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(RobuxTestRunner.class)
@Category(HighAvailability.class)
public class ITHighAvailabilityTest
{
  private static final Logger LOG = new Logger(ITHighAvailabilityTest.class);
  private static final String SYSTEM_QUERIES_RESOURCE = Initializer.queryFile(HighAvailability.class, "sys.json");
  private static final int NUM_LEADERSHIP_SWAPS = 3;

  @Inject
  private IntegrationTestingConfig config;

  @Inject
  private RobuxClusterAdminClient robuxClusterAdminClient;

  @Inject
  private RobuxNodeDiscoveryProvider robuxNodeDiscovery;

  @Inject
  private SqlTestQueryHelper queryHelper;

  @Inject
  @TestClient
  private HttpClient httpClient;

  @Inject
  private RobuxClusterClient clusterClient;

  @Test
  public void testLeadershipChanges() throws Exception
  {
    int runCount = 0;
    String previousCoordinatorLeader = null;
    String previousOverlordLeader = null;
    // fetch current leaders, make sure queries work, then swap leaders and do it again
    do {
      String coordinatorLeader = getLeader("coordinator");
      String overlordLeader = getLeader("indexer");

      // we expect leadership swap to happen
      assertNotEquals(previousCoordinatorLeader, coordinatorLeader);
      assertNotEquals(previousOverlordLeader, overlordLeader);

      previousCoordinatorLeader = coordinatorLeader;
      previousOverlordLeader = overlordLeader;

      String queries = fillTemplate(
          AbstractIndexerTest.getResourceAsString(SYSTEM_QUERIES_RESOURCE),
          overlordLeader,
          coordinatorLeader
      );
      queryHelper.testQueriesFromString(queries);

      swapLeadersAndWait(coordinatorLeader, overlordLeader);
    } while (runCount++ < NUM_LEADERSHIP_SWAPS);
  }

  @Test
  public void testDiscoveryAndSelfDiscovery()
  {
    // The cluster used here has an abbreviated set of services.
    verifyRoleDiscovery(NodeRole.BROKER, 1);
    verifyRoleDiscovery(NodeRole.COORDINATOR, 2);
    verifyRoleDiscovery(NodeRole.OVERLORD, 2);
    verifyRoleDiscovery(NodeRole.ROUTER, 1);
  }

  public void verifyRoleDiscovery(NodeRole role, int expectedCount)
  {
    RobuxNodeDiscovery discovered = robuxNodeDiscovery.getForNodeRole(role);
    try {
      int count = 0;
      for (DiscoveryRobuxNode node : discovered.getAllNodes()) {
        if (clusterClient.selfDiscovered(clusterClient.nodeUrl(node.getRobuxNode()))) {
          count++;
        }
      }
      assertEquals(expectedCount, count);
    }
    catch (Exception e) {
      LOG.error(e, "node discovery failed");
      fail();
    }
  }

  @Test
  public void testCustomDiscovery()
  {
    verifyRoleDiscovery(CliCustomNodeRole.NODE_ROLE, 1);
    verifyCoordinatorCluster();
  }

  private void swapLeadersAndWait(String coordinatorLeader, String overlordLeader)
  {
    String coordUrl;
    String coordLabel;
    if (isCoordinatorOneLeader(coordinatorLeader)) {
      robuxClusterAdminClient.restartCoordinatorContainer();
      coordUrl = config.getCoordinatorUrl();
      coordLabel = "coordinator one";
    } else {
      robuxClusterAdminClient.restartCoordinatorTwoContainer();
      coordUrl = config.getCoordinatorTwoUrl();
      coordLabel = "coordinator two";
    }

    String overlordUrl;
    String overlordLabel;
    if (isOverlordOneLeader(overlordLeader)) {
      robuxClusterAdminClient.restartOverlordContainer();
      overlordUrl = config.getOverlordUrl();
      overlordLabel = "overlord one";
    } else {
      robuxClusterAdminClient.restartOverlordTwoContainer();
      overlordUrl = config.getOverlordTwoUrl();
      overlordLabel = "overlord two";
    }
    clusterClient.waitForNodeReady(coordLabel, coordUrl);
    clusterClient.waitForNodeReady(overlordLabel, overlordUrl);
  }

  private String getLeader(String service)
  {
    return clusterClient.getLeader(service);
  }

  private String fillTemplate(String template, String overlordLeader, String coordinatorLeader)
  {
    /*
      {"host":"%%BROKER%%","server_type":"broker", "is_leader": null},
      {"host":"%%COORDINATOR_ONE%%","server_type":"coordinator", "is_leader": %%COORDINATOR_ONE_LEADER%%},
      {"host":"%%COORDINATOR_TWO%%","server_type":"coordinator", "is_leader": %%COORDINATOR_TWO_LEADER%%},
      {"host":"%%OVERLORD_ONE%%","server_type":"overlord", "is_leader": %%OVERLORD_ONE_LEADER%%},
      {"host":"%%OVERLORD_TWO%%","server_type":"overlord", "is_leader": %%OVERLORD_TWO_LEADER%%},
      {"host":"%%ROUTER%%","server_type":"router", "is_leader": null},
     */
    String working = template;
    working = StringUtils.replace(working, "%%OVERLORD_ONE%%", config.getOverlordInternalHost());
    working = StringUtils.replace(working, "%%OVERLORD_TWO%%", config.getOverlordTwoInternalHost());
    working = StringUtils.replace(working, "%%COORDINATOR_ONE%%", config.getCoordinatorInternalHost());
    working = StringUtils.replace(working, "%%COORDINATOR_TWO%%", config.getCoordinatorTwoInternalHost());
    working = StringUtils.replace(working, "%%BROKER%%", config.getBrokerInternalHost());
    working = StringUtils.replace(working, "%%ROUTER%%", config.getRouterInternalHost());
    if (isOverlordOneLeader(overlordLeader)) {
      working = StringUtils.replace(working, "%%OVERLORD_ONE_LEADER%%", "1");
      working = StringUtils.replace(working, "%%OVERLORD_TWO_LEADER%%", "0");
    } else {
      working = StringUtils.replace(working, "%%OVERLORD_ONE_LEADER%%", "0");
      working = StringUtils.replace(working, "%%OVERLORD_TWO_LEADER%%", "1");
    }
    if (isCoordinatorOneLeader(coordinatorLeader)) {
      working = StringUtils.replace(working, "%%COORDINATOR_ONE_LEADER%%", "1");
      working = StringUtils.replace(working, "%%COORDINATOR_TWO_LEADER%%", "0");
    } else {
      working = StringUtils.replace(working, "%%COORDINATOR_ONE_LEADER%%", "0");
      working = StringUtils.replace(working, "%%COORDINATOR_TWO_LEADER%%", "1");
    }
    return working;
  }

  private boolean isCoordinatorOneLeader(String coordinatorLeader)
  {
    return coordinatorLeader.contains(transformHost(config.getCoordinatorInternalHost()));
  }

  private boolean isOverlordOneLeader(String overlordLeader)
  {
    return overlordLeader.contains(transformHost(config.getOverlordInternalHost()));
  }

  /**
   * host + ':' which should be enough to distinguish subsets, e.g. 'robux-coordinator:8081' from
   * 'robux-coordinator-two:8081' for example
   */
  private static String transformHost(String host)
  {
    return StringUtils.format("%s:", host);
  }

  private void verifyCoordinatorCluster()
  {
    // Verify the basics: 4 service types, excluding the custom node role.
    // One of the two-node services has a size of 2.
    // This endpoint includes an entry for historicals, even if none are running.
    Map<String, Object> results = clusterClient.coordinatorCluster();
    assertEquals(5, results.size());
    @SuppressWarnings("unchecked")
    List<Object> coordNodes = (List<Object>) results.get(NodeRole.COORDINATOR.getJsonName());
    assertEquals(2, coordNodes.size());
    @SuppressWarnings("unchecked")
    List<Object> histNodes = (List<Object>) results.get(NodeRole.HISTORICAL.getJsonName());
    assertTrue(histNodes.isEmpty());
  }
}
