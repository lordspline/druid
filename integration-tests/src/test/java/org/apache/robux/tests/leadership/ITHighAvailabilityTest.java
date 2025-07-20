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

package org.apache.robux.tests.leadership;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.robux.cli.CliCustomNodeRole;
import org.apache.robux.curator.discovery.ServerDiscoveryFactory;
import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.discovery.RobuxNodeDiscovery;
import org.apache.robux.discovery.RobuxNodeDiscoveryProvider;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.java.util.http.client.HttpClient;
import org.apache.robux.java.util.http.client.Request;
import org.apache.robux.java.util.http.client.response.StatusResponseHandler;
import org.apache.robux.java.util.http.client.response.StatusResponseHolder;
import org.apache.robux.testing.IntegrationTestingConfig;
import org.apache.robux.testing.clients.CoordinatorResourceTestClient;
import org.apache.robux.testing.guice.RobuxTestModuleFactory;
import org.apache.robux.testing.guice.TestClient;
import org.apache.robux.testing.utils.RobuxClusterAdminClient;
import org.apache.robux.testing.utils.ITRetryUtil;
import org.apache.robux.testing.utils.SqlTestQueryHelper;
import org.apache.robux.tests.TestNGGroup;
import org.apache.robux.tests.indexer.AbstractIndexerTest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Test(groups = TestNGGroup.HIGH_AVAILABILTY)
@Guice(moduleFactory = RobuxTestModuleFactory.class)
public class ITHighAvailabilityTest
{
  private static final Logger LOG = new Logger(ITHighAvailabilityTest.class);
  private static final String SYSTEM_QUERIES_RESOURCE = "/queries/high_availability_sys.json";
  private static final int NUM_LEADERSHIP_SWAPS = 3;

  private static final int NUM_RETRIES = 120;
  private static final long RETRY_DELAY = TimeUnit.SECONDS.toMillis(5);

  @Inject
  private IntegrationTestingConfig config;

  @Inject
  private RobuxClusterAdminClient robuxClusterAdminClient;

  @Inject
  ServerDiscoveryFactory factory;

  @Inject
  RobuxNodeDiscoveryProvider robuxNodeDiscovery;

  @Inject
  CoordinatorResourceTestClient coordinatorClient;

  @Inject
  SqlTestQueryHelper queryHelper;

  @Inject
  ObjectMapper jsonMapper;

  @Inject
  @TestClient
  HttpClient httpClient;

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
      Assert.assertNotEquals(previousCoordinatorLeader, coordinatorLeader);
      Assert.assertNotEquals(previousOverlordLeader, overlordLeader);

      previousCoordinatorLeader = coordinatorLeader;
      previousOverlordLeader = overlordLeader;

      String queries = fillTemplate(
          config,
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
    ITRetryUtil.retryUntil(
        () -> {
          try {
            List<RobuxNodeDiscovery> disco = ImmutableList.of(
                robuxNodeDiscovery.getForNodeRole(NodeRole.COORDINATOR),
                robuxNodeDiscovery.getForNodeRole(NodeRole.OVERLORD),
                robuxNodeDiscovery.getForNodeRole(NodeRole.HISTORICAL),
                robuxNodeDiscovery.getForNodeRole(NodeRole.MIDDLE_MANAGER),
                robuxNodeDiscovery.getForNodeRole(NodeRole.INDEXER),
                robuxNodeDiscovery.getForNodeRole(NodeRole.BROKER),
                robuxNodeDiscovery.getForNodeRole(NodeRole.ROUTER)
            );

            int servicesDiscovered = 0;
            for (RobuxNodeDiscovery nodeRole : disco) {
              Collection<DiscoveryRobuxNode> nodes = nodeRole.getAllNodes();
              servicesDiscovered += testSelfDiscovery(nodes);
            }
            return servicesDiscovered > 5;
          }
          catch (Throwable t) {
            return false;
          }
        },
        true,
        RETRY_DELAY,
        NUM_RETRIES,
        "Standard services discovered"
    );
  }

  @Test
  public void testCustomDiscovery()
  {
    ITRetryUtil.retryUntil(
        () -> {
          try {
            RobuxNodeDiscovery customDisco =
                robuxNodeDiscovery.getForNodeRole(new NodeRole(CliCustomNodeRole.SERVICE_NAME));
            int count = testSelfDiscovery(customDisco.getAllNodes());
            return count > 0;
          }
          catch (Throwable t) {
            return false;
          }
        },
        true,
        RETRY_DELAY,
        NUM_RETRIES,
        "Custom service discovered"
    );
  }

  private int testSelfDiscovery(Collection<DiscoveryRobuxNode> nodes)
      throws MalformedURLException, ExecutionException, InterruptedException
  {
    int count = 0;

    for (DiscoveryRobuxNode node : nodes) {
      final String location = StringUtils.format(
          "http://%s:%s/status/selfDiscovered",
          config.isDocker() ? config.getDockerHost() : node.getRobuxNode().getHost(),
          node.getRobuxNode().getPlaintextPort()
      );
      LOG.info("testing self discovery %s", location);
      StatusResponseHolder response = httpClient.go(
          new Request(HttpMethod.GET, new URL(location)),
          StatusResponseHandler.getInstance()
      ).get();
      LOG.info("%s responded with %s", location, response.getStatus().getCode());
      Assert.assertEquals(response.getStatus(), HttpResponseStatus.OK);
      count++;
    }
    return count;
  }

  private void swapLeadersAndWait(String coordinatorLeader, String overlordLeader)
  {
    Runnable waitUntilCoordinatorSupplier;
    if (isCoordinatorOneLeader(config, coordinatorLeader)) {
      robuxClusterAdminClient.restartCoordinatorContainer();
      waitUntilCoordinatorSupplier = () -> robuxClusterAdminClient.waitUntilCoordinatorReady();
    } else {
      robuxClusterAdminClient.restartCoordinatorTwoContainer();
      waitUntilCoordinatorSupplier = () -> robuxClusterAdminClient.waitUntilCoordinatorTwoReady();
    }

    Runnable waitUntilOverlordSupplier;
    if (isOverlordOneLeader(config, overlordLeader)) {
      robuxClusterAdminClient.restartOverlordContainer();
      waitUntilOverlordSupplier = () -> robuxClusterAdminClient.waitUntilIndexerReady();
    } else {
      robuxClusterAdminClient.restartOverlordTwoContainer();
      waitUntilOverlordSupplier = () -> robuxClusterAdminClient.waitUntilOverlordTwoReady();
    }
    waitUntilCoordinatorSupplier.run();
    waitUntilOverlordSupplier.run();
  }

  private String getLeader(String service)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.GET,
              new URL(StringUtils.format(
                  "%s/robux/%s/v1/leader",
                  config.getRouterUrl(),
                  service
              ))
          ),
          StatusResponseHandler.getInstance()
      ).get();

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while fetching leader from[%s] status[%s] content[%s]",
            config.getRouterUrl(),
            response.getStatus(),
            response.getContent()
        );
      }
      return response.getContent();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String fillTemplate(IntegrationTestingConfig config, String template, String overlordLeader, String coordinatorLeader)
  {
    /*
      {"host":"%%BROKER%%","server_type":"broker", "is_leader": null},
      {"host":"%%COORDINATOR_ONE%%","server_type":"coordinator", "is_leader": %%COORDINATOR_ONE_LEADER%%},
      {"host":"%%COORDINATOR_TWO%%","server_type":"coordinator", "is_leader": %%COORDINATOR_TWO_LEADER%%},
      {"host":"%%OVERLORD_ONE%%","server_type":"overlord", "is_leader": %%OVERLORD_ONE_LEADER%%},
      {"host":"%%OVERLORD_TWO%%","server_type":"overlord", "is_leader": %%OVERLORD_TWO_LEADER%%},
      {"host":"%%ROUTER%%","server_type":"router", "is_leader": null}
     */
    String working = template;

    working = StringUtils.replace(working, "%%OVERLORD_ONE%%", config.getOverlordInternalHost());
    working = StringUtils.replace(working, "%%OVERLORD_TWO%%", config.getOverlordTwoInternalHost());
    working = StringUtils.replace(working, "%%COORDINATOR_ONE%%", config.getCoordinatorInternalHost());
    working = StringUtils.replace(working, "%%COORDINATOR_TWO%%", config.getCoordinatorTwoInternalHost());
    working = StringUtils.replace(working, "%%BROKER%%", config.getBrokerInternalHost());
    working = StringUtils.replace(working, "%%ROUTER%%", config.getRouterInternalHost());
    if (isOverlordOneLeader(config, overlordLeader)) {
      working = StringUtils.replace(working, "%%OVERLORD_ONE_LEADER%%", "1");
      working = StringUtils.replace(working, "%%OVERLORD_TWO_LEADER%%", "0");
    } else {
      working = StringUtils.replace(working, "%%OVERLORD_ONE_LEADER%%", "0");
      working = StringUtils.replace(working, "%%OVERLORD_TWO_LEADER%%", "1");
    }
    if (isCoordinatorOneLeader(config, coordinatorLeader)) {
      working = StringUtils.replace(working, "%%COORDINATOR_ONE_LEADER%%", "1");
      working = StringUtils.replace(working, "%%COORDINATOR_TWO_LEADER%%", "0");
    } else {
      working = StringUtils.replace(working, "%%COORDINATOR_ONE_LEADER%%", "0");
      working = StringUtils.replace(working, "%%COORDINATOR_TWO_LEADER%%", "1");
    }
    return working;
  }

  private static boolean isCoordinatorOneLeader(IntegrationTestingConfig config, String coordinatorLeader)
  {
    return coordinatorLeader.contains(transformHost(config.getCoordinatorInternalHost()));
  }

  private static boolean isOverlordOneLeader(IntegrationTestingConfig config, String overlordLeader)
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
}
