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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.kubernetes.client.util.Watch;
import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.discovery.RobuxNodeDiscovery;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.server.RobuxNode;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class K8sRobuxNodeDiscoveryProviderTest
{
  private static final Logger LOGGER = new Logger(K8sRobuxNodeDiscoveryProviderTest.class);

  private final DiscoveryRobuxNode testNode1 = new DiscoveryRobuxNode(
      new RobuxNode("robux/router", "test-host1", true, 80, null, true, false),
      NodeRole.ROUTER,
      null
  );

  private final DiscoveryRobuxNode testNode2 = new DiscoveryRobuxNode(
      new RobuxNode("robux/router", "test-host2", true, 80, null, true, false),
      NodeRole.ROUTER,
      null
  );

  private final DiscoveryRobuxNode testNode3 = new DiscoveryRobuxNode(
      new RobuxNode("robux/router", "test-host3", true, 80, null, true, false),
      NodeRole.ROUTER,
      null
  );

  private final DiscoveryRobuxNode testNode4 = new DiscoveryRobuxNode(
      new RobuxNode("robux/router", "test-host4", true, 80, null, true, false),
      NodeRole.ROUTER,
      null
  );

  private final DiscoveryRobuxNode testNode5 = new DiscoveryRobuxNode(
      new RobuxNode("robux/router", "test-host5", true, 80, null, true, false),
      NodeRole.ROUTER,
      null
  );

  private final PodInfo podInfo = new PodInfo("testpod", "testns");

  private final K8sDiscoveryConfig discoveryConfig = new K8sDiscoveryConfig("robux-cluster", null, null, null, null, null, null, null);

  @Test(timeout = 60_000)
  public void testGetForNodeRole() throws Exception
  {
    String labelSelector = "robuxDiscoveryAnnouncement-cluster-identifier=robux-cluster,robuxDiscoveryAnnouncement-router=true";
    K8sApiClient mockK8sApiClient = EasyMock.createMock(K8sApiClient.class);
    EasyMock.expect(mockK8sApiClient.listPods(podInfo.getPodNamespace(), labelSelector, NodeRole.ROUTER)).andReturn(
        new DiscoveryRobuxNodeList(
            "v1",
            ImmutableMap.of(
                testNode1.getRobuxNode().getHostAndPortToUse(), testNode1,
                testNode2.getRobuxNode().getHostAndPortToUse(), testNode2
            )
        )
    );
    EasyMock.expect(mockK8sApiClient.watchPods(
        podInfo.getPodNamespace(), labelSelector, "v1", NodeRole.ROUTER)).andReturn(null);
    EasyMock.expect(mockK8sApiClient.listPods(podInfo.getPodNamespace(), labelSelector, NodeRole.ROUTER)).andReturn(
        new DiscoveryRobuxNodeList(
            "v2",
            ImmutableMap.of(
                testNode2.getRobuxNode().getHostAndPortToUse(), testNode2,
                testNode3.getRobuxNode().getHostAndPortToUse(), testNode3
            )
        )
    );
    EasyMock.expect(mockK8sApiClient.watchPods(
        podInfo.getPodNamespace(), labelSelector, "v2", NodeRole.ROUTER)).andReturn(
            new MockWatchResult(Collections.emptyList(), true, false)
    );
    EasyMock.expect(mockK8sApiClient.watchPods(
        podInfo.getPodNamespace(), labelSelector, "v2", NodeRole.ROUTER)).andReturn(
        new MockWatchResult(
            ImmutableList.of(
                  new Watch.Response<>(WatchResult.ADDED, new DiscoveryRobuxNodeAndResourceVersion("v3", testNode4)),
                  new Watch.Response<>(WatchResult.DELETED, new DiscoveryRobuxNodeAndResourceVersion("v4", testNode2))
              ),
            false,
            true
            )
    );
    EasyMock.expect(mockK8sApiClient.watchPods(
        podInfo.getPodNamespace(), labelSelector, "v4", NodeRole.ROUTER)).andReturn(
        new MockWatchResult(
            ImmutableList.of(
                new Watch.Response<>(WatchResult.ADDED, new DiscoveryRobuxNodeAndResourceVersion("v5", testNode5)),
                new Watch.Response<>(WatchResult.DELETED, new DiscoveryRobuxNodeAndResourceVersion("v6", testNode3))
            ),
            false,
            false
        )
    );
    EasyMock.replay(mockK8sApiClient);

    K8sRobuxNodeDiscoveryProvider discoveryProvider = new K8sRobuxNodeDiscoveryProvider(
        podInfo,
        discoveryConfig,
        mockK8sApiClient,
        1
    );
    discoveryProvider.start();

    K8sRobuxNodeDiscoveryProvider.NodeRoleWatcher nodeDiscovery = discoveryProvider.getForNodeRole(NodeRole.ROUTER, false);

    MockListener testListener = new MockListener(
        ImmutableList.of(
            MockListener.Event.added(testNode1),
            MockListener.Event.added(testNode2),
            MockListener.Event.inited(),
            MockListener.Event.added(testNode3),
            MockListener.Event.deleted(testNode1),
            MockListener.Event.added(testNode4),
            MockListener.Event.deleted(testNode2),
            MockListener.Event.added(testNode5),
            MockListener.Event.deleted(testNode3)
        )
    );
    nodeDiscovery.registerListener(testListener);

    nodeDiscovery.start();

    testListener.assertSuccess();

    discoveryProvider.stop();
  }

  @Test(timeout = 10_000)
  public void testNodeRoleWatcherHandlesNullFromAPIByRestarting() throws Exception
  {
    String labelSelector = "robuxDiscoveryAnnouncement-cluster-identifier=robux-cluster,robuxDiscoveryAnnouncement-router=true";
    K8sApiClient mockK8sApiClient = EasyMock.createMock(K8sApiClient.class);
    EasyMock.expect(mockK8sApiClient.listPods(podInfo.getPodNamespace(), labelSelector, NodeRole.ROUTER)).andReturn(
        new DiscoveryRobuxNodeList(
            "v1",
            ImmutableMap.of(
                testNode1.getRobuxNode().getHostAndPortToUse(), testNode1,
                testNode2.getRobuxNode().getHostAndPortToUse(), testNode2
            )
        )
    );
    EasyMock.expect(mockK8sApiClient.watchPods(
        podInfo.getPodNamespace(), labelSelector, "v1", NodeRole.ROUTER)).andReturn(
        new MockWatchResult(
            ImmutableList.of(
                  new Watch.Response<>(WatchResult.ADDED, null)
              ),
            false,
            false
            )
    );
    EasyMock.expect(mockK8sApiClient.listPods(podInfo.getPodNamespace(), labelSelector, NodeRole.ROUTER)).andReturn(
        new DiscoveryRobuxNodeList(
            "v2",
            ImmutableMap.of(
                testNode2.getRobuxNode().getHostAndPortToUse(), testNode2,
                testNode3.getRobuxNode().getHostAndPortToUse(), testNode3
            )
        )
    );
    EasyMock.replay(mockK8sApiClient);

    K8sRobuxNodeDiscoveryProvider discoveryProvider = new K8sRobuxNodeDiscoveryProvider(
        podInfo,
        discoveryConfig,
        mockK8sApiClient,
        1
    );
    discoveryProvider.start();

    K8sRobuxNodeDiscoveryProvider.NodeRoleWatcher nodeDiscovery = discoveryProvider.getForNodeRole(NodeRole.ROUTER, false);

    MockListener testListener = new MockListener(
        ImmutableList.of(
            MockListener.Event.added(testNode1),
            MockListener.Event.added(testNode2),
            MockListener.Event.inited(),
            MockListener.Event.added(testNode3),
            MockListener.Event.deleted(testNode1)
        )
    );
    nodeDiscovery.registerListener(testListener);

    nodeDiscovery.start();

    testListener.assertSuccess();

    discoveryProvider.stop();
  }

  @Test(timeout = 10_000)
  public void testNodeRoleWatcherLoopOnNullItems() throws Exception
  {
    String labelSelector = "robuxDiscoveryAnnouncement-cluster-identifier=robux-cluster,robuxDiscoveryAnnouncement-router=true";
    K8sApiClient mockK8sApiClient = EasyMock.createMock(K8sApiClient.class);
    EasyMock.expect(mockK8sApiClient.listPods(podInfo.getPodNamespace(), labelSelector, NodeRole.ROUTER)).andReturn(
        new DiscoveryRobuxNodeList(
            "v1",
            ImmutableMap.of(
                testNode1.getRobuxNode().getHostAndPortToUse(), testNode1,
                testNode2.getRobuxNode().getHostAndPortToUse(), testNode2
            )
        )
    );
    List<Watch.Response<DiscoveryRobuxNodeAndResourceVersion>> nullList = new ArrayList<>();
    nullList.add(null);
    EasyMock.expect(mockK8sApiClient.watchPods(
        podInfo.getPodNamespace(), labelSelector, "v1", NodeRole.ROUTER)).andReturn(
        new MockWatchResult(
            nullList,
            false,
            false
            )
    );
    EasyMock.expect(mockK8sApiClient.watchPods(
        podInfo.getPodNamespace(), labelSelector, "v1", NodeRole.ROUTER)).andReturn(
        new MockWatchResult(
            ImmutableList.of(
                  new Watch.Response<>(null, new DiscoveryRobuxNodeAndResourceVersion("v2", testNode4))
              ),
            false,
            false
            )
    );
    EasyMock.expect(mockK8sApiClient.watchPods(
        podInfo.getPodNamespace(), labelSelector, "v2", NodeRole.ROUTER)).andReturn(
        new MockWatchResult(
            ImmutableList.of(
                  new Watch.Response<>(WatchResult.ADDED, new DiscoveryRobuxNodeAndResourceVersion("v2", testNode4))
              ),
            false,
            false
            )
    );
    EasyMock.replay(mockK8sApiClient);

    K8sRobuxNodeDiscoveryProvider discoveryProvider = new K8sRobuxNodeDiscoveryProvider(
        podInfo,
        discoveryConfig,
        mockK8sApiClient,
        1
    );
    discoveryProvider.start();

    K8sRobuxNodeDiscoveryProvider.NodeRoleWatcher nodeDiscovery = discoveryProvider.getForNodeRole(NodeRole.ROUTER, false);

    MockListener testListener = new MockListener(
        ImmutableList.of(
            MockListener.Event.added(testNode1),
            MockListener.Event.added(testNode2)
        )
    );
    nodeDiscovery.registerListener(testListener);

    nodeDiscovery.start();

    testListener.assertSuccess();

    discoveryProvider.stop();
  }

  private static class MockListener implements RobuxNodeDiscovery.Listener
  {
    List<Event> events;
    private boolean failed = false;
    private String failReason;

    public MockListener(List<Event> events)
    {
      this.events = Lists.newArrayList(events);
    }

    @Override
    public void nodeViewInitialized()
    {
      assertNextEvent(Event.inited());
    }

    @Override
    public void nodeViewInitializedTimedOut()
    {
      nodeViewInitialized();
    }

    @Override
    public void nodesAdded(Collection<DiscoveryRobuxNode> nodes)
    {
      List<DiscoveryRobuxNode> l = Lists.newArrayList(nodes);
      Collections.sort(l, (n1, n2) -> n1.getRobuxNode().getHostAndPortToUse().compareTo(n2.getRobuxNode().getHostAndPortToUse()));

      for (DiscoveryRobuxNode node : l) {
        assertNextEvent(Event.added(node));
      }
    }

    @Override
    public void nodesRemoved(Collection<DiscoveryRobuxNode> nodes)
    {
      List<DiscoveryRobuxNode> l = Lists.newArrayList(nodes);
      Collections.sort(l, (n1, n2) -> n1.getRobuxNode().getHostAndPortToUse().compareTo(n2.getRobuxNode().getHostAndPortToUse()));

      for (DiscoveryRobuxNode node : l) {
        assertNextEvent(Event.deleted(node));
      }
    }

    private void assertNextEvent(Event actual)
    {
      if (!failed && !events.isEmpty()) {
        Event expected = events.remove(0);
        failed = !actual.equals(expected);
        if (failed) {
          failReason = StringUtils.format("Failed Equals [%s] and [%s]", expected, actual);
        }
      }
    }

    public void assertSuccess() throws Exception
    {
      while (!events.isEmpty()) {
        Assert.assertFalse(failReason, failed);
        LOGGER.info("Waiting  for events to finish.");
        Thread.sleep(1000);
      }

      Assert.assertFalse(failReason, failed);
    }

    static class Event
    {
      String type;
      DiscoveryRobuxNode node;

      private Event(String type, DiscoveryRobuxNode node)
      {
        this.type = type;
        this.node = node;
      }

      static Event inited()
      {
        return new Event("inited", null);
      }

      static Event added(DiscoveryRobuxNode node)
      {
        return new Event("added", node);
      }

      static Event deleted(DiscoveryRobuxNode node)
      {
        return new Event("deleted", node);
      }

      @Override
      public boolean equals(Object o)
      {
        if (this == o) {
          return true;
        }
        if (o == null || getClass() != o.getClass()) {
          return false;
        }
        Event event = (Event) o;
        return type.equals(event.type) &&
               Objects.equals(node, event.node);
      }

      @Override
      public int hashCode()
      {
        return Objects.hash(type, node);
      }

      @Override
      public String toString()
      {
        return "Event{" +
               "type='" + type + '\'' +
               ", node=" + node +
               '}';
      }
    }
  }

  private static class MockWatchResult implements WatchResult
  {
    private List<Watch.Response<DiscoveryRobuxNodeAndResourceVersion>> results;

    private volatile boolean timeoutOnStart;
    private volatile boolean timeooutOnEmptyResults;
    private volatile boolean closeCalled = false;

    public MockWatchResult(
        List<Watch.Response<DiscoveryRobuxNodeAndResourceVersion>> results,
        boolean timeoutOnStart,
        boolean timeooutOnEmptyResults
    )
    {
      this.results = Lists.newArrayList(results);
      this.timeoutOnStart = timeoutOnStart;
      this.timeooutOnEmptyResults = timeooutOnEmptyResults;
    }

    @Override
    public boolean hasNext() throws SocketTimeoutException
    {
      if (timeoutOnStart) {
        throw new SocketTimeoutException("testing timeout on start!!!");
      }

      if (results.isEmpty()) {
        if (timeooutOnEmptyResults) {
          throw new SocketTimeoutException("testing timeout on end!!!");
        } else {
          try {
            Thread.sleep(Long.MAX_VALUE);
            return false; // just making compiler happy, will never reach this.
          }
          catch (InterruptedException ex) {
            throw new RuntimeException(ex);
          }
        }
      } else {
        return true;
      }
    }

    @Override
    public Watch.Response<DiscoveryRobuxNodeAndResourceVersion> next()
    {
      return results.remove(0);
    }

    @Override
    public void close()
    {
      closeCalled = true;
    }

    public void assertSuccess()
    {
      Assert.assertTrue("close() not called", closeCalled);
    }
  }
}
