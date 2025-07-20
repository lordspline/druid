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

package org.apache.robux.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.robux.discovery.DataNodeService;
import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.discovery.RobuxNodeDiscovery;
import org.apache.robux.discovery.RobuxNodeDiscoveryProvider;
import org.apache.robux.discovery.LookupNodeService;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.error.InvalidInput;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.java.util.common.concurrent.Execs;
import org.apache.robux.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.robux.java.util.common.granularity.Granularities;
import org.apache.robux.java.util.emitter.EmittingLogger;
import org.apache.robux.java.util.emitter.service.AlertEvent;
import org.apache.robux.java.util.metrics.StubServiceEmitter;
import org.apache.robux.segment.TestHelper;
import org.apache.robux.segment.realtime.appenderator.SegmentSchemas;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.coordination.ChangeRequestHistory;
import org.apache.robux.server.coordination.ChangeRequestsSnapshot;
import org.apache.robux.server.coordination.DataSegmentChangeRequest;
import org.apache.robux.server.coordination.RobuxServerMetadata;
import org.apache.robux.server.coordination.SegmentChangeRequestDrop;
import org.apache.robux.server.coordination.SegmentChangeRequestLoad;
import org.apache.robux.server.coordination.ServerType;
import org.apache.robux.server.coordinator.CreateDataSegments;
import org.apache.robux.server.coordinator.simulate.BlockingExecutorService;
import org.apache.robux.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.robux.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class HttpServerInventoryViewTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();
  private static final TypeReference<ChangeRequestsSnapshot<DataSegmentChangeRequest>>
      TYPE_REF = HttpServerInventoryView.SEGMENT_LIST_RESP_TYPE_REF;

  private static final String EXEC_NAME_PREFIX = "InventoryViewTest";

  private static final String METRIC_SUCCESS = "serverview/sync/healthy";
  private static final String METRIC_UNSTABLE_TIME = "serverview/sync/unstableTime";

  private StubServiceEmitter serviceEmitter;

  private HttpServerInventoryView httpServerInventoryView;
  private TestChangeRequestHttpClient<ChangeRequestsSnapshot<DataSegmentChangeRequest>> httpClient;
  private TestExecutorFactory execHelper;

  private TestRobuxNodeDiscovery robuxNodeDiscovery;
  private RobuxNodeDiscoveryProvider robuxNodeDiscoveryProvider;

  private Map<RobuxServerMetadata, Set<DataSegment>> segmentsAddedToView;
  private Map<RobuxServerMetadata, Set<DataSegment>> segmentsRemovedFromView;
  private Set<RobuxServerMetadata> addedServers;
  private Set<RobuxServerMetadata> removedServers;

  private AtomicBoolean inventoryInitialized;

  @Before
  public void setup()
  {
    serviceEmitter = new StubServiceEmitter("test", "localhost");
    EmittingLogger.registerEmitter(serviceEmitter);

    robuxNodeDiscovery = new TestRobuxNodeDiscovery();
    robuxNodeDiscoveryProvider = EasyMock.createMock(RobuxNodeDiscoveryProvider.class);
    EasyMock.expect(robuxNodeDiscoveryProvider.getForService(DataNodeService.DISCOVERY_SERVICE_KEY))
            .andReturn(robuxNodeDiscovery);
    EasyMock.replay(robuxNodeDiscoveryProvider);

    httpClient = new TestChangeRequestHttpClient<>(TYPE_REF, MAPPER);
    execHelper = new TestExecutorFactory();
    inventoryInitialized = new AtomicBoolean(false);

    segmentsAddedToView = new HashMap<>();
    segmentsRemovedFromView = new HashMap<>();
    addedServers = new HashSet<>();
    removedServers = new HashSet<>();

    createInventoryView(
        new HttpServerInventoryViewConfig(null, null, null)
    );
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(robuxNodeDiscoveryProvider);
    if (httpServerInventoryView != null && httpServerInventoryView.isStarted()) {
      httpServerInventoryView.stop();
    }
  }

  @Test
  public void testInitHappensAfterNodeViewInit()
  {
    httpServerInventoryView.start();
    Assert.assertTrue(httpServerInventoryView.isStarted());
    Assert.assertFalse(inventoryInitialized.get());

    robuxNodeDiscovery.markNodeViewInitialized();
    Assert.assertFalse(inventoryInitialized.get());

    execHelper.finishInventoryInitialization();
    Assert.assertTrue(inventoryInitialized.get());

    httpServerInventoryView.stop();
  }

  @Test
  public void testStopShutsDownExecutors()
  {
    httpServerInventoryView.start();
    Assert.assertFalse(execHelper.syncExecutor.isShutdown());

    httpServerInventoryView.stop();
    Assert.assertTrue(execHelper.syncExecutor.isShutdown());
  }

  @Test
  public void testAddNodeStartsSync()
  {
    httpServerInventoryView.start();
    robuxNodeDiscovery.markNodeViewInitialized();
    execHelper.finishInventoryInitialization();

    final DiscoveryRobuxNode robuxNode = robuxNodeDiscovery
        .addNodeAndNotifyListeners("localhost");
    final RobuxServer server = robuxNode.toRobuxServer();

    Collection<RobuxServer> inventory = httpServerInventoryView.getInventory();
    Assert.assertEquals(1, inventory.size());
    Assert.assertTrue(inventory.contains(server));
    Assert.assertTrue(addedServers.contains(server.getMetadata()));

    execHelper.emitMetrics();
    serviceEmitter.verifyValue(METRIC_SUCCESS, 1);
    serviceEmitter.verifyNotEmitted(METRIC_UNSTABLE_TIME);

    DataSegment segment = CreateDataSegments.ofDatasource("wiki").eachOfSizeInMb(500).get(0);
    httpClient.completeNextRequestWith(
        snapshotOf(new SegmentChangeRequestLoad(segment))
    );
    execHelper.sendSyncRequestAndHandleResponse();

    RobuxServer inventoryValue = httpServerInventoryView.getInventoryValue(server.getName());
    Assert.assertNotNull(inventoryValue);
    Assert.assertEquals(1, inventoryValue.getTotalSegments());
    Assert.assertNotNull(inventoryValue.getSegment(segment.getId()));

    httpServerInventoryView.stop();
  }

  @Test
  public void testRemoveNodeStopsSync()
  {
    httpServerInventoryView.start();
    robuxNodeDiscovery.markNodeViewInitialized();
    execHelper.finishInventoryInitialization();

    final DiscoveryRobuxNode robuxNode = robuxNodeDiscovery
        .addNodeAndNotifyListeners("localhost");
    final RobuxServer server = robuxNode.toRobuxServer();

    robuxNodeDiscovery.removeNodesAndNotifyListeners(robuxNode);

    Assert.assertNull(httpServerInventoryView.getInventoryValue(server.getName()));

    execHelper.emitMetrics();
    serviceEmitter.verifyNotEmitted(METRIC_SUCCESS);
    serviceEmitter.verifyNotEmitted(METRIC_UNSTABLE_TIME);

    httpServerInventoryView.stop();
  }

  @Test
  public void testAddNodeTriggersServerAddedCallback()
  {
    httpServerInventoryView.start();
    robuxNodeDiscovery.markNodeViewInitialized();
    execHelper.finishInventoryInitialization();

    final DiscoveryRobuxNode robuxNode = robuxNodeDiscovery
        .addNodeAndNotifyListeners("localhost");
    final RobuxServer server = robuxNode.toRobuxServer();

    Assert.assertTrue(addedServers.contains(server.getMetadata()));

    httpServerInventoryView.stop();
  }

  @Test(timeout = 60_000L)
  public void testSyncSegmentLoadAndDrop()
  {
    httpServerInventoryView.start();
    robuxNodeDiscovery.markNodeViewInitialized();
    execHelper.finishInventoryInitialization();

    final DiscoveryRobuxNode robuxNode = robuxNodeDiscovery
        .addNodeAndNotifyListeners("localhost");
    final RobuxServer server = robuxNode.toRobuxServer();

    Assert.assertTrue(addedServers.contains(server.getMetadata()));

    final DataSegment[] segments =
        CreateDataSegments.ofDatasource("wiki")
                          .forIntervals(4, Granularities.DAY)
                          .eachOfSizeInMb(500)
                          .toArray(new DataSegment[0]);

    // Request 1: Load S1
    httpClient.completeNextRequestWith(
        snapshotOf(new SegmentChangeRequestLoad(segments[0]))
    );
    execHelper.sendSyncRequestAndHandleResponse();
    Assert.assertTrue(isAddedToView(server, segments[0]));

    // Request 2: Drop S1, Load S2, S3
    resetForNextSyncRequest();
    httpClient.completeNextRequestWith(
        snapshotOf(
            new SegmentChangeRequestDrop(segments[0]),
            new SegmentChangeRequestLoad(segments[1]),
            new SegmentChangeRequestLoad(segments[2])
        )
    );
    execHelper.sendSyncRequestAndHandleResponse();
    Assert.assertTrue(isRemovedFromView(server, segments[0]));
    Assert.assertTrue(isAddedToView(server, segments[1]));
    Assert.assertTrue(isAddedToView(server, segments[2]));

    // Request 3: reset the counter
    resetForNextSyncRequest();
    httpClient.completeNextRequestWith(
        new ChangeRequestsSnapshot<>(
            true,
            "Server requested reset",
            ChangeRequestHistory.Counter.ZERO,
            Collections.emptyList()
        )
    );
    execHelper.sendSyncRequestAndHandleResponse();
    Assert.assertTrue(segmentsAddedToView.isEmpty());
    Assert.assertTrue(segmentsRemovedFromView.isEmpty());

    // Request 4: Load S3, S4
    resetForNextSyncRequest();
    httpClient.completeNextRequestWith(
        snapshotOf(
            new SegmentChangeRequestLoad(segments[2]),
            new SegmentChangeRequestLoad(segments[3])
        )
    );
    execHelper.sendSyncRequestAndHandleResponse();
    Assert.assertTrue(isRemovedFromView(server, segments[1]));
    Assert.assertTrue(isAddedToView(server, segments[3]));

    RobuxServer inventoryValue = httpServerInventoryView.getInventoryValue(server.getName());
    Assert.assertNotNull(inventoryValue);
    Assert.assertEquals(2, inventoryValue.getTotalSegments());
    Assert.assertNotNull(inventoryValue.getSegment(segments[2].getId()));
    Assert.assertNotNull(inventoryValue.getSegment(segments[3].getId()));

    // Verify node removal
    robuxNodeDiscovery.removeNodesAndNotifyListeners(robuxNode);

    // test removal event with empty services
    robuxNodeDiscovery.removeNodesAndNotifyListeners(
        new DiscoveryRobuxNode(
            new RobuxNode("service", "host", false, 8080, null, true, false),
            NodeRole.INDEXER,
            Collections.emptyMap()
        )
    );

    // test removal rogue node (announced a service as a DataNodeService but wasn't a DataNodeService at the key)
    robuxNodeDiscovery.removeNodesAndNotifyListeners(
        new DiscoveryRobuxNode(
            new RobuxNode("service", "host", false, 8080, null, true, false),
            NodeRole.INDEXER,
            ImmutableMap.of(
                DataNodeService.DISCOVERY_SERVICE_KEY,
                new LookupNodeService("lookyloo")
            )
        )
    );

    Assert.assertTrue(removedServers.contains(server.getMetadata()));
    Assert.assertNull(httpServerInventoryView.getInventoryValue(server.getName()));

    httpServerInventoryView.stop();
  }

  @Test
  public void testSyncWhenRequestFailedToSend()
  {
    httpServerInventoryView.start();
    robuxNodeDiscovery.markNodeViewInitialized();
    execHelper.finishInventoryInitialization();

    final DiscoveryRobuxNode robuxNode = robuxNodeDiscovery.addNodeAndNotifyListeners("localhost");
    Assert.assertTrue(addedServers.contains(robuxNode.toRobuxServer().getMetadata()));

    httpClient.failToSendNextRequestWith(new ISE("Could not send request to server"));
    execHelper.sendSyncRequest();

    serviceEmitter.flush();
    execHelper.emitMetrics();
    serviceEmitter.verifyValue(METRIC_SUCCESS, 0);

    httpServerInventoryView.stop();
  }

  @Test
  public void testSyncWhenErrorResponse()
  {
    httpServerInventoryView.start();
    robuxNodeDiscovery.markNodeViewInitialized();
    execHelper.finishInventoryInitialization();

    robuxNodeDiscovery.addNodeAndNotifyListeners("localhost");

    httpClient.completeNextRequestWith(InvalidInput.exception("failure on server"));
    execHelper.sendSyncRequestAndHandleResponse();

    serviceEmitter.flush();
    execHelper.emitMetrics();
    serviceEmitter.verifyValue(METRIC_SUCCESS, 0);

    httpServerInventoryView.stop();
  }

  @Test
  public void testUnstableServerAlertsAfterTimeout()
  {
    // Create inventory with alert timeout as 0 ms
    createInventoryView(
        new HttpServerInventoryViewConfig(null, Period.millis(0), null)
    );

    httpServerInventoryView.start();
    robuxNodeDiscovery.markNodeViewInitialized();
    execHelper.finishInventoryInitialization();

    final DiscoveryRobuxNode robuxNode = robuxNodeDiscovery.addNodeAndNotifyListeners("localhost");
    Assert.assertTrue(addedServers.contains(robuxNode.toRobuxServer().getMetadata()));

    serviceEmitter.flush();
    httpClient.completeNextRequestWith(InvalidInput.exception("failure on server"));
    execHelper.sendSyncRequestAndHandleResponse();

    List<AlertEvent> alerts = serviceEmitter.getAlerts();
    Assert.assertEquals(1, alerts.size());
    AlertEvent alert = alerts.get(0);
    Assert.assertTrue(alert.getDescription().contains("Sync failed for server"));

    serviceEmitter.flush();
    execHelper.emitMetrics();
    serviceEmitter.verifyValue(METRIC_SUCCESS, 0);

    httpServerInventoryView.stop();
  }

  @Test(timeout = 60_000)
  public void testInitWaitsForServerToSync()
  {
    httpServerInventoryView.start();
    robuxNodeDiscovery.markNodeViewInitialized();
    final DiscoveryRobuxNode robuxNode = robuxNodeDiscovery.addNodeAndNotifyListeners("localhost");
    final RobuxServer server = robuxNode.toRobuxServer();

    Assert.assertTrue(addedServers.contains(server.getMetadata()));

    ExecutorService initExecutor = Execs.singleThreaded(EXEC_NAME_PREFIX + "-init");

    try {
      initExecutor.submit(() -> execHelper.finishInventoryInitialization());

      // Wait to ensure that init thread is in progress and waiting
      Thread.sleep(1000);
      Assert.assertFalse(inventoryInitialized.get());

      // Finish sync of server
      httpClient.completeNextRequestWith(snapshotOf());
      execHelper.sendSyncRequestAndHandleResponse();

      // Wait for 10 seconds to ensure that init thread knows about server sync
      Thread.sleep(10_000);
      Assert.assertTrue(inventoryInitialized.get());
    }
    catch (InterruptedException e) {
      throw new ISE(e, "Interrupted");
    }
    finally {
      initExecutor.shutdownNow();
    }
  }

  @Test(timeout = 60_000)
  public void testInitDoesNotWaitForRemovedServerToSync()
  {
    httpServerInventoryView.start();
    robuxNodeDiscovery.markNodeViewInitialized();
    DiscoveryRobuxNode node = robuxNodeDiscovery.addNodeAndNotifyListeners("localhost");
    Assert.assertTrue(addedServers.contains(node.toRobuxServer().getMetadata()));

    ExecutorService initExecutor = Execs.singleThreaded(EXEC_NAME_PREFIX + "-init");

    try {
      initExecutor.submit(() -> execHelper.finishInventoryInitialization());

      // Wait to ensure that init thread is in progress and waiting
      Thread.sleep(1000);
      Assert.assertFalse(inventoryInitialized.get());

      // Remove the node from discovery
      robuxNodeDiscovery.removeNodesAndNotifyListeners(node);

      // Wait for 10 seconds to ensure that init thread knows about server removal
      Thread.sleep(10_000);
      Assert.assertTrue(inventoryInitialized.get());
    }
    catch (InterruptedException e) {
      throw new ISE(e, "Interrupted");
    }
    finally {
      initExecutor.shutdownNow();
    }
  }

  private void createInventoryView(HttpServerInventoryViewConfig config)
  {
    httpServerInventoryView = new HttpServerInventoryView(
        MAPPER,
        httpClient,
        robuxNodeDiscoveryProvider,
        pair -> !pair.rhs.getDataSource().equals("non-loading-datasource"),
        config,
        serviceEmitter,
        execHelper,
        EXEC_NAME_PREFIX
    );

    httpServerInventoryView.registerSegmentCallback(
        Execs.directExecutor(),
        new ServerView.SegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(RobuxServerMetadata server, DataSegment segment)
          {
            segmentsAddedToView.computeIfAbsent(server, s -> new HashSet<>()).add(segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(RobuxServerMetadata server, DataSegment segment)
          {
            segmentsRemovedFromView.computeIfAbsent(server, s -> new HashSet<>()).add(segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentViewInitialized()
          {
            inventoryInitialized.set(true);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentSchemasAnnounced(SegmentSchemas segmentSchemas)
          {
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );

    httpServerInventoryView.registerServerCallback(
        Execs.directExecutor(),
        new ServerView.ServerCallback() {
          @Override
          public ServerView.CallbackAction serverAdded(RobuxServer server)
          {
            addedServers.add(server.getMetadata());
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction serverRemoved(RobuxServer server)
          {
            removedServers.add(server.getMetadata());
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  private boolean isAddedToView(RobuxServer server, DataSegment segment)
  {
    return segmentsAddedToView.getOrDefault(server.getMetadata(), Collections.emptySet())
                              .contains(segment);
  }

  private boolean isRemovedFromView(RobuxServer server, DataSegment segment)
  {
    return segmentsRemovedFromView.getOrDefault(server.getMetadata(), Collections.emptySet())
                                  .contains(segment);
  }

  private void resetForNextSyncRequest()
  {
    segmentsAddedToView.clear();
    segmentsRemovedFromView.clear();
  }

  private static ChangeRequestsSnapshot<DataSegmentChangeRequest> snapshotOf(
      DataSegmentChangeRequest... requests
  )
  {
    return ChangeRequestsSnapshot.success(
        ChangeRequestHistory.Counter.ZERO,
        Arrays.asList(requests)
    );
  }

  private static class TestRobuxNodeDiscovery implements RobuxNodeDiscovery
  {
    Listener listener;

    @Override
    public Collection<DiscoveryRobuxNode> getAllNodes()
    {
      throw new UnsupportedOperationException("Not Implemented.");
    }

    @Override
    public void registerListener(Listener listener)
    {
      this.listener = listener;
    }

    /**
     * Marks the node view as initialized and notifies the listeners.
     */
    void markNodeViewInitialized()
    {
      listener.nodeViewInitialized();
    }

    /**
     * Creates and adds a new node and notifies the listeners.
     */
    DiscoveryRobuxNode addNodeAndNotifyListeners(String host)
    {
      final RobuxNode robuxNode = new RobuxNode("robux/historical", host, false, 8080, null, true, false);
      DataNodeService dataNodeService = new DataNodeService("tier", 10L << 30, ServerType.HISTORICAL, 0);
      final DiscoveryRobuxNode discoveryRobuxNode = new DiscoveryRobuxNode(
          robuxNode,
          NodeRole.HISTORICAL,
          ImmutableMap.of(DataNodeService.DISCOVERY_SERVICE_KEY, dataNodeService)
      );
      listener.nodesAdded(ImmutableList.of(discoveryRobuxNode));

      return discoveryRobuxNode;
    }

    void removeNodesAndNotifyListeners(DiscoveryRobuxNode... nodesToRemove)
    {
      listener.nodesRemoved(Arrays.asList(nodesToRemove));
    }
  }

  /**
   * Creates and retains a handle on the executors used by the inventory view.
   * <p>
   * There are 4 types of tasks submitted to the two executors. Upon succesful
   * completion, each of these tasks add another task to the execution queue.
   * <p>
   * Tasks running on sync executor:
   * <ol>
   *   <li>send request to server (adds "handle response" to queue)</li>
   *   <li>handle response and execute callbacks (adds "send request" to queue)</li>
   * </ol>
   * <p>
   * Tasks running on monitoring executor.
   * <ol>
   * <li>check and reset unhealthy servers (adds self to queue)</li>
   * <li>emit metrics (adds self to queue)</li>
   * </ol>
   */
  private static class TestExecutorFactory implements ScheduledExecutorFactory
  {
    private BlockingExecutorService syncExecutor;
    private BlockingExecutorService monitorExecutor;

    @Override
    public ScheduledExecutorService create(int corePoolSize, String nameFormat)
    {
      BlockingExecutorService executorService = new BlockingExecutorService(nameFormat);
      final String syncExecutorPrefix = EXEC_NAME_PREFIX + "-%s";
      final String monitorExecutorPrefix = EXEC_NAME_PREFIX + "-monitor-%s";
      if (syncExecutorPrefix.equals(nameFormat)) {
        syncExecutor = executorService;
      } else if (monitorExecutorPrefix.equals(nameFormat)) {
        monitorExecutor = executorService;
      }

      return new WrappingScheduledExecutorService(nameFormat, executorService, false);
    }

    void sendSyncRequestAndHandleResponse()
    {
      syncExecutor.finishNextPendingTasks(2);
    }

    void sendSyncRequest()
    {
      syncExecutor.finishNextPendingTask();
    }

    void finishInventoryInitialization()
    {
      syncExecutor.finishNextPendingTask();
    }

    void emitMetrics()
    {
      // Finish 1 task for check and reset, 1 for metric emission
      monitorExecutor.finishNextPendingTasks(2);
    }
  }
}
