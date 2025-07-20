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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.robux.java.util.common.IAE;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.server.RobuxNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BooleanSupplier;

/**
 * Provider of {@link RobuxNodeDiscovery} instances.
 */
public abstract class RobuxNodeDiscoveryProvider
{
  private static final Map<String, Set<NodeRole>> SERVICE_TO_NODE_TYPES = ImmutableMap.of(
      LookupNodeService.DISCOVERY_SERVICE_KEY,
      ImmutableSet.of(NodeRole.BROKER, NodeRole.HISTORICAL, NodeRole.PEON, NodeRole.INDEXER),
      DataNodeService.DISCOVERY_SERVICE_KEY,
      ImmutableSet.of(NodeRole.HISTORICAL, NodeRole.PEON, NodeRole.INDEXER, NodeRole.BROKER),
      WorkerNodeService.DISCOVERY_SERVICE_KEY,
      ImmutableSet.of(NodeRole.MIDDLE_MANAGER, NodeRole.INDEXER)
  );

  private final ConcurrentHashMap<String, ServiceRobuxNodeDiscovery> serviceDiscoveryMap =
      new ConcurrentHashMap<>(SERVICE_TO_NODE_TYPES.size());

  public abstract BooleanSupplier getForNode(RobuxNode node, NodeRole nodeRole);

  /** Get a {@link RobuxNodeDiscovery} instance to discover nodes of the given node role. */
  public abstract RobuxNodeDiscovery getForNodeRole(NodeRole nodeRole);

  /**
   * Get RobuxNodeDiscovery instance to discover nodes that announce given service in its metadata.
   */
  public RobuxNodeDiscovery getForService(String serviceName)
  {
    return serviceDiscoveryMap.computeIfAbsent(
        serviceName,
        service -> {

          Set<NodeRole> nodeRolesToWatch = RobuxNodeDiscoveryProvider.SERVICE_TO_NODE_TYPES.get(service);
          if (nodeRolesToWatch == null) {
            throw new IAE("Unknown service [%s].", service);
          }
          ServiceRobuxNodeDiscovery serviceDiscovery = new ServiceRobuxNodeDiscovery(service, nodeRolesToWatch.size());
          RobuxNodeDiscovery.Listener filteringGatheringUpstreamListener =
              serviceDiscovery.filteringUpstreamListener();
          for (NodeRole nodeRole : nodeRolesToWatch) {
            getForNodeRole(nodeRole).registerListener(filteringGatheringUpstreamListener);
          }
          return serviceDiscovery;
        }
    );
  }

  private static class ServiceRobuxNodeDiscovery implements RobuxNodeDiscovery
  {
    private static final Logger log = new Logger(ServiceRobuxNodeDiscovery.class);

    private final String service;
    private final ConcurrentMap<String, DiscoveryRobuxNode> nodes = new ConcurrentHashMap<>();
    private final Collection<DiscoveryRobuxNode> unmodifiableNodes = Collections.unmodifiableCollection(nodes.values());

    private final List<Listener> listeners = new ArrayList<>();

    private final Object lock = new Object();

    private int uninitializedNodeRoles;

    ServiceRobuxNodeDiscovery(String service, int watchedNodeRoles)
    {
      Preconditions.checkArgument(watchedNodeRoles > 0);
      this.service = service;
      this.uninitializedNodeRoles = watchedNodeRoles;
    }

    @Override
    public Collection<DiscoveryRobuxNode> getAllNodes()
    {
      return unmodifiableNodes;
    }

    @Override
    public void registerListener(Listener listener)
    {
      if (listener instanceof FilteringUpstreamListener) {
        throw new IAE("FilteringUpstreamListener should not be registered with ServiceRobuxNodeDiscovery itself");
      }
      synchronized (lock) {
        if (!unmodifiableNodes.isEmpty()) {
          listener.nodesAdded(unmodifiableNodes);
        }
        if (uninitializedNodeRoles == 0) {
          listener.nodeViewInitialized();
        }
        listeners.add(listener);
      }
    }

    @Override
    public void removeListener(Listener listener)
    {
      synchronized (lock) {
        if (listener != null) {
          listeners.remove(listener);
        }
      }
    }

    RobuxNodeDiscovery.Listener filteringUpstreamListener()
    {
      return new FilteringUpstreamListener();
    }

    /**
     * Listens for all node updates and filters them based on {@link #service}. Note: this listener is registered with
     * the objects returned from {@link #getForNodeRole(NodeRole)}, NOT with {@link ServiceRobuxNodeDiscovery} itself.
     */
    class FilteringUpstreamListener implements RobuxNodeDiscovery.Listener
    {
      @Override
      public void nodesAdded(Collection<DiscoveryRobuxNode> nodesDiscovered)
      {
        synchronized (lock) {
          List<DiscoveryRobuxNode> nodesAdded = new ArrayList<>();
          for (DiscoveryRobuxNode node : nodesDiscovered) {
            if (node.getServices().containsKey(service)) {
              DiscoveryRobuxNode prev = nodes.putIfAbsent(node.getRobuxNode().getHostAndPortToUse(), node);

              if (prev == null) {
                nodesAdded.add(node);
              } else {
                log.warn("Node[%s] discovered but already exists [%s].", node, prev);
              }
            } else {
              log.warn("Node[%s] discovered but doesn't have service[%s]. Ignored.", node, service);
            }
          }

          if (nodesAdded.isEmpty()) {
            // Don't bother listeners with an empty update, it doesn't make sense.
            return;
          }

          Collection<DiscoveryRobuxNode> unmodifiableNodesAdded = Collections.unmodifiableCollection(nodesAdded);
          for (Listener listener : listeners) {
            try {
              listener.nodesAdded(unmodifiableNodesAdded);
            }
            catch (Exception ex) {
              log.error(ex, "Listener[%s].nodesAdded(%s) threw exception. Ignored.", listener, nodesAdded);
            }
          }
        }
      }

      @Override
      public void nodesRemoved(Collection<DiscoveryRobuxNode> nodesDisappeared)
      {
        synchronized (lock) {
          List<DiscoveryRobuxNode> nodesRemoved = new ArrayList<>();
          for (DiscoveryRobuxNode node : nodesDisappeared) {
            DiscoveryRobuxNode prev = nodes.remove(node.getRobuxNode().getHostAndPortToUse());
            if (prev != null) {
              nodesRemoved.add(node);
            } else {
              log.warn("Node[%s] disappeared but was unknown for service listener [%s].", node, service);
            }
          }

          if (nodesRemoved.isEmpty()) {
            // Don't bother listeners with an empty update, it doesn't make sense.
            return;
          }

          Collection<DiscoveryRobuxNode> unmodifiableNodesRemoved = Collections.unmodifiableCollection(nodesRemoved);
          for (Listener listener : listeners) {
            try {
              listener.nodesRemoved(unmodifiableNodesRemoved);
            }
            catch (Exception ex) {
              log.error(ex, "Listener[%s].nodesRemoved(%s) threw exception. Ignored.", listener, nodesRemoved);
            }
          }
        }
      }

      @Override
      public void nodeViewInitialized()
      {
        nodeViewInitialized(false);
      }

      @Override
      public void nodeViewInitializedTimedOut()
      {
        nodeViewInitialized(true);
      }

      private void nodeViewInitialized(final boolean timedOut)
      {
        synchronized (lock) {
          if (uninitializedNodeRoles == 0) {
            log.error("Unexpected call of nodeViewInitialized(timedOut = %s)", timedOut);
            return;
          }
          uninitializedNodeRoles--;
          if (uninitializedNodeRoles == 0) {
            for (Listener listener : listeners) {
              try {
                if (timedOut) {
                  listener.nodeViewInitializedTimedOut();
                } else {
                  listener.nodeViewInitialized();
                }
              }
              catch (Exception ex) {
                log.error(ex, "Listener[%s].nodeViewInitialized(%s) threw exception. Ignored.", listener, timedOut);
              }
            }
          }
        }
      }
    }
  }
}
