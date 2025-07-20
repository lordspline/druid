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

package org.apache.robux.msq.dart.controller.sql;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.discovery.RobuxNodeDiscovery;
import org.apache.robux.discovery.RobuxNodeDiscoveryProvider;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.ManageLifecycle;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.java.util.common.lifecycle.LifecycleStart;
import org.apache.robux.java.util.common.lifecycle.LifecycleStop;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.sql.http.SqlResource;

import javax.servlet.http.HttpServletRequest;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Keeps {@link DartSqlClient} for all servers except ourselves. Currently the purpose of this is to power
 * the "get all queries" API at {@link SqlResource#doGetRunningQueries(String, String, HttpServletRequest)}.
 */
@ManageLifecycle
public class DartSqlClients implements RobuxNodeDiscovery.Listener
{
  @GuardedBy("clients")
  private final Map<RobuxNode, DartSqlClient> clients = new HashMap<>();
  private final RobuxNode selfNode;
  private final RobuxNodeDiscoveryProvider discoveryProvider;
  private final DartSqlClientFactory clientFactory;

  private volatile RobuxNodeDiscovery discovery;

  @Inject
  public DartSqlClients(
      @Self RobuxNode selfNode,
      RobuxNodeDiscoveryProvider discoveryProvider,
      DartSqlClientFactory clientFactory
  )
  {
    this.selfNode = selfNode;
    this.discoveryProvider = discoveryProvider;
    this.clientFactory = clientFactory;
  }

  @LifecycleStart
  public void start()
  {
    discovery = discoveryProvider.getForNodeRole(NodeRole.BROKER);
    discovery.registerListener(this);
  }

  public List<DartSqlClient> getAllClients()
  {
    synchronized (clients) {
      return ImmutableList.copyOf(clients.values());
    }
  }

  @Override
  public void nodesAdded(final Collection<DiscoveryRobuxNode> nodes)
  {
    synchronized (clients) {
      for (final DiscoveryRobuxNode node : nodes) {
        final RobuxNode robuxNode = node.getRobuxNode();
        if (!selfNode.equals(robuxNode)) {
          clients.computeIfAbsent(robuxNode, clientFactory::makeClient);
        }
      }
    }
  }

  @Override
  public void nodesRemoved(final Collection<DiscoveryRobuxNode> nodes)
  {
    synchronized (clients) {
      for (final DiscoveryRobuxNode node : nodes) {
        clients.remove(node.getRobuxNode());
      }
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (discovery != null) {
      discovery.removeListener(this);
      discovery = null;
    }

    synchronized (clients) {
      clients.clear();
    }
  }
}
