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

package org.apache.robux.messages.client;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.discovery.RobuxNodeDiscovery;
import org.apache.robux.discovery.RobuxNodeDiscoveryProvider;
import org.apache.robux.guice.ManageLifecycle;
import org.apache.robux.java.util.common.Pair;
import org.apache.robux.java.util.common.lifecycle.LifecycleStart;
import org.apache.robux.java.util.common.lifecycle.LifecycleStop;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.utils.CloseableUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Manages a fleet of {@link MessageRelay}, one for each server discovered by a {@link RobuxNodeDiscoveryProvider}.
 */
@ManageLifecycle
public class MessageRelays<MessageType>
{
  private static final Logger log = new Logger(MessageRelays.class);

  @GuardedBy("serverRelays")
  private final Map<String, MessageRelay<MessageType>> serverRelays = new HashMap<>();
  private final Supplier<RobuxNodeDiscovery> discoverySupplier;
  private final MessageRelayFactory<MessageType> messageRelayFactory;
  private final MessageRelaysListener listener;

  private volatile RobuxNodeDiscovery discovery;

  public MessageRelays(
      final Supplier<RobuxNodeDiscovery> discoverySupplier,
      final MessageRelayFactory<MessageType> messageRelayFactory
  )
  {
    this.discoverySupplier = discoverySupplier;
    this.messageRelayFactory = messageRelayFactory;
    this.listener = new MessageRelaysListener();
  }

  @LifecycleStart
  public void start()
  {
    discovery = discoverySupplier.get();
    discovery.registerListener(listener);
  }

  @LifecycleStop
  public void stop()
  {
    if (discovery != null) {
      discovery.removeListener(listener);
      discovery = null;
    }

    synchronized (serverRelays) {
      try {
        CloseableUtils.closeAll(serverRelays.values());
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      finally {
        serverRelays.clear();
      }
    }
  }

  /**
   * Discovery listener. Creates and tears down individual host relays.
   */
  class MessageRelaysListener implements RobuxNodeDiscovery.Listener
  {
    @Override
    public void nodesAdded(final Collection<DiscoveryRobuxNode> nodes)
    {
      synchronized (serverRelays) {
        for (final DiscoveryRobuxNode node : nodes) {
          final RobuxNode robuxNode = node.getRobuxNode();

          serverRelays.computeIfAbsent(robuxNode.getHostAndPortToUse(), ignored -> {
            final MessageRelay<MessageType> relay = messageRelayFactory.newRelay(robuxNode);
            relay.start();
            return relay;
          });
        }
      }
    }

    @Override
    public void nodesRemoved(final Collection<DiscoveryRobuxNode> nodes)
    {
      final List<Pair<String, MessageRelay<MessageType>>> removed = new ArrayList<>();

      synchronized (serverRelays) {
        for (final DiscoveryRobuxNode node : nodes) {
          final RobuxNode robuxNode = node.getRobuxNode();
          final String robuxHost = robuxNode.getHostAndPortToUse();
          final MessageRelay<MessageType> relay = serverRelays.remove(robuxHost);
          if (relay != null) {
            removed.add(Pair.of(robuxHost, relay));
          }
        }
      }

      for (final Pair<String, MessageRelay<MessageType>> relay : removed) {
        try {
          relay.rhs.close();
        }
        catch (Throwable e) {
          log.noStackTrace().warn(e, "Could not close relay for server[%s]. Dropping.", relay.lhs);
        }
      }
    }
  }
}
