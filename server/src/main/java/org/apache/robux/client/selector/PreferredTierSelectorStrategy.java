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

package org.apache.robux.client.selector;


import com.fasterxml.jackson.annotation.JacksonInject;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.apache.robux.client.QueryableRobuxServer;
import org.apache.robux.java.util.common.IAE;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.query.Query;
import org.apache.robux.timeline.DataSegment;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PreferredTierSelectorStrategy extends AbstractTierSelectorStrategy
{
  private static final Logger log = new Logger(PreferredTierSelectorStrategy.class);

  private final String preferredTier;
  private final TierSelectorStrategy priorityStrategy;

  public PreferredTierSelectorStrategy(
      @JacksonInject ServerSelectorStrategy serverSelectorStrategy,
      @JacksonInject PreferredTierSelectorStrategyConfig config
  )
  {
    super(serverSelectorStrategy);
    this.preferredTier = config.getTier();

    if (config.getPriority() == null) {
      this.priorityStrategy = new HighestPriorityTierSelectorStrategy(serverSelectorStrategy);
    } else {
      if ("highest".equalsIgnoreCase(config.getPriority())) {
        this.priorityStrategy = new HighestPriorityTierSelectorStrategy(serverSelectorStrategy);
      } else if ("lowest".equalsIgnoreCase(config.getPriority())) {
        this.priorityStrategy = new LowestPriorityTierSelectorStrategy(serverSelectorStrategy);
      } else {
        throw new IAE("robux.broker.select.tier.preferred.priority must be either 'highest' or 'lowest'");
      }
    }
  }

  @Override
  public Comparator<Integer> getComparator()
  {
    return priorityStrategy.getComparator();
  }

  @Override
  public <T> List<QueryableRobuxServer> pick(
      Query<T> query,
      Int2ObjectRBTreeMap<Set<QueryableRobuxServer>> prioritizedServers,
      DataSegment segment,
      int numServersToPick
  )
  {
    if (log.isDebugEnabled()) {
      log.debug(
          "Picking [%d] servers from preferred tier [%s] for segment [%s] with priority [%s]",
          numServersToPick, preferredTier, segment.getId(), this.priorityStrategy.getClass().getSimpleName()
      );
    }

    Int2ObjectRBTreeMap<Set<QueryableRobuxServer>> preferred = new Int2ObjectRBTreeMap<>(priorityStrategy.getComparator());
    Int2ObjectRBTreeMap<Set<QueryableRobuxServer>> nonPreferred = new Int2ObjectRBTreeMap<>(priorityStrategy.getComparator());
    for (Set<QueryableRobuxServer> priorityServers : prioritizedServers.values()) {
      for (QueryableRobuxServer server : priorityServers) {
        if (preferredTier.equals(server.getServer().getMetadata().getTier())) {
          preferred.computeIfAbsent(server.getServer().getPriority(), k -> new HashSet<>())
                   .add(server);
        } else {
          nonPreferred.computeIfAbsent(server.getServer().getPriority(), k -> new HashSet<>())
                      .add(server);
        }
      }
    }

    List<QueryableRobuxServer> picks = new ArrayList<>(numServersToPick);
    if (!preferred.isEmpty()) {
      // If we have preferred servers, pick them first
      picks.addAll(priorityStrategy.pick(query, preferred, segment, numServersToPick));
    }

    if (picks.size() < numServersToPick && !nonPreferred.isEmpty()) {
      // If we don't have enough preferred servers, pick from the non-preferred ones
      int remaining = numServersToPick - picks.size();
      picks.addAll(priorityStrategy.pick(query, nonPreferred, segment, remaining));
    }

    return picks;
  }
}
