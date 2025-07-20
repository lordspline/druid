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

package org.apache.robux.server.http;

import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.robux.client.RobuxDataSource;
import org.apache.robux.client.RobuxServer;
import org.apache.robux.client.InventoryView;
import org.apache.robux.server.http.security.StateResourceFilter;
import org.apache.robux.timeline.DataSegment;
import org.joda.time.Interval;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 */
@Path("/robux/coordinator/v1/tiers")
@ResourceFilters(StateResourceFilter.class)
public class TiersResource
{
  private final InventoryView serverInventoryView;

  @Inject
  public TiersResource(InventoryView serverInventoryView)
  {
    this.serverInventoryView = serverInventoryView;
  }

  private enum TierMetadataKeys
  {
    /** Lowercase on purpose, to match the established format. */
    currSize,
    maxSize
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTiers(@QueryParam("simple") String simple)
  {
    Response.ResponseBuilder builder = Response.status(Response.Status.OK);

    if (simple != null) {
      Map<String, Map<TierMetadataKeys, Long>> metadata = new HashMap<>();
      for (RobuxServer robuxServer : serverInventoryView.getInventory()) {
        Map<TierMetadataKeys, Long> tierMetadata = metadata
            .computeIfAbsent(robuxServer.getTier(), tier -> new EnumMap<>(TierMetadataKeys.class));
        tierMetadata.merge(TierMetadataKeys.currSize, robuxServer.getCurrSize(), Long::sum);
        tierMetadata.merge(TierMetadataKeys.maxSize, robuxServer.getMaxSize(), Long::sum);
      }
      return builder.entity(metadata).build();
    }

    Set<String> tiers = serverInventoryView
        .getInventory()
        .stream()
        .map(RobuxServer::getTier)
        .collect(Collectors.toSet());

    return builder.entity(tiers).build();
  }

  private enum IntervalProperties
  {
    /** Lowercase on purpose, to match the established format. */
    size,
    count
  }

  @GET
  @Path("/{tierName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTierDataSources(@PathParam("tierName") String tierName, @QueryParam("simple") String simple)
  {
    if (simple != null) {
      Map<String, Map<Interval, Map<IntervalProperties, Object>>> tierToStatsPerInterval = new HashMap<>();
      for (RobuxServer robuxServer : serverInventoryView.getInventory()) {
        if (robuxServer.getTier().equalsIgnoreCase(tierName)) {
          for (DataSegment dataSegment : robuxServer.iterateAllSegments()) {
            Map<IntervalProperties, Object> properties = tierToStatsPerInterval
                .computeIfAbsent(dataSegment.getDataSource(), dsName -> new HashMap<>())
                .computeIfAbsent(dataSegment.getInterval(), interval -> new EnumMap<>(IntervalProperties.class));
            properties.merge(IntervalProperties.size, dataSegment.getSize(), (a, b) -> (Long) a + (Long) b);
            properties.merge(IntervalProperties.count, 1, (a, b) -> (Integer) a + (Integer) b);
          }
        }
      }

      return Response.ok(tierToStatsPerInterval).build();
    }

    Set<String> retVal = serverInventoryView
        .getInventory()
        .stream()
        .filter(robuxServer -> robuxServer.getTier().equalsIgnoreCase(tierName))
        .flatMap(robuxServer -> robuxServer.getDataSources().stream().map(RobuxDataSource::getName))
        .collect(Collectors.toSet());

    return Response.ok(retVal).build();
  }
}
