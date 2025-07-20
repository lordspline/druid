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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.robux.client.RobuxServer;
import org.apache.robux.jackson.StringObjectPairList;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.common.IAE;
import org.apache.robux.java.util.common.NonnullPair;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.server.RobuxNode;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * Representation of all information related to discovery of a node and all the other metadata associated with
 * the node per nodeRole such as broker, historical etc.
 * Note that one Robux process might announce multiple DiscoveryRobuxNode if it acts in multiple {@link NodeRole}s e. g.
 * Coordinator would announce DiscoveryRobuxNode for {@link NodeRole#OVERLORD} as well when acting as Overlord.
 */
public class DiscoveryRobuxNode
{
  private static final Logger LOG = new Logger(DiscoveryRobuxNode.class);

  private final RobuxNode robuxNode;
  private final NodeRole nodeRole;
  private final DateTime startTime;

  /**
   * Map of service name -> RobuxServices.
   * This map has only the RobuxServices that are understandable.
   * It means, if there is some RobuxService not understandable found while converting rawServices to services,
   * that RobuxService will be ignored and not stored in this map.
   *
   * @see RobuxNodeDiscoveryProvider#SERVICE_TO_NODE_TYPES
   */
  private final Map<String, RobuxService> services = new HashMap<>();

  public DiscoveryRobuxNode(
      RobuxNode robuxNode,
      NodeRole nodeRole,
      Map<String, RobuxService> services
  )
  {
    this(robuxNode, nodeRole, services, DateTimes.nowUtc());
  }

  public DiscoveryRobuxNode(
      RobuxNode robuxNode,
      NodeRole nodeRole,
      Map<String, RobuxService> services,
      DateTime startTime
  )
  {
    this.robuxNode = robuxNode;
    this.nodeRole = nodeRole;

    if (services != null && !services.isEmpty()) {
      this.services.putAll(services);
    }
    this.startTime = startTime;
  }

  @JsonCreator
  private static DiscoveryRobuxNode fromJson(
      @JsonProperty("robuxNode") RobuxNode robuxNode,
      @JsonProperty("nodeType") NodeRole nodeRole,
      @JsonProperty("services") Map<String, StringObjectPairList> rawServices,
      @JsonProperty("startTime") DateTime startTime,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    Map<String, RobuxService> services = new HashMap<>();
    if (rawServices != null && !rawServices.isEmpty()) {
      for (Entry<String, StringObjectPairList> entry : rawServices.entrySet()) {
        List<NonnullPair<String, Object>> val = entry.getValue().getPairs();
        try {
          services.put(entry.getKey(), jsonMapper.convertValue(toMap(val), RobuxService.class));
        }
        catch (RuntimeException e) {
          LOG.warn("Ignore unparseable RobuxService for [%s]: %s", robuxNode.getHostAndPortToUse(), val);
        }
      }
    }
    return new DiscoveryRobuxNode(robuxNode, nodeRole, services, startTime);
  }

  /**
   * A JSON of a {@link RobuxService} is deserialized to a Map and then converted to aRobuxService
   * to ignore any "unknown" RobuxServices to the current node. However, directly deserializing a JSON to a Map
   * is problematic for {@link DataNodeService} as it has duplicate "type" keys in its serialized form.
   * Because of the duplicate key, if we directly deserialize a JSON to a Map, we will lose one of the "type" property.
   * This is definitely a bug of DataNodeService, but, since renaming one of those duplicate keys will
   * break compatibility, DataNodeService still has the deprecated "type" property.
   * See the Javadoc of DataNodeService for more details.
   * <p>
   * This function catches such duplicate keys and rewrites the deprecated "type" to "serverType",
   * so that we don't lose any properties.
   * <p>
   * This method can be removed together when we entirely remove the deprecated "type" property from DataNodeService.
   */
  @Deprecated
  private static Map<String, Object> toMap(List<NonnullPair<String, Object>> pairs)
  {
    final Map<String, Object> map = Maps.newHashMapWithExpectedSize(pairs.size());
    for (NonnullPair<String, Object> pair : pairs) {
      final Object prevVal = map.put(pair.lhs, pair.rhs);
      if (prevVal != null) {
        if ("type".equals(pair.lhs)) {
          if (DataNodeService.DISCOVERY_SERVICE_KEY.equals(prevVal)) {
            map.put("type", prevVal);
            // this one is likely serverType.
            map.put(DataNodeService.SERVER_TYPE_PROP_KEY, pair.rhs);
            continue;
          } else if (DataNodeService.DISCOVERY_SERVICE_KEY.equals(pair.rhs)) {
            // this one is likely serverType.
            map.put(DataNodeService.SERVER_TYPE_PROP_KEY, prevVal);
            continue;
          }
        } else if (DataNodeService.SERVER_TYPE_PROP_KEY.equals(pair.lhs)) {
          // Ignore duplicate "serverType" keys since it can happen
          // when the JSON has both "type" and "serverType" keys for serverType.
          continue;
        }

        if (!prevVal.equals(pair.rhs)) {
          throw new IAE("Duplicate key[%s] with different values: [%s] and [%s]", pair.lhs, prevVal, pair.rhs);
        }
      }
    }
    return map;
  }

  @JsonProperty
  public Map<String, RobuxService> getServices()
  {
    return services;
  }

  /**
   * Keeping the legacy name 'nodeType' property name for backward compatibility. When the project is updated to
   * Jackson 2.9 it could be changed, see https://github.com/apache/robux/issues/7152.
   */
  @JsonProperty("nodeType")
  public NodeRole getNodeRole()
  {
    return nodeRole;
  }

  @JsonProperty
  public RobuxNode getRobuxNode()
  {
    return robuxNode;
  }

  @JsonProperty
  public DateTime getStartTime()
  {
    return startTime;
  }

  @Nullable
  @JsonIgnore
  public <T extends RobuxService> T getService(String key, Class<T> clazz)
  {
    final RobuxService o = services.get(key);
    if (o != null && clazz.isAssignableFrom(o.getClass())) {
      //noinspection unchecked
      return (T) o;
    }
    return null;
  }

  public RobuxServer toRobuxServer()
  {
    final DataNodeService dataNodeService = getService(
        DataNodeService.DISCOVERY_SERVICE_KEY,
        DataNodeService.class
    );

    final RobuxNode robuxNode = getRobuxNode();
    if (dataNodeService == null || robuxNode == null) {
      return null;
    }

    return new RobuxServer(
        robuxNode.getHostAndPortToUse(),
        robuxNode.getHostAndPort(),
        robuxNode.getHostAndTlsPort(),
        dataNodeService.getMaxSize(),
        dataNodeService.getServerType(),
        dataNodeService.getTier(),
        dataNodeService.getPriority()
    );
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
    DiscoveryRobuxNode that = (DiscoveryRobuxNode) o;
    return Objects.equals(robuxNode, that.robuxNode) &&
           Objects.equals(nodeRole, that.nodeRole) &&
           Objects.equals(services, that.services);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(robuxNode, nodeRole, services);
  }

  @Override
  public String toString()
  {
    return "DiscoveryRobuxNode{" +
           "robuxNode=" + robuxNode +
           ", nodeRole='" + nodeRole + '\'' +
           ", services=" + services + '\'' +
           ", startTime=" + startTime +
           '}';
  }
}
