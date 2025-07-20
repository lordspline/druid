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

package org.apache.robux.curator.discovery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.curator.utils.ZKPaths;
import org.apache.robux.curator.announcement.ServiceAnnouncer;
import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.discovery.RobuxNodeAnnouncer;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.annotations.Json;
import org.apache.robux.guice.annotations.SingleThreadedAnnouncer;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.initialization.ZkPathsConfig;

public class CuratorRobuxNodeAnnouncer implements RobuxNodeAnnouncer
{
  static String makeNodeAnnouncementPath(ZkPathsConfig config, NodeRole nodeRole, RobuxNode node)
  {
    return ZKPaths.makePath(config.getInternalDiscoveryPath(), nodeRole.toString(), node.getHostAndPortToUse());
  }

  private static final Logger log = new Logger(CuratorRobuxNodeAnnouncer.class);

  private final ServiceAnnouncer announcer;
  private final ZkPathsConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public CuratorRobuxNodeAnnouncer(
      @SingleThreadedAnnouncer ServiceAnnouncer announcer,
      ZkPathsConfig config,
      @Json ObjectMapper jsonMapper
  )
  {
    this.announcer = announcer;
    this.config = config;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void announce(DiscoveryRobuxNode discoveryRobuxNode)
  {
    try {
      final String asString = jsonMapper.writeValueAsString(discoveryRobuxNode);

      log.debug("Announcing self [%s].", asString);

      String path =
          makeNodeAnnouncementPath(config, discoveryRobuxNode.getNodeRole(), discoveryRobuxNode.getRobuxNode());
      announcer.announce(path, StringUtils.toUtf8(asString));

      log.info("Announced self [%s].", asString);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void unannounce(DiscoveryRobuxNode discoveryRobuxNode)
  {
    try {
      final String asString = jsonMapper.writeValueAsString(discoveryRobuxNode);

      log.debug("Unannouncing self [%s].", asString);

      String path =
          makeNodeAnnouncementPath(config, discoveryRobuxNode.getNodeRole(), discoveryRobuxNode.getRobuxNode());
      announcer.unannounce(path);

      log.info("Unannounced self [%s].", asString);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
