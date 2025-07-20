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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.discovery.RobuxNodeAnnouncer;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.annotations.Json;
import org.apache.robux.java.util.common.RE;
import org.apache.robux.java.util.common.RetryUtils;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.server.RobuxNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Announcement creates following in the pod def...
 *
 * Labels -
 * robuxDiscoveryAnnouncement-<nodeRole.getJsonName()> = true
 * robuxDiscoveryAnnouncement-id-hash = hashEncodeStringForLabelValue(host:port)
 * robuxDiscoveryAnnouncement-cluster-identifier = <clusterIdentifier>
 *
 * Annotation -
 * robuxNodeInfo-<nodeRole.getJsonName()> = json_serialize(DiscoveryRobuxNode)
 *
 * Note that, a node can have multiple roles e.g. coordinator can take up overlord's role as well.
 */
public class K8sRobuxNodeAnnouncer implements RobuxNodeAnnouncer
{
  private static final Logger LOGGER = new Logger(K8sRobuxNodeAnnouncer.class);

  private static String POD_LABELS_PATH_PREFIX = "/metadata/labels";
  private static String POD_ANNOTATIONS_PATH_PREFIX = "/metadata/annotations";

  private static final String OP_ADD = "add";
  private static final String OP_REMOVE = "remove";

  public static final String ANNOUNCEMENT_DONE = "true";

  private final ObjectMapper jsonMapper;
  private final K8sDiscoveryConfig discoveryConfig;
  private final PodInfo podInfo;
  private final K8sApiClient k8sApiClient;

  @Inject
  public K8sRobuxNodeAnnouncer(
      PodInfo podInfo,
      K8sDiscoveryConfig discoveryConfig,
      K8sApiClient k8sApiClient,
      @Json ObjectMapper jsonMapper
  )
  {
    this.discoveryConfig = discoveryConfig;
    this.podInfo = podInfo;
    this.k8sApiClient = k8sApiClient;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void announce(DiscoveryRobuxNode discoveryRobuxNode)
  {
    LOGGER.info("Announcing DiscoveryRobuxNode[%s]", discoveryRobuxNode);

    String roleAnnouncementLabel = getRoleAnnouncementLabel(discoveryRobuxNode.getNodeRole());
    String idAnnouncementLabel = getIdHashAnnouncementLabel();
    String clusterIdentifierAnnouncementLabel = getClusterIdentifierAnnouncementLabel();
    String infoAnnotation = getInfoAnnotation(discoveryRobuxNode.getNodeRole());

    try {
      List<Map<String, Object>> patches = new ArrayList<>();

      // Note: We assume here that at least one label and annotation exists on the pod already, so that
      // paths where labels/annotations are created, pre-exist.
      // See https://github.com/kubernetes-sigs/kustomize/issues/2986 , we can add workaround of getting pod spec,
      // checking if label/annotation path exists and create if not, however that could lead to race conditions
      // so assuming the existence for now.
      patches.add(createPatchObj(OP_ADD, getPodDefLabelPath(roleAnnouncementLabel), ANNOUNCEMENT_DONE));
      patches.add(createPatchObj(OP_ADD, getPodDefLabelPath(idAnnouncementLabel), hashEncodeStringForLabelValue(discoveryRobuxNode.getRobuxNode().getHostAndPortToUse())));
      patches.add(createPatchObj(OP_ADD, getPodDefLabelPath(clusterIdentifierAnnouncementLabel), discoveryConfig.getClusterIdentifier()));
      patches.add(createPatchObj(OP_ADD, getPodDefAnnocationPath(infoAnnotation), jsonMapper.writeValueAsString(discoveryRobuxNode)));

      // Creating patch string outside of retry block to not retry json serialization failures
      String jsonPatchStr = jsonMapper.writeValueAsString(patches);
      LOGGER.info("Json Patch For Node Announcement: [%s]", jsonPatchStr);

      RetryUtils.retry(
          () -> {
            k8sApiClient.patchPod(podInfo.getPodName(), podInfo.getPodNamespace(), jsonPatchStr);
            return "na";
          },
          (throwable) -> true,
          3
      );

      LOGGER.info("Announced DiscoveryRobuxNode[%s]", discoveryRobuxNode);
    }
    catch (Exception ex) {
      throw new RE(ex, "Failed to announce DiscoveryRobuxNode[%s]", discoveryRobuxNode);
    }
  }

  @Override
  public void unannounce(DiscoveryRobuxNode discoveryRobuxNode)
  {
    LOGGER.info("Unannouncing DiscoveryRobuxNode[%s]", discoveryRobuxNode);

    String roleAnnouncementLabel = getRoleAnnouncementLabel(discoveryRobuxNode.getNodeRole());
    String idHashAnnouncementLabel = getIdHashAnnouncementLabel();
    String clusterIdentifierAnnouncementLabel = getClusterIdentifierAnnouncementLabel();
    String infoAnnotation = getInfoAnnotation(discoveryRobuxNode.getNodeRole());

    try {
      List<Map<String, Object>> patches = new ArrayList<>();
      patches.add(createPatchObj(OP_REMOVE, getPodDefLabelPath(roleAnnouncementLabel), null));
      patches.add(createPatchObj(OP_REMOVE, getPodDefLabelPath(idHashAnnouncementLabel), null));
      patches.add(createPatchObj(OP_REMOVE, getPodDefLabelPath(clusterIdentifierAnnouncementLabel), null));
      patches.add(createPatchObj(OP_REMOVE, getPodDefAnnocationPath(infoAnnotation), null));

      // Creating patch string outside of retry block to not retry json serialization failures
      String jsonPatchStr = jsonMapper.writeValueAsString(patches);

      RetryUtils.retry(
          () -> {
            k8sApiClient.patchPod(podInfo.getPodName(), podInfo.getPodNamespace(), jsonPatchStr);
            return "na";
          },
          (throwable) -> true,
          3
      );

      LOGGER.info("Unannounced DiscoveryRobuxNode[%s]", discoveryRobuxNode);

    }
    catch (Exception ex) {
      // Unannouncement happens when robux process is shutting down, there is no point throwing exception
      // in shutdown sequence.
      if (ex instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }

      LOGGER.error(ex, "Failed to unannounce DiscoveryRobuxNode[%s]", discoveryRobuxNode);
    }
  }

  private Map<String, Object> createPatchObj(String op, String path, Object value)
  {
    if (value == null) {
      return ImmutableMap.of(
          "op", op,
          "path", path
      );
    } else {
      return ImmutableMap.of(
          "op", op,
          "path", path,
          "value", value
      );
    }
  }

  public static String getRoleAnnouncementLabel(NodeRole nodeRole)
  {
    return StringUtils.format("robuxDiscoveryAnnouncement-%s", nodeRole.getJsonName());
  }

  private static String getIdHashAnnouncementLabel()
  {
    return "robuxDiscoveryAnnouncement-id-hash";
  }

  public static String getClusterIdentifierAnnouncementLabel()
  {
    return "robuxDiscoveryAnnouncement-cluster-identifier";
  }

  public static String getInfoAnnotation(NodeRole nodeRole)
  {
    return StringUtils.format("robuxNodeInfo-%s", nodeRole.getJsonName());
  }

  public static String getLabelSelectorForNodeRole(K8sDiscoveryConfig discoveryConfig, NodeRole nodeRole)
  {
    return StringUtils.format(
        "%s=%s,%s=%s",
        getClusterIdentifierAnnouncementLabel(),
        discoveryConfig.getClusterIdentifier(),
        K8sRobuxNodeAnnouncer.getRoleAnnouncementLabel(nodeRole),
        K8sRobuxNodeAnnouncer.ANNOUNCEMENT_DONE
    );
  }

  public static String getLabelSelectorForNode(K8sDiscoveryConfig discoveryConfig, NodeRole nodeRole, RobuxNode node)
  {
    return StringUtils.format(
        "%s=%s,%s=%s,%s=%s",
        getClusterIdentifierAnnouncementLabel(),
        discoveryConfig.getClusterIdentifier(),
        K8sRobuxNodeAnnouncer.getRoleAnnouncementLabel(nodeRole),
        K8sRobuxNodeAnnouncer.ANNOUNCEMENT_DONE,
        K8sRobuxNodeAnnouncer.getIdHashAnnouncementLabel(),
        hashEncodeStringForLabelValue(node.getHostAndPortToUse())
    );
  }

  private String getPodDefLabelPath(String label)
  {
    return StringUtils.format("%s/%s", POD_LABELS_PATH_PREFIX, label);
  }

  private String getPodDefAnnocationPath(String annotation)
  {
    return StringUtils.format("%s/%s", POD_ANNOTATIONS_PATH_PREFIX, annotation);
  }

  // a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and
  // must start and end with an alphanumeric character
  private static String hashEncodeStringForLabelValue(String str)
  {
    int hash = str.hashCode();
    if (hash < 0) {
      hash = -1 * hash;
    }
    return String.valueOf(hash);
  }
}
