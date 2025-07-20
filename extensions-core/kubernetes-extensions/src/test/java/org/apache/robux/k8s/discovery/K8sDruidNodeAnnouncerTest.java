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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.jackson.DefaultObjectMapper;
import org.apache.robux.server.RobuxNode;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class K8sRobuxNodeAnnouncerTest
{
  private final DiscoveryRobuxNode testNode = new DiscoveryRobuxNode(
      new RobuxNode("robux/router", "test-host", true, 80, null, true, false),
      NodeRole.ROUTER,
      null
  );

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private final PodInfo podInfo = new PodInfo("testpod", "testns");

  private final K8sDiscoveryConfig discoveryConfig = new K8sDiscoveryConfig("robux-cluster", null, null, null, null, null, null, null);

  @Test
  public void testAnnounce() throws Exception
  {
    K8sApiClient mockK8sApiClient = EasyMock.createMock(K8sApiClient.class);
    Capture<String> podNameArg = Capture.newInstance();
    Capture<String> namespaceArg = Capture.newInstance();
    Capture<String> patchArg = Capture.newInstance();
    mockK8sApiClient.patchPod(EasyMock.capture(podNameArg), EasyMock.capture(namespaceArg), EasyMock.capture(patchArg));
    EasyMock.replay(mockK8sApiClient);

    K8sRobuxNodeAnnouncer announcer = new K8sRobuxNodeAnnouncer(podInfo, discoveryConfig, mockK8sApiClient, jsonMapper);
    announcer.announce(testNode);

    Assert.assertEquals(podInfo.getPodName(), podNameArg.getValue());
    Assert.assertEquals(podInfo.getPodNamespace(), namespaceArg.getValue());

    List<Map<String, Object>> actualPatchList = jsonMapper.readValue(
        patchArg.getValue(),
        new TypeReference<>() {}
    );

    List<Map<String, Object>> expectedPatchList = Lists.newArrayList(
        ImmutableMap.of(
            "op", "add",
            "path", "/metadata/labels/robuxDiscoveryAnnouncement-router",
            "value", "true"
        ),
        ImmutableMap.of(
            "op", "add",
            "path", "/metadata/labels/robuxDiscoveryAnnouncement-id-hash",
            "value", "1429561393"
        ),
        ImmutableMap.of(
            "op", "add",
            "path", "/metadata/labels/robuxDiscoveryAnnouncement-cluster-identifier",
            "value", discoveryConfig.getClusterIdentifier()
        ),
        ImmutableMap.of(
            "op", "add",
            "path", "/metadata/annotations/robuxNodeInfo-router",
            "value", jsonMapper.writeValueAsString(testNode)
        )
    );
    Assert.assertEquals(expectedPatchList, actualPatchList);
  }

  @Test
  public void testUnannounce() throws Exception
  {
    K8sApiClient mockK8sApiClient = EasyMock.createMock(K8sApiClient.class);
    Capture<String> podNameArg = Capture.newInstance();
    Capture<String> namespaceArg = Capture.newInstance();
    Capture<String> patchArg = Capture.newInstance();
    mockK8sApiClient.patchPod(EasyMock.capture(podNameArg), EasyMock.capture(namespaceArg), EasyMock.capture(patchArg));
    EasyMock.replay(mockK8sApiClient);

    K8sRobuxNodeAnnouncer announcer = new K8sRobuxNodeAnnouncer(podInfo, discoveryConfig, mockK8sApiClient, jsonMapper);
    announcer.unannounce(testNode);

    Assert.assertEquals(podInfo.getPodName(), podNameArg.getValue());
    Assert.assertEquals(podInfo.getPodNamespace(), namespaceArg.getValue());

    List<Map<String, String>> actualPatchList = jsonMapper.readValue(
        patchArg.getValue(),
        new TypeReference<>() {}
    );

    List<Map<String, String>> expectedPatchList = Lists.newArrayList(
        ImmutableMap.of(
            "op", "remove",
            "path", "/metadata/labels/robuxDiscoveryAnnouncement-router"
        ),
        ImmutableMap.of(
            "op", "remove",
            "path", "/metadata/labels/robuxDiscoveryAnnouncement-id-hash"
        ),
        ImmutableMap.of(
            "op", "remove",
            "path", "/metadata/labels/robuxDiscoveryAnnouncement-cluster-identifier"
        ),
        ImmutableMap.of(
            "op", "remove",
            "path", "/metadata/annotations/robuxNodeInfo-router"
        )
    );
    Assert.assertEquals(expectedPatchList, actualPatchList);
  }
}
