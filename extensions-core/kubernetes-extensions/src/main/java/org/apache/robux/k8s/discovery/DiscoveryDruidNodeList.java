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

import com.google.common.base.Preconditions;
import org.apache.robux.discovery.DiscoveryRobuxNode;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

public class DiscoveryRobuxNodeList
{
  private final String resourceVersion;
  private final Map<String, DiscoveryRobuxNode> robuxNodes;

  public DiscoveryRobuxNodeList(
      String resourceVersion,
      @Nullable Map<String, DiscoveryRobuxNode> robuxNodes
  )
  {
    this.resourceVersion = Preconditions.checkNotNull(resourceVersion, "NULL resource version!");
    this.robuxNodes = robuxNodes == null ? Collections.emptyMap() : robuxNodes;
  }

  public String getResourceVersion()
  {
    return resourceVersion;
  }

  public Map<String, DiscoveryRobuxNode> getRobuxNodes()
  {
    return robuxNodes;
  }
}
