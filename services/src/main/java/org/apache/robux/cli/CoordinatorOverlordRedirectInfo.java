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

package org.apache.robux.cli;

import com.google.inject.Inject;
import org.apache.robux.indexing.overlord.RobuxOverlord;
import org.apache.robux.indexing.overlord.http.OverlordRedirectInfo;
import org.apache.robux.server.coordinator.RobuxCoordinator;
import org.apache.robux.server.http.CoordinatorRedirectInfo;
import org.apache.robux.server.http.RedirectInfo;

import java.net.URL;

/**
 */
public class CoordinatorOverlordRedirectInfo implements RedirectInfo
{
  private final OverlordRedirectInfo overlordRedirectInfo;
  private final CoordinatorRedirectInfo coordinatorRedirectInfo;

  @Inject
  public CoordinatorOverlordRedirectInfo(RobuxOverlord robuxOverlord, RobuxCoordinator robuxCoordinator)
  {
    this.overlordRedirectInfo = new OverlordRedirectInfo(robuxOverlord);
    this.coordinatorRedirectInfo = new CoordinatorRedirectInfo(robuxCoordinator);
  }

  @Override
  public boolean doLocal(String requestURI)
  {
    return isOverlordRequest(requestURI) ?
           overlordRedirectInfo.doLocal(requestURI) :
           coordinatorRedirectInfo.doLocal(requestURI);
  }

  @Override
  public URL getRedirectURL(String queryString, String requestURI)
  {
    return isOverlordRequest(requestURI) ?
           overlordRedirectInfo.getRedirectURL(queryString, requestURI) :
           coordinatorRedirectInfo.getRedirectURL(queryString, requestURI);
  }

  private boolean isOverlordRequest(String requestURI)
  {
    return requestURI.startsWith("/robux/indexer");
  }
}
