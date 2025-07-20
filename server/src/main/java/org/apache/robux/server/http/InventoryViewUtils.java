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

import org.apache.robux.client.RobuxDataSource;
import org.apache.robux.client.ImmutableRobuxDataSource;
import org.apache.robux.client.InventoryView;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.server.security.AuthorizationUtils;
import org.apache.robux.server.security.AuthorizerMapper;

import javax.servlet.http.HttpServletRequest;
import java.util.Collections;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

public interface InventoryViewUtils
{
  static Comparator<ImmutableRobuxDataSource> comparingByName()
  {
    return Comparator.comparing(ImmutableRobuxDataSource::getName);
  }

  static SortedSet<ImmutableRobuxDataSource> getDataSources(InventoryView serverInventoryView)
  {
    return serverInventoryView.getInventory()
                              .stream()
                              .flatMap(server -> server.getDataSources().stream())
                              .map(RobuxDataSource::toImmutableRobuxDataSource)
                              .collect(Collectors.toCollection(() -> new TreeSet<>(comparingByName())));
  }

  static SortedSet<ImmutableRobuxDataSource> getSecuredDataSources(
      HttpServletRequest request,
      InventoryView inventoryView,
      final AuthorizerMapper authorizerMapper
  )
  {
    if (authorizerMapper == null) {
      throw new ISE("No authorization mapper found");
    }

    Iterable<ImmutableRobuxDataSource> filteredResources = AuthorizationUtils.filterAuthorizedResources(
        request,
        getDataSources(inventoryView),
        datasource ->
            Collections.singletonList(AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR.apply(datasource.getName())),
        authorizerMapper
    );
    SortedSet<ImmutableRobuxDataSource> set = new TreeSet<>(comparingByName());
    filteredResources.forEach(set::add);
    return Collections.unmodifiableSortedSet(set);
  }
}
