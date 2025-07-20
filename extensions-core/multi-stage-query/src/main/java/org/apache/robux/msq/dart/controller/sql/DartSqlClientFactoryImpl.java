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

package org.apache.robux.msq.dart.controller.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.robux.guice.annotations.EscalatedGlobal;
import org.apache.robux.guice.annotations.Json;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.rpc.FixedServiceLocator;
import org.apache.robux.rpc.ServiceClient;
import org.apache.robux.rpc.ServiceClientFactory;
import org.apache.robux.rpc.ServiceLocation;
import org.apache.robux.rpc.StandardRetryPolicy;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.sql.http.SqlResource;

/**
 * Production implementation of {@link DartSqlClientFactory}.
 */
public class DartSqlClientFactoryImpl implements DartSqlClientFactory
{
  private final ServiceClientFactory clientFactory;
  private final ObjectMapper jsonMapper;

  @Inject
  public DartSqlClientFactoryImpl(
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      @Json final ObjectMapper jsonMapper
  )
  {
    this.clientFactory = clientFactory;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public DartSqlClient makeClient(RobuxNode node)
  {
    final ServiceClient client = clientFactory.makeClient(
        StringUtils.format("%s[dart-sql]", node.getHostAndPortToUse()),
        new FixedServiceLocator(ServiceLocation.fromRobuxNode(node).withBasePath(SqlResource.PATH)),
        StandardRetryPolicy.noRetries()
    );

    return new DartSqlClientImpl(client, jsonMapper);
  }
}
