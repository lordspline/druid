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

package org.apache.robux.msq.dart.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.robux.guice.annotations.EscalatedGlobal;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.guice.annotations.Smile;
import org.apache.robux.messages.client.MessageRelay;
import org.apache.robux.messages.client.MessageRelayClientImpl;
import org.apache.robux.messages.client.MessageRelayFactory;
import org.apache.robux.msq.dart.controller.messages.ControllerMessage;
import org.apache.robux.msq.dart.worker.http.DartWorkerResource;
import org.apache.robux.rpc.FixedServiceLocator;
import org.apache.robux.rpc.ServiceClient;
import org.apache.robux.rpc.ServiceClientFactory;
import org.apache.robux.rpc.ServiceLocation;
import org.apache.robux.rpc.StandardRetryPolicy;
import org.apache.robux.server.RobuxNode;

/**
 * Production implementation of {@link MessageRelayFactory}.
 */
public class DartMessageRelayFactoryImpl implements MessageRelayFactory<ControllerMessage>
{
  private final String clientHost;
  private final ControllerMessageListener messageListener;
  private final ServiceClientFactory clientFactory;
  private final String basePath;
  private final ObjectMapper smileMapper;

  @Inject
  public DartMessageRelayFactoryImpl(
      @Self RobuxNode selfNode,
      @EscalatedGlobal ServiceClientFactory clientFactory,
      @Smile ObjectMapper smileMapper,
      ControllerMessageListener messageListener
  )
  {
    this.clientHost = selfNode.getHostAndPortToUse();
    this.messageListener = messageListener;
    this.clientFactory = clientFactory;
    this.smileMapper = smileMapper;
    this.basePath = DartWorkerResource.PATH + "/relay";
  }

  @Override
  public MessageRelay<ControllerMessage> newRelay(RobuxNode clientNode)
  {
    final ServiceLocation location = ServiceLocation.fromRobuxNode(clientNode).withBasePath(basePath);
    final ServiceClient client = clientFactory.makeClient(
        clientNode.getHostAndPortToUse(),
        new FixedServiceLocator(location),
        StandardRetryPolicy.unlimited()
    );

    return new MessageRelay<>(
        clientHost,
        clientNode,
        new MessageRelayClientImpl<>(client, smileMapper, ControllerMessage.class),
        messageListener
    );
  }
}
