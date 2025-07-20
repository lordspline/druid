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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.robux.guice.ServerModule;
import org.apache.robux.jackson.DefaultObjectMapper;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.coordination.ServerType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;

public class DiscoveryRobuxNodeTest
{
  private final RobuxNode robuxNode;

  private final NodeRole nodeRole;

  public DiscoveryRobuxNodeTest()
  {
    this.robuxNode = new RobuxNode(
        "testNode",
        "host",
        true,
        8082,
        null,
        true,
        false
    );
    nodeRole = NodeRole.BROKER;
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(DiscoveryRobuxNode.class)
                  .withNonnullFields("robuxNode", "nodeRole", "services")
                  .withIgnoredFields("startTime")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testDeserialize() throws JsonProcessingException
  {
    final ObjectMapper mapper = createObjectMapper(ImmutableList.of(Service1.class, Service2.class));
    final DiscoveryRobuxNode node = new DiscoveryRobuxNode(
        robuxNode,
        nodeRole,
        ImmutableMap.of("service1", new Service1(), "service2", new Service2())
    );
    final String json = mapper.writeValueAsString(node);
    final DiscoveryRobuxNode fromJson = mapper.readValue(json, DiscoveryRobuxNode.class);
    Assert.assertEquals(node, fromJson);
  }

  @Test
  public void testDeserializeIgnorUnknownRobuxService() throws JsonProcessingException
  {
    final ObjectMapper mapper = createObjectMapper(ImmutableList.of(Service1.class));
    final DiscoveryRobuxNode node = new DiscoveryRobuxNode(
        robuxNode,
        nodeRole,
        ImmutableMap.of("service1", new Service1(), "service2", new Service2())
    );
    final String json = mapper.writeValueAsString(node);
    final DiscoveryRobuxNode fromJson = mapper.readValue(json, DiscoveryRobuxNode.class);
    Assert.assertEquals(
        new DiscoveryRobuxNode(
            robuxNode,
            nodeRole,
            ImmutableMap.of("service1", new Service1())
        ),
        fromJson
    );
  }

  @Test
  public void testSerdeWithDataNodeAndLookupNodeServices() throws JsonProcessingException
  {
    final ObjectMapper mapper = createObjectMapper(ImmutableList.of());
    final DiscoveryRobuxNode node = new DiscoveryRobuxNode(
        new RobuxNode(
            "robux/broker",
            "robux-broker",
            false,
            8082,
            -1,
            8282,
            true,
            true
        ),
        NodeRole.BROKER,
        ImmutableMap.of(
            DataNodeService.DISCOVERY_SERVICE_KEY,
            new DataNodeService("_default_tier", 1000000000, ServerType.BROKER, 0),
            LookupNodeService.DISCOVERY_SERVICE_KEY,
            new LookupNodeService("lookup_tier")
        )
    );
    final String json = mapper.writeValueAsString(node);
    Assert.assertEquals(
        node,
        mapper.readValue(json, DiscoveryRobuxNode.class)
    );
  }

  @Test
  public void testDeserializeWithDataNodeServiceWithAWrongPropertyOrder() throws JsonProcessingException
  {
    final ObjectMapper mapper = createObjectMapper(ImmutableList.of());
    final String json = "{\n"
                        + "  \"robuxNode\" : {\n"
                        + "    \"service\" : \"robux/broker\",\n"
                        + "    \"host\" : \"robux-broker\",\n"
                        + "    \"bindOnHost\" : false,\n"
                        + "    \"plaintextPort\" : 8082,\n"
                        + "    \"port\" : -1,\n"
                        + "    \"tlsPort\" : 8282,\n"
                        + "    \"enablePlaintextPort\" : true,\n"
                        + "    \"enableTlsPort\" : true\n"
                        + "  },\n"
                        + "  \"nodeType\" : \"broker\",\n"
                        + "  \"services\" : {\n"
                        + "    \"dataNodeService\" : {\n"
                        // In normal case, this proprty must appear after another "type" below.
                        + "      \"type\" : \"broker\",\n"
                        + "      \"type\" : \"dataNodeService\",\n"
                        + "      \"tier\" : \"_default_tier\",\n"
                        + "      \"maxSize\" : 1000000000,\n"
                        + "      \"serverType\" : \"broker\",\n"
                        + "      \"priority\" : 0\n"
                        + "    }\n"
                        + "  }\n"
                        + "}";
    Assert.assertEquals(
        new DiscoveryRobuxNode(
            new RobuxNode(
                "robux/broker",
                "robux-broker",
                false,
                8082,
                -1,
                8282,
                true,
                true
            ),
            NodeRole.BROKER,
            ImmutableMap.of(
                "dataNodeService",
                new DataNodeService("_default_tier", 1000000000, ServerType.BROKER, 0)
            )
        ),
        mapper.readValue(json, DiscoveryRobuxNode.class)
    );
  }

  @Test
  public void testDeserialize_duplicateProperties_shouldSucceedToDeserialize() throws JsonProcessingException
  {
    final ObjectMapper mapper = createObjectMapper(ImmutableList.of());
    final String json = "{\n"
                        + "  \"robuxNode\" : {\n"
                        + "    \"service\" : \"robux/broker\",\n"
                        + "    \"host\" : \"robux-broker\",\n"
                        + "    \"bindOnHost\" : false,\n"
                        + "    \"plaintextPort\" : 8082,\n"
                        + "    \"port\" : -1,\n"
                        + "    \"tlsPort\" : 8282,\n"
                        + "    \"enablePlaintextPort\" : true,\n"
                        + "    \"enableTlsPort\" : true\n"
                        + "  },\n"
                        + "  \"nodeType\" : \"broker\",\n"
                        + "  \"services\" : {\n"
                        + "    \"dataNodeService\" : {\n"
                        + "      \"type\" : \"dataNodeService\",\n"
                        + "      \"tier\" : \"_default_tier\",\n"
                        + "      \"maxSize\" : 1000000000,\n"
                        + "      \"maxSize\" : 1000000000,\n"
                        + "      \"serverType\" : \"broker\",\n"
                        + "      \"priority\" : 0\n"
                        + "    }\n"
                        + "  }\n"
                        + "}";
    Assert.assertEquals(
        new DiscoveryRobuxNode(
            new RobuxNode(
                "robux/broker",
                "robux-broker",
                false,
                8082,
                -1,
                8282,
                true,
                true
            ),
            NodeRole.BROKER,
            ImmutableMap.of(
                "dataNodeService",
                new DataNodeService("_default_tier", 1000000000, ServerType.BROKER, 0)
            )
        ),
        mapper.readValue(json, DiscoveryRobuxNode.class)
    );
  }

  @Test
  public void testDeserialize_duplicateKeysWithDifferentValus_shouldIgnoreDataNodeService()
      throws JsonProcessingException
  {
    final ObjectMapper mapper = createObjectMapper(ImmutableList.of());
    final String json = "{\n"
                        + "  \"robuxNode\" : {\n"
                        + "    \"service\" : \"robux/broker\",\n"
                        + "    \"host\" : \"robux-broker\",\n"
                        + "    \"bindOnHost\" : false,\n"
                        + "    \"plaintextPort\" : 8082,\n"
                        + "    \"port\" : -1,\n"
                        + "    \"tlsPort\" : 8282,\n"
                        + "    \"enablePlaintextPort\" : true,\n"
                        + "    \"enableTlsPort\" : true\n"
                        + "  },\n"
                        + "  \"nodeType\" : \"broker\",\n"
                        + "  \"services\" : {\n"
                        + "    \"dataNodeService\" : {\n"
                        + "      \"type\" : \"dataNodeService\",\n"
                        + "      \"tier\" : \"_default_tier\",\n"
                        + "      \"maxSize\" : 1000000000,\n"
                        + "      \"maxSize\" : 10,\n"
                        + "      \"serverType\" : \"broker\",\n"
                        + "      \"priority\" : 0\n"
                        + "    }\n"
                        + "  }\n"
                        + "}";
    Assert.assertEquals(
        new DiscoveryRobuxNode(
            new RobuxNode(
                "robux/broker",
                "robux-broker",
                false,
                8082,
                -1,
                8282,
                true,
                true
            ),
            NodeRole.BROKER,
            ImmutableMap.of()
        ),
        mapper.readValue(json, DiscoveryRobuxNode.class)
    );
  }

  private static class Service1 extends RobuxService
  {
    @Override
    public String getName()
    {
      return "service1";
    }

    @Override
    public int hashCode()
    {
      return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
      return obj instanceof Service1;
    }
  }

  private static class Service2 extends RobuxService
  {
    @Override
    public String getName()
    {
      return "service2";
    }

    @Override
    public int hashCode()
    {
      return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
      return obj instanceof Service2;
    }
  }

  private static ObjectMapper createObjectMapper(Collection<Class<? extends RobuxService>> robuxServicesToRegister)
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerModules(new ServerModule().getJacksonModules());
    //noinspection unchecked,rawtypes
    mapper.registerSubtypes((Collection) robuxServicesToRegister);
    mapper.setInjectableValues(new Std().addValue(ObjectMapper.class, mapper));
    return mapper;
  }
}
