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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.servlet.GuiceFilter;
import org.apache.robux.guice.GuiceInjectors;
import org.apache.robux.guice.Jerseys;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.LifecycleModule;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.initialization.Initialization;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.http.client.HttpClient;
import org.apache.robux.java.util.http.client.Request;
import org.apache.robux.java.util.http.client.response.StringFullResponseHolder;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.initialization.BaseJettyTest;
import org.apache.robux.server.initialization.jetty.JettyServerInitializer;
import org.easymock.EasyMock;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 */
@SuppressWarnings("DoNotMock")
public class RobuxLeaderClientTest extends BaseJettyTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private DiscoveryRobuxNode discoveryRobuxNode;
  private HttpClient httpClient;

  @Override
  protected Injector setupInjector()
  {
    final RobuxNode node = new RobuxNode("test", "localhost", false, null, null, true, false);
    discoveryRobuxNode = new DiscoveryRobuxNode(node, NodeRole.PEON, ImmutableMap.of());

    Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder,
                    Key.get(RobuxNode.class, Self.class),
                    node
                );
                binder.bind(Integer.class).annotatedWith(Names.named("port")).toInstance(node.getPlaintextPort());
                binder.bind(JettyServerInitializer.class).to(TestJettyServerInitializer.class).in(LazySingleton.class);
                Jerseys.addResource(binder, SimpleResource.class);
                LifecycleModule.register(binder, Server.class);
              }
            }
        )
    );
    httpClient = injector.getInstance(ClientHolder.class).getClient();
    return injector;
  }

  @Test
  public void testSimple() throws Exception
  {
    RobuxNodeDiscovery robuxNodeDiscovery = EasyMock.createMock(RobuxNodeDiscovery.class);
    EasyMock.expect(robuxNodeDiscovery.getAllNodes()).andReturn(
        ImmutableList.of(discoveryRobuxNode)
    );

    RobuxNodeDiscoveryProvider robuxNodeDiscoveryProvider = EasyMock.createMock(RobuxNodeDiscoveryProvider.class);
    EasyMock.expect(robuxNodeDiscoveryProvider.getForNodeRole(NodeRole.PEON)).andReturn(robuxNodeDiscovery);

    EasyMock.replay(robuxNodeDiscovery, robuxNodeDiscoveryProvider);

    RobuxLeaderClient robuxLeaderClient = new RobuxLeaderClient(
        httpClient,
        robuxNodeDiscoveryProvider,
        NodeRole.PEON,
        "/simple/leader"
    );
    robuxLeaderClient.start();

    Request request = robuxLeaderClient.makeRequest(HttpMethod.POST, "/simple/direct");
    request.setContent("hello".getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals("hello", robuxLeaderClient.go(request).getContent());
  }

  @Test
  public void testNoLeaderFound() throws Exception
  {
    RobuxNodeDiscovery robuxNodeDiscovery = EasyMock.createMock(RobuxNodeDiscovery.class);
    EasyMock.expect(robuxNodeDiscovery.getAllNodes()).andReturn(ImmutableList.of());

    RobuxNodeDiscoveryProvider robuxNodeDiscoveryProvider = EasyMock.createMock(RobuxNodeDiscoveryProvider.class);
    EasyMock.expect(robuxNodeDiscoveryProvider.getForNodeRole(NodeRole.PEON)).andReturn(robuxNodeDiscovery);

    EasyMock.replay(robuxNodeDiscovery, robuxNodeDiscoveryProvider);

    RobuxLeaderClient robuxLeaderClient = new RobuxLeaderClient(
        httpClient,
        robuxNodeDiscoveryProvider,
        NodeRole.PEON,
        "/simple/leader"
    );
    robuxLeaderClient.start();

    expectedException.expect(IOException.class);
    expectedException.expectMessage(
        "A leader node could not be found for [PEON] service. "
        + "Check logs of service [PEON] to confirm it is healthy.");
    robuxLeaderClient.makeRequest(HttpMethod.POST, "/simple/direct");
  }

  @Test
  public void testRedirection() throws Exception
  {
    RobuxNodeDiscovery robuxNodeDiscovery = EasyMock.createMock(RobuxNodeDiscovery.class);
    EasyMock.expect(robuxNodeDiscovery.getAllNodes()).andReturn(
        ImmutableList.of(discoveryRobuxNode)
    );

    RobuxNodeDiscoveryProvider robuxNodeDiscoveryProvider = EasyMock.createMock(RobuxNodeDiscoveryProvider.class);
    EasyMock.expect(robuxNodeDiscoveryProvider.getForNodeRole(NodeRole.PEON)).andReturn(robuxNodeDiscovery);

    EasyMock.replay(robuxNodeDiscovery, robuxNodeDiscoveryProvider);

    RobuxLeaderClient robuxLeaderClient = new RobuxLeaderClient(
        httpClient,
        robuxNodeDiscoveryProvider,
        NodeRole.PEON,
        "/simple/leader"
    );
    robuxLeaderClient.start();

    Request request = robuxLeaderClient.makeRequest(HttpMethod.POST, "/simple/redirect");
    request.setContent("hello".getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals("hello", robuxLeaderClient.go(request).getContent());
  }

  @Test
  public void testServerFailureAndRedirect() throws Exception
  {
    RobuxNodeDiscovery robuxNodeDiscovery = EasyMock.createMock(RobuxNodeDiscovery.class);
    DiscoveryRobuxNode dummyNode = new DiscoveryRobuxNode(
        new RobuxNode("test", "dummyhost", false, 64231, null, true, false),
        NodeRole.PEON,
        ImmutableMap.of()
    );
    EasyMock.expect(robuxNodeDiscovery.getAllNodes()).andReturn(ImmutableList.of(dummyNode));
    EasyMock.expect(robuxNodeDiscovery.getAllNodes()).andReturn(ImmutableList.of(discoveryRobuxNode));

    RobuxNodeDiscoveryProvider robuxNodeDiscoveryProvider = EasyMock.createMock(RobuxNodeDiscoveryProvider.class);
    EasyMock.expect(robuxNodeDiscoveryProvider.getForNodeRole(NodeRole.PEON)).andReturn(robuxNodeDiscovery).anyTimes();

    EasyMock.replay(robuxNodeDiscovery, robuxNodeDiscoveryProvider);

    RobuxLeaderClient robuxLeaderClient = new RobuxLeaderClient(
        httpClient,
        robuxNodeDiscoveryProvider,
        NodeRole.PEON,
        "/simple/leader"
    );
    robuxLeaderClient.start();

    Request request = robuxLeaderClient.makeRequest(HttpMethod.POST, "/simple/redirect");
    request.setContent("hello".getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals("hello", robuxLeaderClient.go(request).getContent());
  }

  @Test
  public void test503ResponseFromServerAndCacheRefresh() throws Exception
  {
    RobuxNodeDiscovery robuxNodeDiscovery = EasyMock.createMock(RobuxNodeDiscovery.class);
    // Should be called twice. Second time is when we refresh the cache since we get 503 in the first request
    EasyMock.expect(robuxNodeDiscovery.getAllNodes()).andReturn(ImmutableList.of(discoveryRobuxNode)).times(2);

    RobuxNodeDiscoveryProvider robuxNodeDiscoveryProvider = EasyMock.createMock(RobuxNodeDiscoveryProvider.class);
    EasyMock.expect(robuxNodeDiscoveryProvider.getForNodeRole(NodeRole.PEON)).andReturn(robuxNodeDiscovery).anyTimes();

    ListenableFutureTask task = EasyMock.createMock(ListenableFutureTask.class);
    EasyMock.expect(task.get()).andReturn(new StringFullResponseHolder(new DefaultHttpResponse(
        HttpVersion.HTTP_1_1,
        HttpResponseStatus.SERVICE_UNAVAILABLE
    ), Charset.defaultCharset()));
    EasyMock.replay(robuxNodeDiscovery, robuxNodeDiscoveryProvider, task);

    HttpClient spyHttpClient = Mockito.spy(this.httpClient);
    // Override behavior for the first call only
    Mockito.doReturn(task).doCallRealMethod().when(spyHttpClient).go(ArgumentMatchers.any(), ArgumentMatchers.any());

    RobuxLeaderClient robuxLeaderClient = new RobuxLeaderClient(
        spyHttpClient,
        robuxNodeDiscoveryProvider,
        NodeRole.PEON,
        "/simple/leader"
    );
    robuxLeaderClient.start();

    Request request = robuxLeaderClient.makeRequest(HttpMethod.POST, "/simple/direct");
    request.setContent("hello".getBytes(StandardCharsets.UTF_8));
    Assert.assertEquals("hello", robuxLeaderClient.go(request).getContent());
    EasyMock.verify(robuxNodeDiscovery);
  }

  @Test
  public void testFindCurrentLeader()
  {
    RobuxNodeDiscovery robuxNodeDiscovery = EasyMock.createMock(RobuxNodeDiscovery.class);
    EasyMock.expect(robuxNodeDiscovery.getAllNodes()).andReturn(
        ImmutableList.of(discoveryRobuxNode)
    );

    RobuxNodeDiscoveryProvider robuxNodeDiscoveryProvider = EasyMock.createMock(RobuxNodeDiscoveryProvider.class);
    EasyMock.expect(robuxNodeDiscoveryProvider.getForNodeRole(NodeRole.PEON)).andReturn(robuxNodeDiscovery);

    EasyMock.replay(robuxNodeDiscovery, robuxNodeDiscoveryProvider);

    RobuxLeaderClient robuxLeaderClient = new RobuxLeaderClient(
        httpClient,
        robuxNodeDiscoveryProvider,
        NodeRole.PEON,
        "/simple/leader"
    );
    robuxLeaderClient.start();

    Assert.assertEquals("http://localhost:1234/", robuxLeaderClient.findCurrentLeader());
  }

  static class TestJettyServerInitializer implements JettyServerInitializer
  {
    @Override
    public void initialize(Server server, Injector injector)
    {
      final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
      root.addServlet(new ServletHolder(new DefaultServlet()), "/*");
      root.addFilter(GuiceFilter.class, "/*", null);

      final HandlerList handlerList = new HandlerList();
      handlerList.setHandlers(new Handler[]{root});
      server.setHandler(handlerList);
    }
  }

  @Path("/simple")
  public static class SimpleResource
  {
    private final int port;

    @Inject
    public SimpleResource(@Named("port") int port)
    {
      this.port = port;
    }

    @POST
    @Path("/direct")
    @Produces(MediaType.APPLICATION_JSON)
    public Response direct(String input)
    {
      if ("hello".equals(input)) {
        return Response.ok("hello").build();
      } else {
        return Response.serverError().build();
      }
    }

    @POST
    @Path("/redirect")
    @Produces(MediaType.APPLICATION_JSON)
    public Response redirecting() throws Exception
    {
      return Response.temporaryRedirect(new URI(StringUtils.format("http://localhost:%s/simple/direct", port))).build();
    }

    @GET
    @Path("/leader")
    @Produces(MediaType.APPLICATION_JSON)
    public Response leader()
    {
      return Response.ok("http://localhost:1234/").build();
    }
  }
}
