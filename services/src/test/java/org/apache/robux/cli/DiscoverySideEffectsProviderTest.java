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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.multibindings.ProvidesIntoSet;
import com.google.inject.name.Named;
import org.apache.robux.cli.ServerRunnable.DiscoverySideEffectsProvider;
import org.apache.robux.curator.discovery.ServiceAnnouncer;
import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.discovery.RobuxNodeAnnouncer;
import org.apache.robux.discovery.RobuxService;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.AbstractRobuxServiceModule;
import org.apache.robux.guice.StartupInjectorBuilder;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.initialization.ServerInjectorBuilder;
import org.apache.robux.java.util.common.lifecycle.Lifecycle;
import org.apache.robux.server.RobuxNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class DiscoverySideEffectsProviderTest
{
  private NodeRole nodeRole;
  @Mock
  private RobuxNode robuxNode;
  /**
   * This announcer is mocked to fail if it tries to announce a Robux service that is not discoverable.
   */
  @Mock
  private RobuxNodeAnnouncer discoverableOnlyAnnouncer;
  @Mock
  private ServiceAnnouncer legacyAnnouncer;
  @Mock
  private Lifecycle lifecycle;
  private List<Lifecycle.Handler> lifecycleHandlers;

  private ServerRunnable.DiscoverySideEffectsProvider target;

  @Before
  public void setUp()
  {
    nodeRole = NodeRole.HISTORICAL;
    lifecycleHandlers = new ArrayList<>();
    Mockito.doAnswer((invocation) -> {
      DiscoveryRobuxNode discoveryRobuxNode = invocation.getArgument(0);
      boolean isAllServicesDiscoverable =
          discoveryRobuxNode.getServices().values().stream().allMatch(RobuxService::isDiscoverable);
      Assert.assertTrue(isAllServicesDiscoverable);
      return null;
    }).when(discoverableOnlyAnnouncer).announce(ArgumentMatchers.any(DiscoveryRobuxNode.class));
    Mockito
        .doAnswer((invocation) -> lifecycleHandlers.add(invocation.getArgument(0)))
        .when(lifecycle)
        .addHandler(ArgumentMatchers.any(Lifecycle.Handler.class), ArgumentMatchers.eq(Lifecycle.Stage.ANNOUNCEMENTS));
    target = DiscoverySideEffectsProvider.withLegacyAnnouncer();
  }

  @Test
  public void testGetShouldAddAnnouncementsForDiscoverableServices() throws Exception
  {
    createInjector(
        ImmutableList.of(new DiscoverableServiceTestModule(), new UndiscoverableServiceTestModule())
    ).injectMembers(target);
    ServerRunnable.DiscoverySideEffectsProvider.Child child = target.get();
    Assert.assertNotNull(child);
    Assert.assertEquals(1, lifecycleHandlers.size());
    // Start the lifecycle handler. This will make announcements via the announcer
    lifecycleHandlers.get(0).start();
  }

  @Test
  public void testInjectWithEmptyRobuxService() throws Exception
  {
    createInjector(ImmutableList.of()).injectMembers(target);
    ServerRunnable.DiscoverySideEffectsProvider.Child child = target.get();
    Assert.assertNotNull(child);
    Assert.assertEquals(1, lifecycleHandlers.size());
    // Start the lifecycle handler. This will make announcements via the announcer
    lifecycleHandlers.get(0).start();
  }

  /**
   * Dummy service which is discoverable.
   */
  private static class DiscoverableRobuxService extends RobuxService
  {
    @Override
    public String getName()
    {
      return "DiscoverableRobuxService";
    }

    @Override
    public boolean isDiscoverable()
    {
      return true;
    }
  }

  /**
   * Dummy service which is not discoverable.
   */
  private static class UndiscoverableRobuxService extends RobuxService
  {
    @Override
    public String getName()
    {
      return "UnDiscoverableRobuxService";
    }

    @Override
    public boolean isDiscoverable()
    {
      return false;
    }
  }

  private static class DiscoverableServiceTestModule extends AbstractRobuxServiceModule
  {
    @ProvidesIntoSet
    @Named("historical")
    public Class<? extends RobuxService> getDiscoverableRobuxService()
    {
      return DiscoverableRobuxService.class;
    }

    @Override
    protected NodeRole getNodeRoleKey()
    {
      return NodeRole.HISTORICAL;
    }
  }

  private static class UndiscoverableServiceTestModule extends AbstractRobuxServiceModule
  {
    @ProvidesIntoSet
    @Named("historical")
    public Class<? extends RobuxService> getUndiscoverableRobuxService()
    {
      return UndiscoverableRobuxService.class;
    }

    @Override
    protected NodeRole getNodeRoleKey()
    {
      return NodeRole.HISTORICAL;
    }
  }

  private Injector createInjector(List<Module> modules)
  {
    return new StartupInjectorBuilder()
        .add(
            ServerInjectorBuilder.registerNodeRoleModule(ImmutableSet.of(nodeRole)),
            binder -> {
              binder.bind(RobuxNodeAnnouncer.class).toInstance(discoverableOnlyAnnouncer);
              binder.bind(RobuxNode.class).annotatedWith(Self.class).toInstance(robuxNode);
              binder.bind(ServiceAnnouncer.class).toInstance(legacyAnnouncer);
              binder.bind(Lifecycle.class).toInstance(lifecycle);
            }
         )
        .addAll(modules)
        .build();
  }
}
