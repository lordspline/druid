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

package org.apache.robux.sql.calcite.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.apache.robux.client.FilteredServerInventoryView;
import org.apache.robux.client.TimelineServerView;
import org.apache.robux.client.coordinator.Coordinator;
import org.apache.robux.client.coordinator.CoordinatorClient;
import org.apache.robux.client.coordinator.NoopCoordinatorClient;
import org.apache.robux.client.indexing.IndexingService;
import org.apache.robux.discovery.RobuxLeaderClient;
import org.apache.robux.discovery.RobuxNodeDiscoveryProvider;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.LifecycleModule;
import org.apache.robux.guice.annotations.Json;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.robux.query.lookup.LookupReferencesManager;
import org.apache.robux.rpc.indexing.NoopOverlordClient;
import org.apache.robux.rpc.indexing.OverlordClient;
import org.apache.robux.segment.join.JoinableFactory;
import org.apache.robux.segment.join.MapJoinableFactory;
import org.apache.robux.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.robux.server.QueryLifecycleFactory;
import org.apache.robux.server.SegmentManager;
import org.apache.robux.server.security.AuthorizerMapper;
import org.apache.robux.server.security.Escalator;
import org.apache.robux.sql.calcite.planner.CatalogResolver;
import org.apache.robux.sql.calcite.planner.RobuxOperatorTable;
import org.apache.robux.sql.calcite.planner.PlannerConfig;
import org.apache.robux.sql.calcite.util.CalciteTestBase;
import org.apache.robux.sql.calcite.view.ViewManager;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Set;
import java.util.stream.Collectors;

@ExtendWith(EasyMockExtension.class)
public class RobuxCalciteSchemaModuleTest extends CalciteTestBase
{
  private static final String ROBUX_SCHEMA_NAME = "robux";

  @Mock
  private QueryLifecycleFactory queryLifecycleFactory;
  @Mock
  private TimelineServerView serverView;
  @Mock
  private PlannerConfig plannerConfig;
  @Mock
  private ViewManager viewManager;
  @Mock
  private Escalator escalator;
  @Mock
  AuthorizerMapper authorizerMapper;
  @Mock
  private FilteredServerInventoryView serverInventoryView;
  @Mock
  private RobuxLeaderClient coordinatorRobuxLeaderClient;
  @Mock
  private RobuxLeaderClient overlordRobuxLeaderClient;
  @Mock
  private RobuxNodeDiscoveryProvider robuxNodeDiscoveryProvider;
  @Mock
  private ObjectMapper objectMapper;
  @Mock
  private LookupReferencesManager lookupReferencesManager;
  @Mock
  private SegmentManager segmentManager;
  @Mock
  private RobuxOperatorTable robuxOperatorTable;

  private RobuxCalciteSchemaModule target;
  private Injector injector;

  @BeforeEach
  public void setUp()
  {
    EasyMock.replay(plannerConfig);
    target = new RobuxCalciteSchemaModule();
    injector = Guice.createInjector(
        binder -> {
          binder.bind(QueryLifecycleFactory.class).toInstance(queryLifecycleFactory);
          binder.bind(TimelineServerView.class).toInstance(serverView);
          binder.bind(JoinableFactory.class).toInstance(new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()));
          binder.bind(PlannerConfig.class).toInstance(plannerConfig);
          binder.bind(ViewManager.class).toInstance(viewManager);
          binder.bind(Escalator.class).toInstance(escalator);
          binder.bind(AuthorizerMapper.class).toInstance(authorizerMapper);
          binder.bind(FilteredServerInventoryView.class).toInstance(serverInventoryView);
          binder.bind(SegmentManager.class).toInstance(segmentManager);
          binder.bind(RobuxOperatorTable.class).toInstance(robuxOperatorTable);
          binder.bind(RobuxLeaderClient.class)
                .annotatedWith(Coordinator.class)
                .toInstance(coordinatorRobuxLeaderClient);
          binder.bind(RobuxLeaderClient.class)
                .annotatedWith(IndexingService.class)
                .toInstance(overlordRobuxLeaderClient);
          binder.bind(RobuxNodeDiscoveryProvider.class).toInstance(robuxNodeDiscoveryProvider);
          binder.bind(RobuxSchemaManager.class).toInstance(new NoopRobuxSchemaManager());
          binder.bind(ObjectMapper.class).annotatedWith(Json.class).toInstance(objectMapper);
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
          binder.bind(LookupExtractorFactoryContainerProvider.class).toInstance(lookupReferencesManager);
          binder.bind(CatalogResolver.class).toInstance(CatalogResolver.NULL_RESOLVER);
          binder.bind(ServiceEmitter.class).toInstance(new ServiceEmitter("", "", null));
          binder.bind(OverlordClient.class).to(NoopOverlordClient.class);
          binder.bind(CoordinatorClient.class).to(NoopCoordinatorClient.class);
          binder.bind(CentralizedDatasourceSchemaConfig.class)
                .toInstance(CentralizedDatasourceSchemaConfig.create());
        },
        new LifecycleModule(),
        target);
  }

  @Test
  public void testRobuxSchemaNameIsInjected()
  {
    String schemaName = injector.getInstance(Key.get(String.class, RobuxSchemaName.class));
    Assert.assertEquals(ROBUX_SCHEMA_NAME, schemaName);
  }

  @Test
  public void testRobuxSqlSchemaIsInjectedAsSingleton()
  {
    NamedRobuxSchema namedRobuxSchema = injector.getInstance(NamedRobuxSchema.class);
    Assert.assertNotNull(namedRobuxSchema);
    NamedRobuxSchema other = injector.getInstance(NamedRobuxSchema.class);
    Assert.assertSame(other, namedRobuxSchema);
  }

  @Test
  public void testSystemSqlSchemaIsInjectedAsSingleton()
  {
    NamedSystemSchema namedSystemSchema = injector.getInstance(NamedSystemSchema.class);
    Assert.assertNotNull(namedSystemSchema);
    NamedSystemSchema other = injector.getInstance(NamedSystemSchema.class);
    Assert.assertSame(other, namedSystemSchema);
  }

  @Test
  public void testRobuxCalciteSchemasAreInjected()
  {
    Set<NamedSchema> sqlSchemas = injector.getInstance(Key.get(new TypeLiteral<>() {}));
    Set<Class<? extends NamedSchema>> expectedSchemas =
        ImmutableSet.of(NamedSystemSchema.class, NamedRobuxSchema.class, NamedLookupSchema.class, NamedViewSchema.class);
    Assert.assertEquals(expectedSchemas.size(), sqlSchemas.size());
    Assert.assertEquals(
        expectedSchemas,
        sqlSchemas.stream().map(NamedSchema::getClass).collect(Collectors.toSet()));
  }

  @Test
  public void testRobuxSchemaIsInjectedAsSingleton()
  {
    RobuxSchema schema = injector.getInstance(RobuxSchema.class);
    Assert.assertNotNull(schema);
    RobuxSchema other = injector.getInstance(RobuxSchema.class);
    Assert.assertSame(other, schema);
  }

  @Test
  public void testSystemSchemaIsInjectedAsSingleton()
  {
    SystemSchema schema = injector.getInstance(SystemSchema.class);
    Assert.assertNotNull(schema);
    SystemSchema other = injector.getInstance(SystemSchema.class);
    Assert.assertSame(other, schema);
  }

  @Test
  public void testInformationSchemaIsInjectedAsSingleton()
  {
    InformationSchema schema = injector.getInstance(InformationSchema.class);
    Assert.assertNotNull(schema);
    InformationSchema other = injector.getInstance(InformationSchema.class);
    Assert.assertSame(other, schema);
  }

  @Test
  public void testLookupSchemaIsInjectedAsSingleton()
  {
    LookupSchema schema = injector.getInstance(LookupSchema.class);
    Assert.assertNotNull(schema);
    LookupSchema other = injector.getInstance(LookupSchema.class);
    Assert.assertSame(other, schema);
  }

  @Test
  public void testRootSchemaAnnotatedIsInjectedAsSingleton()
  {
    RobuxSchemaCatalog rootSchema = injector.getInstance(
        Key.get(RobuxSchemaCatalog.class, Names.named(RobuxCalciteSchemaModule.INCOMPLETE_SCHEMA))
    );
    Assert.assertNotNull(rootSchema);
    RobuxSchemaCatalog other = injector.getInstance(
        Key.get(RobuxSchemaCatalog.class, Names.named(RobuxCalciteSchemaModule.INCOMPLETE_SCHEMA))
    );
    Assert.assertSame(other, rootSchema);
  }

  @Test
  public void testRootSchemaIsInjectedAsSingleton()
  {
    RobuxSchemaCatalog rootSchema = injector.getInstance(Key.get(RobuxSchemaCatalog.class));
    Assert.assertNotNull(rootSchema);
    RobuxSchemaCatalog other = injector.getInstance(
        Key.get(RobuxSchemaCatalog.class, Names.named(RobuxCalciteSchemaModule.INCOMPLETE_SCHEMA))
    );
    Assert.assertSame(other, rootSchema);
  }

  @Test
  public void testRootSchemaIsInjectedAndHasInformationSchema()
  {
    RobuxSchemaCatalog rootSchema = injector.getInstance(Key.get(RobuxSchemaCatalog.class));
    InformationSchema expectedSchema = injector.getInstance(InformationSchema.class);
    Assert.assertNotNull(rootSchema);
    Assert.assertSame(expectedSchema, rootSchema.getSubSchema("INFORMATION_SCHEMA").unwrap(InformationSchema.class));
  }
}
