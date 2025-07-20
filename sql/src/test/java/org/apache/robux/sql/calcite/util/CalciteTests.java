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

package org.apache.robux.sql.calcite.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.robux.client.BrokerSegmentWatcherConfig;
import org.apache.robux.client.RobuxServer;
import org.apache.robux.client.FilteredServerInventoryView;
import org.apache.robux.client.ServerInventoryView;
import org.apache.robux.client.ServerView;
import org.apache.robux.client.TimelineServerView;
import org.apache.robux.client.coordinator.CoordinatorClient;
import org.apache.robux.client.coordinator.NoopCoordinatorClient;
import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.discovery.RobuxNodeDiscovery;
import org.apache.robux.discovery.RobuxNodeDiscoveryProvider;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.annotations.Json;
import org.apache.robux.indexer.RunnerTaskState;
import org.apache.robux.indexer.TaskLocation;
import org.apache.robux.indexer.TaskState;
import org.apache.robux.indexer.TaskStatusPlus;
import org.apache.robux.java.util.common.CloseableIterators;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.common.Pair;
import org.apache.robux.java.util.common.parsers.CloseableIterator;
import org.apache.robux.java.util.http.client.HttpClient;
import org.apache.robux.java.util.http.client.Request;
import org.apache.robux.java.util.http.client.response.HttpResponseHandler;
import org.apache.robux.math.expr.ExprMacroTable;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.QuerySegmentWalker;
import org.apache.robux.query.policy.NoRestrictionPolicy;
import org.apache.robux.query.policy.Policy;
import org.apache.robux.query.policy.RowFilterPolicy;
import org.apache.robux.rpc.indexing.NoopOverlordClient;
import org.apache.robux.rpc.indexing.OverlordClient;
import org.apache.robux.segment.column.ColumnType;
import org.apache.robux.segment.join.JoinableFactory;
import org.apache.robux.segment.join.JoinableFactoryWrapper;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.QueryLifecycleFactory;
import org.apache.robux.server.QueryScheduler;
import org.apache.robux.server.QueryStackTests;
import org.apache.robux.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.robux.server.coordination.RobuxServerMetadata;
import org.apache.robux.server.security.Access;
import org.apache.robux.server.security.Action;
import org.apache.robux.server.security.AllowAllAuthenticator;
import org.apache.robux.server.security.AuthConfig;
import org.apache.robux.server.security.AuthenticationResult;
import org.apache.robux.server.security.Authenticator;
import org.apache.robux.server.security.AuthenticatorMapper;
import org.apache.robux.server.security.Authorizer;
import org.apache.robux.server.security.AuthorizerMapper;
import org.apache.robux.server.security.Escalator;
import org.apache.robux.server.security.NoopEscalator;
import org.apache.robux.server.security.ResourceType;
import org.apache.robux.sql.SqlStatementFactory;
import org.apache.robux.sql.calcite.BaseCalciteQueryTest;
import org.apache.robux.sql.calcite.aggregation.SqlAggregationModule;
import org.apache.robux.sql.calcite.planner.RobuxOperatorTable;
import org.apache.robux.sql.calcite.planner.PlannerConfig;
import org.apache.robux.sql.calcite.run.NativeSqlEngine;
import org.apache.robux.sql.calcite.schema.BrokerSegmentMetadataCacheConfig;
import org.apache.robux.sql.calcite.schema.RobuxSchema;
import org.apache.robux.sql.calcite.schema.RobuxSchemaCatalog;
import org.apache.robux.sql.calcite.schema.MetadataSegmentView;
import org.apache.robux.sql.calcite.schema.SystemSchema;
import org.apache.robux.sql.calcite.util.testoperator.CalciteTestOperatorModule;
import org.apache.robux.timeline.DataSegment;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

/**
 * Utility functions for Calcite tests.
 */
public class CalciteTests
{
  public static final String DATASOURCE1 = "foo";
  public static final String DATASOURCE2 = "foo2";
  public static final String DATASOURCE3 = "numfoo";
  public static final String DATASOURCE4 = "foo4";
  public static final String DATASOURCE5 = "lotsocolumns";
  public static final String ARRAYS_DATASOURCE = "arrays";
  public static final String BROADCAST_DATASOURCE = "broadcast";
  public static final String FORBIDDEN_DATASOURCE = "forbiddenDatasource";
  public static final String RESTRICTED_DATASOURCE = "restrictedDatasource_m1_is_6";
  public static final String RESTRICTED_BROADCAST_DATASOURCE = "restrictedBroadcastDatasource_m1_is_6";
  public static final String FORBIDDEN_DESTINATION = "forbiddenDestination";
  public static final String SOME_DATASOURCE = "some_datasource";
  public static final String SOME_DATSOURCE_ESCAPED = "some\\_datasource";
  public static final String SOMEXDATASOURCE = "somexdatasource";
  public static final String USERVISITDATASOURCE = "visits";
  public static final String ROBUX_SCHEMA_NAME = "robux";
  public static final String WIKIPEDIA = "wikipedia";
  public static final String WIKIPEDIA_FIRST_LAST = "wikipedia_first_last";
  public static final String TBL_WITH_NULLS_PARQUET = "tblWnulls.parquet";
  public static final String SML_TBL_PARQUET = "smlTbl.parquet";
  public static final String ALL_TYPES_UNIQ_PARQUET = "allTypsUniq.parquet";
  public static final String FEW_ROWS_ALL_DATA_PARQUET = "fewRowsAllData.parquet";
  public static final String T_ALL_TYPE_PARQUET = "t_alltype.parquet";
  public static final String BENCHMARK_DATASOURCE = "benchmark_ds";

  public static final String TEST_SUPERUSER_NAME = "testSuperuser";
  public static final Policy POLICY_NO_RESTRICTION_SUPERUSER = NoRestrictionPolicy.instance();
  public static final Policy POLICY_RESTRICTION = RowFilterPolicy.from(
      BaseCalciteQueryTest.equality("m1", 6, ColumnType.LONG)
  );
  public static final AuthorizerMapper TEST_AUTHORIZER_MAPPER = new AuthorizerMapper(null)
  {
    @Override
    public Authorizer getAuthorizer(String name)
    {
      return (authenticationResult, resource, action) -> {
        boolean readRestrictedTable = ImmutableSet.of(RESTRICTED_DATASOURCE, RESTRICTED_BROADCAST_DATASOURCE)
                                                  .contains(resource.getName()) && action.equals(Action.READ);

        if (TEST_SUPERUSER_NAME.equals(authenticationResult.getIdentity())) {
          return readRestrictedTable ? Access.allowWithRestriction(POLICY_NO_RESTRICTION_SUPERUSER) : Access.OK;
        }

        switch (resource.getType()) {
          case ResourceType.DATASOURCE:
            switch (resource.getName()) {
              case FORBIDDEN_DATASOURCE:
                return Access.DENIED;
              default:
                return readRestrictedTable ? Access.allowWithRestriction(POLICY_RESTRICTION) : Access.OK;
            }
          case ResourceType.VIEW:
            if ("forbiddenView".equals(resource.getName())) {
              return Access.DENIED;
            } else {
              return Access.OK;
            }
          case ResourceType.QUERY_CONTEXT:
            return Access.OK;
          case ResourceType.EXTERNAL:
            if (Action.WRITE.equals(action)) {
              if (FORBIDDEN_DESTINATION.equals(resource.getName())) {
                return Access.DENIED;
              } else {
                return Access.OK;
              }
            }
            return Access.DENIED;
          default:
            return Access.DENIED;
        }
      };
    }
  };

  public static final AuthorizerMapper TEST_EXTERNAL_AUTHORIZER_MAPPER = new AuthorizerMapper(null)
  {
    @Override
    public Authorizer getAuthorizer(String name)
    {
      return (authenticationResult, resource, action) -> {
        boolean readRestrictedTable = ImmutableSet.of(RESTRICTED_DATASOURCE, RESTRICTED_BROADCAST_DATASOURCE)
                                                  .contains(resource.getName()) && action.equals(Action.READ);

        if (TEST_SUPERUSER_NAME.equals(authenticationResult.getIdentity())) {
          return readRestrictedTable ? Access.allowWithRestriction(POLICY_NO_RESTRICTION_SUPERUSER) : Access.OK;
        }

        switch (resource.getType()) {
          case ResourceType.DATASOURCE:
            if (FORBIDDEN_DATASOURCE.equals(resource.getName())) {
              return Access.DENIED;
            } else {
              return readRestrictedTable ? Access.allowWithRestriction(POLICY_RESTRICTION) : Access.OK;
            }
          case ResourceType.VIEW:
            if ("forbiddenView".equals(resource.getName())) {
              return Access.DENIED;
            } else {
              return Access.OK;
            }
          case ResourceType.QUERY_CONTEXT:
          case ResourceType.EXTERNAL:
            return Access.OK;
          default:
            return Access.DENIED;
        }
      };
    }
  };

  public static final AuthenticatorMapper TEST_AUTHENTICATOR_MAPPER;

  static {
    final Map<String, Authenticator> defaultMap = new HashMap<>();
    defaultMap.put(
        AuthConfig.ALLOW_ALL_NAME,
        new AllowAllAuthenticator()
        {
          @Override
          public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
          {
            return new AuthenticationResult((String) context.get("user"), AuthConfig.ALLOW_ALL_NAME, null, null);
          }
        }
    );
    TEST_AUTHENTICATOR_MAPPER = new AuthenticatorMapper(defaultMap);
  }

  public static final Escalator TEST_AUTHENTICATOR_ESCALATOR;

  static {
    TEST_AUTHENTICATOR_ESCALATOR = new NoopEscalator()
    {

      @Override
      public AuthenticationResult createEscalatedAuthenticationResult()
      {
        return SUPER_USER_AUTH_RESULT;
      }
    };
  }

  public static final AuthenticationResult REGULAR_USER_AUTH_RESULT = new AuthenticationResult(
      AuthConfig.ALLOW_ALL_NAME,
      AuthConfig.ALLOW_ALL_NAME,
      null,
      null
  );

  public static final AuthenticationResult SUPER_USER_AUTH_RESULT = new AuthenticationResult(
      TEST_SUPERUSER_NAME,
      AuthConfig.ALLOW_ALL_NAME,
      null,
      null
  );

  public static final Injector INJECTOR = QueryStackTests.defaultInjectorBuilder()
                                                         .addModule(new LookylooModule())
                                                         .addModule(new SqlAggregationModule())
                                                         .addModule(new CalciteTestOperatorModule())
                                                         .build();

  private CalciteTests()
  {
    // No instantiation.
  }

  public static NativeSqlEngine createMockSqlEngine(
      final QuerySegmentWalker walker,
      final QueryRunnerFactoryConglomerate conglomerate
  )
  {
    return createMockSqlEngine(walker, conglomerate, null);
  }

  public static NativeSqlEngine createMockSqlEngine(
      final QuerySegmentWalker walker,
      final QueryRunnerFactoryConglomerate conglomerate,
      final SqlStatementFactory sqlStatementFactory
  )
  {
    return new NativeSqlEngine(
        createMockQueryLifecycleFactory(walker, conglomerate),
        getJsonMapper(),
        sqlStatementFactory
    );
  }

  public static QueryLifecycleFactory createMockQueryLifecycleFactory(
      final QuerySegmentWalker walker,
      final QueryRunnerFactoryConglomerate conglomerate
  )
  {
    return QueryFrameworkUtils.createMockQueryLifecycleFactory(
        walker,
        conglomerate,
        CalciteTests.TEST_AUTHORIZER_MAPPER
    );
  }

  public static ObjectMapper getJsonMapper()
  {
    return INJECTOR.getInstance(Key.get(ObjectMapper.class, Json.class));
  }

  public static SpecificSegmentsQuerySegmentWalker createMockWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final File tmpDir
  )
  {
    return TestDataBuilder.createMockWalker(INJECTOR, conglomerate, tmpDir);
  }

  public static SpecificSegmentsQuerySegmentWalker createMockWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final File tmpDir,
      final QueryScheduler scheduler
  )
  {
    return TestDataBuilder.createMockWalker(INJECTOR, conglomerate, tmpDir, scheduler);
  }

  public static SpecificSegmentsQuerySegmentWalker createMockWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final File tmpDir,
      final QueryScheduler scheduler,
      final JoinableFactory joinableFactory
  )
  {
    return TestDataBuilder.createMockWalker(
        INJECTOR,
        conglomerate,
        tmpDir,
        scheduler,
        joinableFactory
    );
  }

  public static SpecificSegmentsQuerySegmentWalker createMockWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final File tmpDir,
      final QueryScheduler scheduler,
      final JoinableFactoryWrapper joinableFactoryWrapper
  )
  {
    return TestDataBuilder.createMockWalker(
        INJECTOR,
        conglomerate,
        tmpDir,
        scheduler,
        joinableFactoryWrapper
    );
  }

  public static ExprMacroTable createExprMacroTable()
  {
    return INJECTOR.getInstance(ExprMacroTable.class);
  }

  public static JoinableFactoryWrapper createJoinableFactoryWrapper()
  {
    return new JoinableFactoryWrapper(QueryFrameworkUtils.createDefaultJoinableFactory(INJECTOR));
  }

  public static RobuxOperatorTable createOperatorTable()
  {
    return QueryFrameworkUtils.createOperatorTable(INJECTOR);
  }


  public static RobuxNode mockCoordinatorNode()
  {
    return new RobuxNode("test-coordinator", "dummy", false, 8081, null, true, false);
  }

  public static FakeRobuxNodeDiscoveryProvider mockRobuxNodeDiscoveryProvider(final RobuxNode coordinatorNode)
  {
    FakeRobuxNodeDiscoveryProvider provider = new FakeRobuxNodeDiscoveryProvider(
        ImmutableMap.of(
            NodeRole.COORDINATOR, new FakeRobuxNodeDiscovery(ImmutableMap.of(NodeRole.COORDINATOR, coordinatorNode))
        )
    );
    return provider;
  }

  public static SystemSchema createMockSystemSchema(
      final RobuxSchema robuxSchema,
      final SpecificSegmentsQuerySegmentWalker walker,
      final AuthorizerMapper authorizerMapper
  )
  {
    return createMockSystemSchema(robuxSchema, new TestTimelineServerView(walker.getSegments()), authorizerMapper);
  }

  public static SystemSchema createMockSystemSchema(
      final RobuxSchema robuxSchema,
      final TimelineServerView timelineServerView,
      final AuthorizerMapper authorizerMapper
  )
  {
    final RobuxNode coordinatorNode = mockCoordinatorNode();
    FakeRobuxNodeDiscoveryProvider provider = mockRobuxNodeDiscoveryProvider(coordinatorNode);

    final RobuxNode overlordNode = new RobuxNode("test-overlord", "dummy", false, 8090, null, true, false);

    final CoordinatorClient coordinatorClient = new NoopCoordinatorClient()
    {
      @Override
      public ListenableFuture<URI> findCurrentLeader()
      {
        try {
          return Futures.immediateFuture(new URI(coordinatorNode.getHostAndPortToUse()));
        }
        catch (URISyntaxException e) {
          throw new RuntimeException(e);
        }
      }
    };

    final OverlordClient overlordClient = new NoopOverlordClient()
    {
      @Override
      public ListenableFuture<URI> findCurrentLeader()
      {
        try {
          return Futures.immediateFuture(new URI(overlordNode.getHostAndPortToUse()));
        }
        catch (URISyntaxException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public ListenableFuture<CloseableIterator<TaskStatusPlus>> taskStatuses(
          @Nullable String state,
          @Nullable String dataSource,
          @Nullable Integer maxCompletedTasks
      )
      {
        List<TaskStatusPlus> tasks = new ArrayList<>();
        tasks.add(createTaskStatus("id1", DATASOURCE1, 10L));
        tasks.add(createTaskStatus("id1", DATASOURCE1, 1L));
        tasks.add(createTaskStatus("id2", DATASOURCE2, 20L));
        tasks.add(createTaskStatus("id2", DATASOURCE2, 2L));
        return Futures.immediateFuture(CloseableIterators.withEmptyBaggage(tasks.iterator()));
      }

      private TaskStatusPlus createTaskStatus(String id, String datasource, Long duration)
      {
        return new TaskStatusPlus(
            id,
            "testGroupId",
            "testType",
            DateTimes.nowUtc(),
            DateTimes.nowUtc(),
            TaskState.RUNNING,
            RunnerTaskState.RUNNING,
            duration,
            TaskLocation.create("testHost", 1010, -1),
            datasource,
            null
        );
      }
    };

    return new SystemSchema(
        robuxSchema,
        new MetadataSegmentView(
            coordinatorClient,
            new BrokerSegmentWatcherConfig(),
            BrokerSegmentMetadataCacheConfig.create()
        ),
        timelineServerView,
        new FakeServerInventoryView(),
        authorizerMapper,
        coordinatorClient,
        overlordClient,
        provider,
        getJsonMapper()
    );
  }

  public static RobuxSchemaCatalog createMockRootSchema(
      final QueryRunnerFactoryConglomerate conglomerate,
      final SpecificSegmentsQuerySegmentWalker walker,
      final PlannerConfig plannerConfig,
      final AuthorizerMapper authorizerMapper
  )
  {
    return QueryFrameworkUtils.createMockRootSchema(
        INJECTOR,
        conglomerate,
        walker,
        plannerConfig,
        authorizerMapper
    );
  }

  /**
   * A fake {@link HttpClient} for {@link #createMockSystemSchema}.
   */
  private static class FakeHttpClient implements HttpClient
  {
    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> handler
    )
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> handler,
        Duration readTimeout
    )
    {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * A fake {@link RobuxNodeDiscoveryProvider} for {@link #createMockSystemSchema}.
   */
  public static class FakeRobuxNodeDiscoveryProvider extends RobuxNodeDiscoveryProvider
  {
    private final Map<NodeRole, FakeRobuxNodeDiscovery> nodeDiscoveries;

    public FakeRobuxNodeDiscoveryProvider(Map<NodeRole, FakeRobuxNodeDiscovery> nodeDiscoveries)
    {
      this.nodeDiscoveries = nodeDiscoveries;
    }

    @Override
    public BooleanSupplier getForNode(RobuxNode node, NodeRole nodeRole)
    {
      boolean get = nodeDiscoveries.getOrDefault(nodeRole, new FakeRobuxNodeDiscovery())
                                   .getAllNodes()
                                   .stream()
                                   .anyMatch(x -> x.getRobuxNode().equals(node));
      return () -> get;
    }

    @Override
    public RobuxNodeDiscovery getForNodeRole(NodeRole nodeRole)
    {
      return nodeDiscoveries.getOrDefault(nodeRole, new FakeRobuxNodeDiscovery());
    }
  }

  private static class FakeRobuxNodeDiscovery implements RobuxNodeDiscovery
  {
    private final Set<DiscoveryRobuxNode> nodes;

    FakeRobuxNodeDiscovery()
    {
      this.nodes = new HashSet<>();
    }

    FakeRobuxNodeDiscovery(Map<NodeRole, RobuxNode> nodes)
    {
      this.nodes = Sets.newHashSetWithExpectedSize(nodes.size());
      nodes.forEach((k, v) -> {
        addNode(v, k);
      });
    }

    @Override
    public Collection<DiscoveryRobuxNode> getAllNodes()
    {
      return nodes;
    }

    void addNode(RobuxNode node, NodeRole role)
    {
      final DiscoveryRobuxNode discoveryNode = new DiscoveryRobuxNode(node, role, ImmutableMap.of());
      this.nodes.add(discoveryNode);
    }

    @Override
    public void registerListener(Listener listener)
    {

    }
  }

  /**
   * A fake {@link ServerInventoryView} for {@link #createMockSystemSchema}.
   */
  private static class FakeServerInventoryView implements FilteredServerInventoryView
  {
    @Nullable
    @Override
    public RobuxServer getInventoryValue(String serverKey)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<RobuxServer> getInventory()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isStarted()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSegmentLoadedByServer(String serverKey, DataSegment segment)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void registerSegmentCallback(
        Executor exec,
        ServerView.SegmentCallback callback,
        Predicate<Pair<RobuxServerMetadata, DataSegment>> filter
    )
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void registerServerCallback(
        Executor exec,
        ServerView.ServerCallback callback
    )
    {
      throw new UnsupportedOperationException();
    }
  }
}
