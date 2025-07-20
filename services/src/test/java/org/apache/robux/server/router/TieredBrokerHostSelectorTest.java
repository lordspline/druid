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

package org.apache.robux.server.router;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.robux.client.RobuxServer;
import org.apache.robux.client.selector.Server;
import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.discovery.RobuxNodeDiscovery;
import org.apache.robux.discovery.RobuxNodeDiscoveryProvider;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.java.util.common.Intervals;
import org.apache.robux.java.util.common.Pair;
import org.apache.robux.query.Robuxs;
import org.apache.robux.query.QueryContexts;
import org.apache.robux.query.aggregation.CountAggregatorFactory;
import org.apache.robux.query.spec.MultipleIntervalSegmentSpec;
import org.apache.robux.query.timeseries.TimeseriesQuery;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.coordinator.rules.IntervalLoadRule;
import org.apache.robux.server.coordinator.rules.Rule;
import org.apache.robux.sql.http.SqlQuery;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class TieredBrokerHostSelectorTest
{
  private RobuxNodeDiscoveryProvider robuxNodeDiscoveryProvider;
  private RobuxNodeDiscovery robuxNodeDiscovery;
  private TieredBrokerHostSelector brokerSelector;

  private DiscoveryRobuxNode node1;
  private DiscoveryRobuxNode node2;
  private DiscoveryRobuxNode node3;

  @Before
  public void setUp()
  {
    robuxNodeDiscoveryProvider = EasyMock.createStrictMock(RobuxNodeDiscoveryProvider.class);

    node1 = new DiscoveryRobuxNode(
        new RobuxNode("hotBroker", "hotHost", false, 8080, null, true, false),
        NodeRole.BROKER,
        ImmutableMap.of()
    );

    node2 = new DiscoveryRobuxNode(
        new RobuxNode("coldBroker", "coldHost1", false, 8080, null, true, false),
        NodeRole.BROKER,
        ImmutableMap.of()
    );

    node3 = new DiscoveryRobuxNode(
        new RobuxNode("coldBroker", "coldHost2", false, 8080, null, true, false),
        NodeRole.BROKER,
        ImmutableMap.of()
    );

    robuxNodeDiscovery = new RobuxNodeDiscovery()
    {
      @Override
      public Collection<DiscoveryRobuxNode> getAllNodes()
      {
        return ImmutableSet.of(node1, node2, node3);
      }

      @Override
      public void registerListener(Listener listener)
      {
        listener.nodesAdded(ImmutableList.of(node1, node2, node3));
        listener.nodeViewInitialized();
      }
    };

    EasyMock.expect(robuxNodeDiscoveryProvider.getForNodeRole(NodeRole.BROKER))
            .andReturn(robuxNodeDiscovery);

    EasyMock.replay(robuxNodeDiscoveryProvider);

    brokerSelector = new TieredBrokerHostSelector(
        new TestRuleManager(null),
        new TieredBrokerConfig()
        {
          @Override
          public LinkedHashMap<String, String> getTierToBrokerMap()
          {
            return new LinkedHashMap<>(
                ImmutableMap.of(
                    "hot", "hotBroker",
                    "medium", "mediumBroker",
                    RobuxServer.DEFAULT_TIER, "coldBroker"
                )
            );
          }

          @Override
          public String getDefaultBrokerServiceName()
          {
            return "hotBroker";
          }
        },
        robuxNodeDiscoveryProvider,
        Arrays.asList(
            new ManualTieredBrokerSelectorStrategy(null),
            new TimeBoundaryTieredBrokerSelectorStrategy(),
            new PriorityTieredBrokerSelectorStrategy(0, 1)
        )
    );

    brokerSelector.start();
  }

  @After
  public void tearDown()
  {
    brokerSelector.stop();

    EasyMock.verify(robuxNodeDiscoveryProvider);
  }

  @Test
  public void testBasicSelect()
  {
    TimeseriesQuery query = Robuxs.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .granularity("all")
                                  .aggregators(
                                      Collections.singletonList(new CountAggregatorFactory("rows")))
                                  .intervals(Collections.singletonList(Intervals.of("2011-08-31/2011-09-01")))
                                  .build();

    Pair<String, Server> p = brokerSelector.select(query);
    Assert.assertEquals("coldBroker", p.lhs);
    Assert.assertEquals("coldHost1:8080", p.rhs.getHost());

    p = brokerSelector.select(query);
    Assert.assertEquals("coldBroker", p.lhs);
    Assert.assertEquals("coldHost2:8080", p.rhs.getHost());

    p = brokerSelector.select(query);
    Assert.assertEquals("coldBroker", p.lhs);
    Assert.assertEquals("coldHost1:8080", p.rhs.getHost());
  }


  @Test
  public void testBasicSelect2()
  {
    Pair<String, Server> p = brokerSelector.select(
        Robuxs.newTimeseriesQueryBuilder()
              .dataSource("test")
              .granularity("all")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("rows")))
              .intervals(Collections.singletonList(Intervals.of("2013-08-31/2013-09-01")))
              .build()
    );

    Assert.assertEquals("hotBroker", p.lhs);
    Assert.assertEquals("hotHost:8080", p.rhs.getHost());
  }

  @Test
  public void testSelectMatchesNothing()
  {
    String brokerName = (String) brokerSelector.select(
        Robuxs.newTimeseriesQueryBuilder()
              .dataSource("test")
              .granularity("all")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("rows")))
              .intervals(Collections.singletonList(Intervals.of("2010-08-31/2010-09-01")))
              .build()
    ).lhs;

    Assert.assertEquals("hotBroker", brokerName);
  }

  @Test
  public void testSelectMultiInterval()
  {
    String brokerName = (String) brokerSelector.select(
        Robuxs.newTimeseriesQueryBuilder()
              .dataSource("test")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      Arrays.asList(
                          Intervals.of("2013-08-31/2013-09-01"),
                          Intervals.of("2012-08-31/2012-09-01"),
                          Intervals.of("2011-08-31/2011-09-01")
                      )
                  )
              ).build()
    ).lhs;

    Assert.assertEquals("coldBroker", brokerName);
  }

  @Test
  public void testSelectMultiInterval2()
  {
    String brokerName = (String) brokerSelector.select(
        Robuxs.newTimeseriesQueryBuilder()
              .dataSource("test")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      Arrays.asList(
                          Intervals.of("2011-08-31/2011-09-01"),
                          Intervals.of("2012-08-31/2012-09-01"),
                          Intervals.of("2013-08-31/2013-09-01")
                      )
                  )
              ).build()
    ).lhs;

    Assert.assertEquals("coldBroker", brokerName);
  }

  @Test
  public void testPrioritySelect()
  {
    String brokerName = (String) brokerSelector.select(
        Robuxs.newTimeseriesQueryBuilder()
              .dataSource("test")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      Arrays.asList(
                          Intervals.of("2011-08-31/2011-09-01"),
                          Intervals.of("2012-08-31/2012-09-01"),
                          Intervals.of("2013-08-31/2013-09-01")
                      )
                  )
              )
              .context(ImmutableMap.of("priority", -1))
              .build()
    ).lhs;

    Assert.assertEquals("hotBroker", brokerName);
  }

  @Test
  public void testPrioritySelect2()
  {
    String brokerName = (String) brokerSelector.select(
        Robuxs.newTimeseriesQueryBuilder()
              .dataSource("test")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      Arrays.asList(
                          Intervals.of("2011-08-31/2011-09-01"),
                          Intervals.of("2012-08-31/2012-09-01"),
                          Intervals.of("2013-08-31/2013-09-01")
                      )
                  )
              )
              .context(ImmutableMap.of("priority", 5))
              .build()
    ).lhs;

    Assert.assertEquals("hotBroker", brokerName);
  }

  @Test
  public void testSelectBasedOnQueryContext()
  {
    final Robuxs.TimeseriesQueryBuilder queryBuilder =
        Robuxs.newTimeseriesQueryBuilder()
              .dataSource("test")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      Collections.singletonList(Intervals.of("2009/2010"))
                  )
              );

    Assert.assertEquals(
        brokerSelector.getDefaultServiceName(),
        brokerSelector.select(queryBuilder.build()).lhs
    );
    Assert.assertEquals(
        "hotBroker",
        brokerSelector.select(
            queryBuilder
                .context(ImmutableMap.of(QueryContexts.BROKER_SERVICE_NAME, "hotBroker"))
                .build()
        ).lhs
    );
    Assert.assertEquals(
        "coldBroker",
        brokerSelector.select(
            queryBuilder
                .context(ImmutableMap.of(QueryContexts.BROKER_SERVICE_NAME, "coldBroker"))
                .build()
        ).lhs
    );
  }

  @Test
  public void testSelectForSql()
  {
    Assert.assertEquals(
        brokerSelector.getDefaultServiceName(),
        brokerSelector.selectForSql(
            createSqlQueryWithContext(null)
        ).lhs
    );
    Assert.assertEquals(
        "hotBroker",
        brokerSelector.selectForSql(
            createSqlQueryWithContext(
                ImmutableMap.of(QueryContexts.BROKER_SERVICE_NAME, "hotBroker")
            )
        ).lhs
    );
    Assert.assertEquals(
        "coldBroker",
        brokerSelector.selectForSql(
            createSqlQueryWithContext(
                ImmutableMap.of(QueryContexts.BROKER_SERVICE_NAME, "coldBroker")
            )
        ).lhs
    );
  }

  @Test
  public void testGetAllBrokers()
  {
    Assert.assertEquals(
        ImmutableMap.of(
            "mediumBroker", ImmutableList.of(),
            "coldBroker", ImmutableList.of("coldHost1:8080", "coldHost2:8080"),
            "hotBroker", ImmutableList.of("hotHost:8080")
        ),
        Maps.transformValues(
            brokerSelector.getAllBrokers(),
            new Function<List<Server>, List<String>>()
            {
              @Override
              public List<String> apply(@Nullable List<Server> servers)
              {
                return Lists.transform(servers, server -> server.getHost());
              }
            }
        )
    );
  }

  private SqlQuery createSqlQueryWithContext(Map<String, Object> queryContext)
  {
    return new SqlQuery(
        "SELECT * FROM test",
        null,
        false,
        false,
        false,
        queryContext,
        null
    );
  }

  private static class TestRuleManager extends CoordinatorRuleManager
  {
    public TestRuleManager(
        Supplier<TieredBrokerConfig> config
    )
    {
      super(config, null);
    }

    @Override
    public boolean isStarted()
    {
      return true;
    }

    @Override
    public List<Rule> getRulesWithDefault(String dataSource)
    {
      return Arrays.asList(
          new IntervalLoadRule(Intervals.of("2013/2014"), ImmutableMap.of("hot", 1), null),
          new IntervalLoadRule(Intervals.of("2012/2013"), ImmutableMap.of("medium", 1), null),
          new IntervalLoadRule(
              Intervals.of("2011/2012"),
              ImmutableMap.of(RobuxServer.DEFAULT_TIER, 1),
              null
          )
      );
    }
  }
}
