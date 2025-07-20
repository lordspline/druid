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

package org.apache.robux.sql.avatica;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.common.io.Closer;
import org.apache.robux.math.expr.ExprMacroTable;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.policy.NoopPolicyEnforcer;
import org.apache.robux.segment.join.JoinableFactoryWrapper;
import org.apache.robux.server.QueryStackTests;
import org.apache.robux.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.robux.server.security.AllowAllAuthenticator;
import org.apache.robux.server.security.AuthConfig;
import org.apache.robux.server.security.AuthTestUtils;
import org.apache.robux.sql.SqlQueryPlus;
import org.apache.robux.sql.SqlStatementFactory;
import org.apache.robux.sql.avatica.RobuxJdbcResultSet.ResultFetcherFactory;
import org.apache.robux.sql.calcite.planner.CalciteRulesManager;
import org.apache.robux.sql.calcite.planner.CatalogResolver;
import org.apache.robux.sql.calcite.planner.RobuxOperatorTable;
import org.apache.robux.sql.calcite.planner.PlannerConfig;
import org.apache.robux.sql.calcite.planner.PlannerFactory;
import org.apache.robux.sql.calcite.schema.RobuxSchemaCatalog;
import org.apache.robux.sql.calcite.util.CalciteTestBase;
import org.apache.robux.sql.calcite.util.CalciteTests;
import org.apache.robux.sql.calcite.util.QueryFrameworkUtils;
import org.apache.robux.sql.hook.RobuxHookDispatcher;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class RobuxStatementTest extends CalciteTestBase
{
  private static String SUB_QUERY_WITH_ORDER_BY =
        "select T20.F13 as F22\n"
      + "from (SELECT DISTINCT dim1 as F13 FROM robux.foo T10) T20\n"
      + "order by T20.F13 ASC";
  private static String SELECT_FROM_FOO =
      "SELECT __time, cnt, dim1, dim2, m1 FROM robux.foo";
  private static String SELECT_STAR_FROM_FOO =
      "SELECT * FROM robux.foo";

  private static SpecificSegmentsQuerySegmentWalker walker;
  private static QueryRunnerFactoryConglomerate conglomerate;
  private static Closer resourceCloser;

  @BeforeAll
  public static void setUpClass(@TempDir File tempDir)
  {
    resourceCloser = Closer.create();
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(resourceCloser);
    walker = CalciteTests.createMockWalker(conglomerate, tempDir);
    resourceCloser.register(walker);
  }

  @AfterAll
  public static void tearDownClass() throws IOException
  {
    resourceCloser.close();
  }

  private SqlStatementFactory sqlStatementFactory;

  @BeforeEach
  public void setUp()
  {
    final PlannerConfig plannerConfig = new PlannerConfig();
    final RobuxOperatorTable operatorTable = CalciteTests.createOperatorTable();
    final ExprMacroTable macroTable = CalciteTests.createExprMacroTable();
    RobuxSchemaCatalog rootSchema =
        CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);
    final JoinableFactoryWrapper joinableFactoryWrapper = CalciteTests.createJoinableFactoryWrapper();
    final PlannerFactory plannerFactory = new PlannerFactory(
        rootSchema,
        operatorTable,
        macroTable,
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.ROBUX_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of()),
        joinableFactoryWrapper,
        CatalogResolver.NULL_RESOLVER,
        new AuthConfig(),
        NoopPolicyEnforcer.instance(),
        new RobuxHookDispatcher()
    );
    this.sqlStatementFactory = QueryFrameworkUtils.createSqlStatementFactory(
        CalciteTests.createMockSqlEngine(walker, conglomerate),
        plannerFactory
    );
  }

  @AfterEach
  public void tearDown()
  {

  }

  //-----------------------------------------------------------------
  // Robux JDBC Statement
  //
  // The JDBC Statement class starts "empty", then allows executing
  // one statement at a time. Executing a second automatically closes
  // the result set from the first. Each statement takes a new query.
  // Parameters are not generally used in this pattern.

  private RobuxJdbcStatement jdbcStatement()
  {
    return new RobuxJdbcStatement(
        "",
        0,
        Collections.emptyMap(),
        sqlStatementFactory,
        new ResultFetcherFactory(AvaticaServerConfig.DEFAULT_FETCH_TIMEOUT_MS)
    );
  }

  @Test
  public void testSubQueryWithOrderByDirect()
  {
    SqlQueryPlus queryPlus =
        SqlQueryPlus.builder()
                    .sql(SUB_QUERY_WITH_ORDER_BY)
                    .auth(AllowAllAuthenticator.ALLOW_ALL_RESULT)
                    .buildJdbc();
    try (final RobuxJdbcStatement statement = jdbcStatement()) {
      // First frame, ask for all rows.
      statement.execute(queryPlus, -1);
      Meta.Frame frame = statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          subQueryWithOrderByResults(),
          frame
      );
      Assert.assertTrue(statement.isDone());
    }
  }

  @Test
  public void testFetchPastEOFDirect()
  {
    SqlQueryPlus queryPlus =
        SqlQueryPlus.builder()
                    .sql(SUB_QUERY_WITH_ORDER_BY)
                    .auth(AllowAllAuthenticator.ALLOW_ALL_RESULT)
                    .buildJdbc();
    try (final RobuxJdbcStatement statement = jdbcStatement()) {
      // First frame, ask for all rows.
      statement.execute(queryPlus, -1);
      Meta.Frame frame = statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          subQueryWithOrderByResults(),
          frame
      );
      Assert.assertTrue(statement.isDone());
      try {
        statement.nextFrame(6, 6);
        Assert.fail();
      }
      catch (Exception e) {
        // Expected: can't work with an auto-closed result set.
      }
    }
  }

  /**
   * Ensure an error is thrown if the execution step is skipped.
   */
  @Test
  public void testSkipExecuteDirect()
  {
    try (final RobuxJdbcStatement statement = jdbcStatement()) {
      // Error: no call to execute;
      statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 6);
      Assert.fail();
    }
    catch (Exception e) {
      // Expected
    }
  }

  /**
   * Ensure an error is thrown if the client attempts to fetch from a
   * statement after its result set is closed.
   */
  @Test
  public void testFetchAfterResultCloseDirect()
  {
    SqlQueryPlus queryPlus =
        SqlQueryPlus.builder()
                    .sql(SUB_QUERY_WITH_ORDER_BY)
                    .auth(AllowAllAuthenticator.ALLOW_ALL_RESULT)
                    .buildJdbc();
    try (final RobuxJdbcStatement statement = jdbcStatement()) {
      // First frame, ask for all rows.
      statement.execute(queryPlus, -1);
      statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 6);
      statement.closeResultSet();
      statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 6);
      Assert.fail();
    }
    catch (Exception e) {
      // Expected
    }
  }

  @Test
  public void testSubQueryWithOrderByDirectTwice()
  {
    SqlQueryPlus queryPlus =
        SqlQueryPlus.builder()
                    .sql(SUB_QUERY_WITH_ORDER_BY)
                    .auth(AllowAllAuthenticator.ALLOW_ALL_RESULT)
                    .buildJdbc();
    try (final RobuxJdbcStatement statement = jdbcStatement()) {
      statement.execute(queryPlus, -1);
      Meta.Frame frame = statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          subQueryWithOrderByResults(),
          frame
      );

      // Do it again. JDBC says we can reuse statements sequentially.
      Assert.assertTrue(statement.isDone());
      statement.execute(queryPlus, -1);
      frame = statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          subQueryWithOrderByResults(),
          frame
      );
      Assert.assertTrue(statement.isDone());
    }
  }

  private Meta.Frame subQueryWithOrderByResults()
  {
    return Meta.Frame.create(
        0,
        true,
        Lists.newArrayList(
            new Object[]{""},
            new Object[]{"1"},
            new Object[]{"10.1"},
            new Object[]{"2"},
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }

  @Test
  public void testSelectAllInFirstFrameDirect()
  {
    SqlQueryPlus queryPlus =
        SqlQueryPlus.builder()
                    .sql(SELECT_FROM_FOO)
                    .auth(AllowAllAuthenticator.ALLOW_ALL_RESULT)
                    .buildJdbc();
    try (final RobuxJdbcStatement statement = jdbcStatement()) {
      // First frame, ask for all rows.
      statement.execute(queryPlus, -1);
      Meta.Frame frame = statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          Meta.Frame.create(
              0,
              true,
              Lists.newArrayList(
                  new Object[]{DateTimes.of("2000-01-01").getMillis(), 1L, "", "a", 1.0f},
                  new Object[]{
                      DateTimes.of("2000-01-02").getMillis(),
                      1L,
                      "10.1",
                      null,
                      2.0f
                  },
                  new Object[]{DateTimes.of("2000-01-03").getMillis(), 1L, "2", "", 3.0f},
                  new Object[]{DateTimes.of("2001-01-01").getMillis(), 1L, "1", "a", 4.0f},
                  new Object[]{DateTimes.of("2001-01-02").getMillis(), 1L, "def", "abc", 5.0f},
                  new Object[]{DateTimes.of("2001-01-03").getMillis(), 1L, "abc", null, 6.0f}
              )
          ),
          frame
      );
      Assert.assertTrue(statement.isDone());
    }
  }

  /**
   * Test results spread over two frames. Also checks various state-related
   * methods.
   */
  @Test
  public void testSelectSplitOverTwoFramesDirect()
  {
    SqlQueryPlus queryPlus =
        SqlQueryPlus.builder()
                    .sql(SELECT_FROM_FOO)
                    .auth(AllowAllAuthenticator.ALLOW_ALL_RESULT)
                    .buildJdbc();
    try (final RobuxJdbcStatement statement = jdbcStatement()) {

      // First frame, ask for 2 rows.
      statement.execute(queryPlus, -1);
      Assert.assertEquals(0, statement.getCurrentOffset());
      Assert.assertFalse(statement.isDone());
      Meta.Frame frame = statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 2);
      Assert.assertEquals(
          firstFrameResults(),
          frame
      );
      Assert.assertFalse(statement.isDone());
      Assert.assertEquals(2, statement.getCurrentOffset());

      // Last frame, ask for all remaining rows.
      frame = statement.nextFrame(2, 10);
      Assert.assertEquals(
          secondFrameResults(),
          frame
      );
      Assert.assertTrue(statement.isDone());
    }
  }

  /**
   * Verify that JDBC automatically closes the first result set when we
   * open a second for the same statement.
   */
  @Test
  public void testTwoFramesAutoCloseDirect()
  {
    SqlQueryPlus queryPlus =
        SqlQueryPlus.builder()
                    .sql(SELECT_FROM_FOO)
                    .auth(AllowAllAuthenticator.ALLOW_ALL_RESULT)
                    .buildJdbc();
    try (final RobuxJdbcStatement statement = jdbcStatement()) {
      // First frame, ask for 2 rows.
      statement.execute(queryPlus, -1);
      Meta.Frame frame = statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 2);
      Assert.assertEquals(
          firstFrameResults(),
          frame
      );
      Assert.assertFalse(statement.isDone());

      // Do it again. Closes the prior result set.
      statement.execute(queryPlus, -1);
      frame = statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 2);
      Assert.assertEquals(
          firstFrameResults(),
          frame
      );
      Assert.assertFalse(statement.isDone());

      // Last frame, ask for all remaining rows.
      frame = statement.nextFrame(2, 10);
      Assert.assertEquals(
          secondFrameResults(),
          frame
      );
      Assert.assertTrue(statement.isDone());
    }
  }

  /**
   * Test that closing a statement with pending results automatically
   * closes the underlying result set.
   */
  @Test
  public void testTwoFramesCloseWithResultSetDirect()
  {
    SqlQueryPlus queryPlus =
        SqlQueryPlus.builder()
                    .sql(SELECT_FROM_FOO)
                    .auth(AllowAllAuthenticator.ALLOW_ALL_RESULT)
                    .buildJdbc();
    try (final RobuxJdbcStatement statement = jdbcStatement()) {
      // First frame, ask for 2 rows.
      statement.execute(queryPlus, -1);
      Meta.Frame frame = statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 2);
      Assert.assertEquals(
          firstFrameResults(),
          frame
      );
      Assert.assertFalse(statement.isDone());

      // Leave result set open; close statement.
    }
  }

  private Meta.Frame firstFrameResults()
  {
    return Meta.Frame.create(
        0,
        false,
        Lists.newArrayList(
            new Object[]{DateTimes.of("2000-01-01").getMillis(), 1L, "", "a", 1.0f},
            new Object[]{
                DateTimes.of("2000-01-02").getMillis(),
                1L,
                "10.1",
                null,
                2.0f
            }
        )
    );
  }

  private Meta.Frame secondFrameResults()
  {
    return Meta.Frame.create(
        2,
        true,
        Lists.newArrayList(
            new Object[]{DateTimes.of("2000-01-03").getMillis(), 1L, "2", "", 3.0f},
            new Object[]{DateTimes.of("2001-01-01").getMillis(), 1L, "1", "a", 4.0f},
            new Object[]{DateTimes.of("2001-01-02").getMillis(), 1L, "def", "abc", 5.0f},
            new Object[]{DateTimes.of("2001-01-03").getMillis(), 1L, "abc", null, 6.0f}
        )
    );
  }

  @Test
  public void testSignatureDirect()
  {
    SqlQueryPlus queryPlus =
        SqlQueryPlus.builder()
                    .sql(SELECT_STAR_FROM_FOO)
                    .auth(AllowAllAuthenticator.ALLOW_ALL_RESULT)
                    .buildJdbc();
    try (final RobuxJdbcStatement statement = jdbcStatement()) {
      // Check signature.
      statement.execute(queryPlus, -1);
      verifySignature(statement.getSignature());
    }
  }

  @SuppressWarnings("unchecked")
  private void verifySignature(Meta.Signature signature)
  {
    Assert.assertEquals(Meta.CursorFactory.ARRAY, signature.cursorFactory);
    Assert.assertEquals(Meta.StatementType.SELECT, signature.statementType);
    Assert.assertEquals(SELECT_STAR_FROM_FOO, signature.sql);
    Assert.assertEquals(
        Lists.newArrayList(
            Lists.newArrayList("__time", "TIMESTAMP", "java.math.BigDecimal"),
            Lists.newArrayList("dim1", "VARCHAR", "java.lang.String"),
            Lists.newArrayList("dim2", "VARCHAR", "java.lang.String"),
            Lists.newArrayList("dim3", "VARCHAR", "java.lang.String"),
            Lists.newArrayList("cnt", "BIGINT", "java.math.BigDecimal"),
            Lists.newArrayList("m1", "FLOAT", "java.math.BigDecimal"),
            Lists.newArrayList("m2", "DOUBLE", "java.math.BigDecimal"),
            Lists.newArrayList("unique_dim1", "OTHER", "java.lang.Object")
        ),
        Lists.transform(
            signature.columns,
            new Function<ColumnMetaData, List<String>>()
            {
              @Override
              public List<String> apply(final ColumnMetaData columnMetaData)
              {
                return Lists.newArrayList(
                    columnMetaData.label,
                    columnMetaData.type.name,
                    columnMetaData.type.rep.clazz.getName()
                );
              }
            }
        )
    );
  }

  //-----------------------------------------------------------------
  // Robux JDBC Prepared Statement
  //
  // The JDBC PreparedStatement class starts with, then allows executing
  // the statement sequentially, typically with a set of parameters.

  private RobuxJdbcPreparedStatement jdbcPreparedStatement(SqlQueryPlus queryPlus)
  {
    return new RobuxJdbcPreparedStatement(
        "",
        0,
        sqlStatementFactory.preparedStatement(queryPlus),
        Long.MAX_VALUE,
        new ResultFetcherFactory(AvaticaServerConfig.DEFAULT_FETCH_TIMEOUT_MS)
    );
  }

  @Test
  public void testSubQueryWithOrderByPrepared()
  {
    final String sql = "select T20.F13 as F22  from (SELECT DISTINCT dim1 as F13 FROM robux.foo T10) T20 order by T20.F13 ASC";
    SqlQueryPlus queryPlus =
        SqlQueryPlus.builder()
                    .sql(sql)
                    .auth(AllowAllAuthenticator.ALLOW_ALL_RESULT)
                    .buildJdbc();
    try (final RobuxJdbcPreparedStatement statement = jdbcPreparedStatement(queryPlus)) {
      statement.prepare();
      // First frame, ask for all rows.
      statement.execute(Collections.emptyList());
      Meta.Frame frame = statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          subQueryWithOrderByResults(),
          frame
      );
      Assert.assertTrue(statement.isDone());
    }
  }

  @Test
  public void testSubQueryWithOrderByPreparedTwice()
  {
    final String sql = "select T20.F13 as F22  from (SELECT DISTINCT dim1 as F13 FROM robux.foo T10) T20 order by T20.F13 ASC";
    SqlQueryPlus queryPlus =
        SqlQueryPlus.builder()
                    .sql(sql)
                    .auth(AllowAllAuthenticator.ALLOW_ALL_RESULT)
                    .buildJdbc();
    try (final RobuxJdbcPreparedStatement statement = jdbcPreparedStatement(queryPlus)) {
      statement.prepare();
      statement.execute(Collections.emptyList());
      Meta.Frame frame = statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          subQueryWithOrderByResults(),
          frame
      );

      // Do it again. JDBC says we can reuse prepared statements sequentially.
      Assert.assertTrue(statement.isDone());
      statement.execute(Collections.emptyList());
      frame = statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          subQueryWithOrderByResults(),
          frame
      );
      Assert.assertTrue(statement.isDone());
    }
  }

  @Test
  public void testSignaturePrepared()
  {
    SqlQueryPlus queryPlus =
        SqlQueryPlus.builder()
                    .sql(SELECT_STAR_FROM_FOO)
                    .auth(AllowAllAuthenticator.ALLOW_ALL_RESULT)
                    .buildJdbc();
    try (final RobuxJdbcPreparedStatement statement = jdbcPreparedStatement(queryPlus)) {
      statement.prepare();
      verifySignature(statement.getSignature());
    }
  }

  @Test
  public void testParameters()
  {
    SqlQueryPlus queryPlus =
        SqlQueryPlus.builder()
                    .sql("SELECT COUNT(*) AS cnt FROM sys.servers WHERE servers.host = ?")
                    .auth(AllowAllAuthenticator.ALLOW_ALL_RESULT)
                    .buildJdbc();
    Meta.Frame expected = Meta.Frame.create(0, true, Collections.singletonList(new Object[] {1L}));
    List<TypedValue> matchingParams = Collections.singletonList(TypedValue.ofLocal(ColumnMetaData.Rep.STRING, "dummy"));
    try (final RobuxJdbcPreparedStatement statement = jdbcPreparedStatement(queryPlus)) {

      // PreparedStatement protocol: prepare once...
      statement.prepare();

      // Execute many times. First time.
      statement.execute(matchingParams);
      Meta.Frame frame = statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          expected,
          frame
      );

      // Again, same value.
      statement.execute(matchingParams);
      frame = statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          expected,
          frame
      );

      // Again, no matches.
      statement.execute(
          Collections.singletonList(
              TypedValue.ofLocal(ColumnMetaData.Rep.STRING, "foo")));
      frame = statement.nextFrame(AbstractRobuxJdbcStatement.START_OFFSET, 6);
      Assert.assertEquals(
          Meta.Frame.create(0, true, Collections.emptyList()),
          frame
      );
    }
  }
}
