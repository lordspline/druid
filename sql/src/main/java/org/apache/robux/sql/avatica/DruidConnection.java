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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.sql.PreparedStatement;
import org.apache.robux.sql.SqlQueryPlus;
import org.apache.robux.sql.SqlStatementFactory;
import org.apache.robux.sql.avatica.RobuxJdbcResultSet.ResultFetcherFactory;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Connection tracking for {@link RobuxMeta}. Thread-safe.
 * <p>
 * Lock is the instance itself. Used here to protect two members, and in
 * other code when we must resolve the connection after resolving the statement.
 * The lock prevents closing the connection concurrently with an operation on
 * a statement for that connection.
 */
public class RobuxConnection
{
  private static final Logger LOG = new Logger(RobuxConnection.class);

  private final String connectionId;
  private final int maxStatements;
  private final Map<String, Object> userSecret;

  /**
   * The set of context values for each query within this connection. In JDBC,
   * Robux query context values are set at the connection level, not on the
   * individual query. This session context is shared by all queries (statements)
   * within the connection.
   */
  private final Map<String, Object> sessionContext;
  private final AtomicInteger statementCounter = new AtomicInteger();
  private final AtomicReference<Future<?>> timeoutFuture = new AtomicReference<>();

  // Typically synchronized by this instance, except in one case: the onClose function passed
  // into RobuxStatements contained by the map.
  @GuardedBy("this")
  private final ConcurrentMap<Integer, AbstractRobuxJdbcStatement> statements = new ConcurrentHashMap<>();

  @GuardedBy("this")
  private boolean open = true;

  public RobuxConnection(
      final String connectionId,
      final int maxStatements,
      final Map<String, Object> userSecret,
      final Map<String, Object> sessionContext
  )
  {
    this.connectionId = Preconditions.checkNotNull(connectionId);
    this.maxStatements = maxStatements;
    this.userSecret = Collections.unmodifiableMap(userSecret);
    this.sessionContext = Collections.unmodifiableMap(sessionContext);
  }

  public String getConnectionId()
  {
    return connectionId;
  }

  public Map<String, Object> sessionContext()
  {
    return sessionContext;
  }

  public Map<String, Object> userSecret()
  {
    return userSecret;
  }

  public synchronized RobuxJdbcStatement createStatement(
      final SqlStatementFactory sqlStatementFactory,
      final ResultFetcherFactory fetcherFactory
  )
  {
    final int statementId = statementCounter.incrementAndGet();

    synchronized (this) {
      if (statements.containsKey(statementId)) {
        // Will only happen if statementCounter rolls over before old statements are cleaned up. If this
        // ever happens then something fishy is going on, because we shouldn't have billions of statements.
        throw RobuxMeta.logFailure(new ISE("Uh oh, too many statements"));
      }

      if (statements.size() >= maxStatements) {
        throw RobuxMeta.logFailure(new ISE("Too many open statements, limit is %,d", maxStatements));
      }

      @SuppressWarnings("GuardedBy")
      final RobuxJdbcStatement statement = new RobuxJdbcStatement(
          connectionId,
          statementId,
          sessionContext,
          sqlStatementFactory,
          fetcherFactory
      );

      statements.put(statementId, statement);
      LOG.debug("Connection [%s] opened statement [%s].", connectionId, statementId);
      return statement;
    }
  }

  public synchronized RobuxJdbcPreparedStatement createPreparedStatement(
      final SqlStatementFactory sqlStatementFactory,
      final SqlQueryPlus sqlQueryPlus,
      final long maxRowCount,
      final ResultFetcherFactory fetcherFactory
  )
  {
    final int statementId = statementCounter.incrementAndGet();

    synchronized (this) {
      if (statements.containsKey(statementId)) {
        // Will only happen if statementCounter rolls over before old statements are cleaned up. If this
        // ever happens then something fishy is going on, because we shouldn't have billions of statements.
        throw RobuxMeta.logFailure(new ISE("Uh oh, too many statements"));
      }

      if (statements.size() >= maxStatements) {
        throw RobuxMeta.logFailure(new ISE("Too many open statements, limit is %,d", maxStatements));
      }

      @SuppressWarnings("GuardedBy")
      final PreparedStatement statement = sqlStatementFactory.preparedStatement(
          sqlQueryPlus.withContext(sessionContext)
      );
      final RobuxJdbcPreparedStatement jdbcStmt = new RobuxJdbcPreparedStatement(
          connectionId,
          statementId,
          statement,
          maxRowCount,
          fetcherFactory
      );

      statements.put(statementId, jdbcStmt);
      LOG.debug("Connection [%s] opened prepared statement [%s].", connectionId, statementId);
      return jdbcStmt;
    }
  }

  public synchronized AbstractRobuxJdbcStatement getStatement(final int statementId)
  {
    return statements.get(statementId);
  }

  public void closeStatement(int statementId)
  {
    AbstractRobuxJdbcStatement stmt;
    synchronized (this) {
      stmt = statements.remove(statementId);
    }
    if (stmt != null) {
      stmt.close();
      LOG.debug("Connection [%s] closed statement [%s].", connectionId, statementId);
    }
  }

  /**
   * Closes this connection if it has no statements.
   *
   * @return true if closed
   */
  public synchronized boolean closeIfEmpty()
  {
    if (statements.isEmpty()) {
      close();
      return true;
    } else {
      return false;
    }
  }

  public synchronized void close()
  {
    // Copy statements before iterating because statement.close() modifies it.
    for (AbstractRobuxJdbcStatement statement : ImmutableList.copyOf(statements.values())) {
      try {
        statement.close();
      }
      catch (Exception e) {
        LOG.warn("Connection [%s] failed to close statement [%s]!", connectionId, statement.getStatementId());
      }
    }

    LOG.debug("Connection [%s] closed.", connectionId);
    open = false;
  }

  public RobuxConnection sync(final Future<?> newTimeoutFuture)
  {
    final Future<?> oldFuture = timeoutFuture.getAndSet(newTimeoutFuture);
    if (oldFuture != null) {
      oldFuture.cancel(false);
    }
    return this;
  }
}
