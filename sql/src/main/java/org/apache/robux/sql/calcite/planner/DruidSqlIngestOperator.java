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

package org.apache.robux.sql.calcite.planner;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.robux.server.security.ResourceAction;
import org.apache.robux.sql.calcite.expression.AuthorizableOperator;
import org.apache.robux.sql.calcite.parser.RobuxSqlInsert;
import org.apache.robux.sql.calcite.parser.RobuxSqlReplace;
import org.apache.robux.sql.calcite.parser.SqlGranularityLiteral;

import java.util.HashSet;
import java.util.Set;

public class RobuxSqlIngestOperator extends SqlSpecialOperator implements AuthorizableOperator
{
  public static final SqlSpecialOperator INSERT_OPERATOR =
      new RobuxSqlInsertOperator();
  public static final SqlSpecialOperator REPLACE_OPERATOR =
      new RobuxSqlReplaceOperator();

  public static class RobuxSqlInsertOperator extends RobuxSqlIngestOperator
  {
    public RobuxSqlInsertOperator()
    {
      super("INSERT");
    }

    @Override
    public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode... operands
    )
    {
      return new RobuxSqlInsert(
          pos,
          (SqlNodeList) operands[0],
          operands[1],
          operands[2],
          (SqlNodeList) operands[3],
          (SqlGranularityLiteral) operands[4],
          (SqlNodeList) operands[5],
          (SqlIdentifier) operands[6]
      );
    }
  }

  public static class RobuxSqlReplaceOperator extends RobuxSqlIngestOperator
  {
    public RobuxSqlReplaceOperator()
    {
      super("REPLACE");
    }

    @Override
    public SqlCall createCall(
        SqlLiteral functionQualifier,
        SqlParserPos pos,
        SqlNode... operands
    )
    {
      return new RobuxSqlReplace(
          pos,
          (SqlNodeList) operands[0],
          operands[1],
          operands[2],
          (SqlNodeList) operands[3],
          (SqlGranularityLiteral) operands[4],
          (SqlNodeList) operands[5],
          (SqlIdentifier) operands[6],
          operands[7]
      );
    }
  }

  public RobuxSqlIngestOperator(String name)
  {
    super(name, SqlKind.INSERT);
  }

  @Override
  public Set<ResourceAction> computeResources(SqlCall call, boolean inputSourceTypeSecurityEnabled)
  {
    // resource actions are computed in the respective ingest handlers.
    return new HashSet<>();
  }
}
