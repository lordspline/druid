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

package org.apache.robux.error;

import org.apache.robux.query.QueryException;

/**
 * A {@link RobuxException.Failure} that serves to cover conversions from {@link QueryException}.
 *
 * When/if QueryException is completely eliminated from the code base, this compat layer should also be able to
 * be removed.  Additionally, it is the hope that nobody should actually be interacting with this class as it should
 * be an implementation detail of {@link RobuxException} and not really seen outside of that.
 */
public class QueryExceptionCompat extends RobuxException.Failure
{
  public static final String ERROR_CODE = "legacyQueryException";

  private final QueryException exception;

  public QueryExceptionCompat(
      QueryException exception
  )
  {
    super(ERROR_CODE);
    this.exception = exception;
  }

  @Override
  protected RobuxException makeException(RobuxException.RobuxExceptionBuilder bob)
  {
    return bob.forPersona(RobuxException.Persona.OPERATOR)
              .ofCategory(convertFailType(exception.getFailType()))
              .build(exception, "%s", exception.getMessage())
              .withContext("host", exception.getHost())
              .withContext("errorClass", exception.getErrorClass())
              .withContext("legacyErrorCode", exception.getErrorCode());
  }

  private RobuxException.Category convertFailType(QueryException.FailType failType)
  {
    switch (failType) {
      case USER_ERROR:
        return RobuxException.Category.INVALID_INPUT;
      case UNAUTHORIZED:
        return RobuxException.Category.UNAUTHORIZED;
      case CAPACITY_EXCEEDED:
        return RobuxException.Category.CAPACITY_EXCEEDED;
      case QUERY_RUNTIME_FAILURE:
        return RobuxException.Category.RUNTIME_FAILURE;
      case CANCELED:
        return RobuxException.Category.CANCELED;
      case UNSUPPORTED:
        return RobuxException.Category.UNSUPPORTED;
      case TIMEOUT:
        return RobuxException.Category.TIMEOUT;
      case UNKNOWN:
      default:
        return RobuxException.Category.UNCATEGORIZED;
    }
  }
}
