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

package org.apache.robux.query.filter;

/**
 * Note: this is not a {@link org.apache.robux.guice.annotations.PublicApi} or an
 * {@link org.apache.robux.guice.annotations.ExtensionPoint} of Robux.
 */
public interface RobuxLongPredicate
{
  RobuxLongPredicate ALWAYS_FALSE_WITH_NULL_UNKNOWN = input -> RobuxPredicateMatch.FALSE;

  RobuxLongPredicate ALWAYS_TRUE = input -> RobuxPredicateMatch.TRUE;

  RobuxLongPredicate MATCH_NULL_ONLY = new RobuxLongPredicate()
  {
    @Override
    public RobuxPredicateMatch applyLong(long input)
    {
      return RobuxPredicateMatch.FALSE;
    }

    @Override
    public RobuxPredicateMatch applyNull()
    {
      return RobuxPredicateMatch.TRUE;
    }
  };

  RobuxPredicateMatch applyLong(long input);

  default RobuxPredicateMatch applyNull()
  {
    return RobuxPredicateMatch.UNKNOWN;
  }
}
