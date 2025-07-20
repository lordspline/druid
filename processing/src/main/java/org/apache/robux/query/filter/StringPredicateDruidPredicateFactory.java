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

import org.apache.robux.error.RobuxException;

import javax.annotation.Nullable;
import java.util.Objects;

public class StringPredicateRobuxPredicateFactory implements RobuxPredicateFactory
{
  public static StringPredicateRobuxPredicateFactory equalTo(@Nullable String value)
  {
    if (value == null) {
      return new StringPredicateRobuxPredicateFactory(RobuxObjectPredicate.isNull());
    }
    return new StringPredicateRobuxPredicateFactory(RobuxObjectPredicate.equalTo(value));
  }

  public static StringPredicateRobuxPredicateFactory of(@Nullable RobuxObjectPredicate<String> predicate)
  {
    return new StringPredicateRobuxPredicateFactory(predicate);
  }

  @Nullable
  private final RobuxObjectPredicate<String> predicate;

  private StringPredicateRobuxPredicateFactory(RobuxObjectPredicate<String> predicate)
  {
    this.predicate = predicate;
  }

  @Override
  public RobuxObjectPredicate<String> makeStringPredicate()
  {
    return predicate;
  }

  @Override
  public RobuxLongPredicate makeLongPredicate()
  {
    throw RobuxException.defensive("String equality predicate factory only supports string predicates");
  }

  @Override
  public RobuxFloatPredicate makeFloatPredicate()
  {
    throw RobuxException.defensive("String equality predicate factory only supports string predicates");
  }

  @Override
  public RobuxDoublePredicate makeDoublePredicate()
  {
    throw RobuxException.defensive("String equality predicate factory only supports string predicates");
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StringPredicateRobuxPredicateFactory that = (StringPredicateRobuxPredicateFactory) o;
    return Objects.equals(predicate, that.predicate);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(predicate);
  }
}
