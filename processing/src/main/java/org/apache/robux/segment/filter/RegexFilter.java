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

package org.apache.robux.segment.filter;

import com.google.common.annotations.VisibleForTesting;
import org.apache.robux.java.util.common.IAE;
import org.apache.robux.query.extraction.ExtractionFn;
import org.apache.robux.query.filter.RobuxDoublePredicate;
import org.apache.robux.query.filter.RobuxFloatPredicate;
import org.apache.robux.query.filter.RobuxLongPredicate;
import org.apache.robux.query.filter.RobuxObjectPredicate;
import org.apache.robux.query.filter.RobuxPredicateFactory;
import org.apache.robux.query.filter.RobuxPredicateMatch;
import org.apache.robux.query.filter.Filter;
import org.apache.robux.query.filter.FilterTuning;

import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 */
public class RegexFilter extends DimensionPredicateFilter
{
  private final Pattern pattern;

  public RegexFilter(
      final String dimension,
      final Pattern pattern,
      final ExtractionFn extractionFn,
      final FilterTuning filterTuning
  )
  {
    super(
        dimension,
        new PatternRobuxPredicateFactory(pattern),
        extractionFn,
        filterTuning
    );
    this.pattern = pattern;
  }

  @VisibleForTesting
  static class PatternRobuxPredicateFactory implements RobuxPredicateFactory
  {
    private final Pattern pattern;

    PatternRobuxPredicateFactory(Pattern pattern)
    {
      this.pattern = pattern;
    }

    @Override
    public RobuxObjectPredicate<String> makeStringPredicate()
    {
      return input -> input == null ? RobuxPredicateMatch.UNKNOWN : RobuxPredicateMatch.of(pattern.matcher(input).find());
    }

    @Override
    public RobuxLongPredicate makeLongPredicate()
    {
      return input -> RobuxPredicateMatch.of(pattern.matcher(String.valueOf(input)).find());
    }

    @Override
    public RobuxFloatPredicate makeFloatPredicate()
    {
      return input -> RobuxPredicateMatch.of(pattern.matcher(String.valueOf(input)).find());
    }

    @Override
    public RobuxDoublePredicate makeDoublePredicate()
    {
      return input -> RobuxPredicateMatch.of(pattern.matcher(String.valueOf(input)).find());
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
      PatternRobuxPredicateFactory that = (PatternRobuxPredicateFactory) o;
      return Objects.equals(pattern.toString(), that.pattern.toString());
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(pattern.toString());
    }

    @Override
    public String toString()
    {
      return "RegexFilter$PatternRobuxPredicateFactory{" +
             "pattern='" + pattern + '\'' +
             '}';
    }
  }

  @Override
  public String toString()
  {
    return "RegexFilter{" +
           "pattern='" + pattern + '\'' +
           '}';
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    return true;
  }

  @Override
  public Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    String rewriteDimensionTo = columnRewrites.get(dimension);

    if (rewriteDimensionTo == null) {
      throw new IAE(
          "Received a non-applicable rewrite: %s, filter's dimension: %s",
          columnRewrites,
          dimension
      );
    }

    return new RegexFilter(
        rewriteDimensionTo,
        pattern,
        extractionFn,
        filterTuning
    );
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
    if (!super.equals(o)) {
      return false;
    }
    RegexFilter that = (RegexFilter) o;
    return Objects.equals(pattern.toString(), that.pattern.toString());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), pattern.toString());
  }
}
