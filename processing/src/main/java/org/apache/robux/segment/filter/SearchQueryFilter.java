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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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
import org.apache.robux.query.search.SearchQuerySpec;

import java.util.Map;
import java.util.Objects;

/**
 */
public class SearchQueryFilter extends DimensionPredicateFilter
{
  private final SearchQuerySpec query;

  @JsonCreator
  public SearchQueryFilter(
      @JsonProperty("dimension") final String dimension,
      @JsonProperty("query") final SearchQuerySpec query,
      @JsonProperty("extractionFn") final ExtractionFn extractionFn,
      @JsonProperty("filterTuning") final FilterTuning filterTuning
  )
  {
    super(
        dimension,
        new SearchQueryRobuxPredicateFactory(query),
        extractionFn,
        filterTuning
    );

    this.query = query;
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

    return new SearchQueryFilter(
        rewriteDimensionTo,
        query,
        extractionFn,
        filterTuning
    );
  }

  @Override
  public String toString()
  {
    return "SearchFilter{" +
           "query='" + query + '\'' +
           '}';
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
    SearchQueryFilter that = (SearchQueryFilter) o;
    return Objects.equals(query, that.query);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), query);
  }

  @VisibleForTesting
  static class SearchQueryRobuxPredicateFactory implements RobuxPredicateFactory
  {
    private final SearchQuerySpec query;

    SearchQueryRobuxPredicateFactory(SearchQuerySpec query)
    {
      this.query = query;
    }

    @Override
    public RobuxObjectPredicate<String> makeStringPredicate()
    {
      return input -> input == null ? RobuxPredicateMatch.UNKNOWN : RobuxPredicateMatch.of(query.accept(input));
    }

    @Override
    public RobuxLongPredicate makeLongPredicate()
    {
      return input -> RobuxPredicateMatch.of(query.accept(String.valueOf(input)));
    }

    @Override
    public RobuxFloatPredicate makeFloatPredicate()
    {
      return input -> RobuxPredicateMatch.of(query.accept(String.valueOf(input)));
    }

    @Override
    public RobuxDoublePredicate makeDoublePredicate()
    {
      return input -> RobuxPredicateMatch.of(query.accept(String.valueOf(input)));
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
      SearchQueryRobuxPredicateFactory that = (SearchQueryRobuxPredicateFactory) o;
      return Objects.equals(query, that.query);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(query);
    }
  }
}
