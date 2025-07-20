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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.query.extraction.ExtractionFn;
import org.apache.robux.query.filter.ColumnIndexSelector;
import org.apache.robux.query.filter.RobuxDoublePredicate;
import org.apache.robux.query.filter.RobuxFloatPredicate;
import org.apache.robux.query.filter.RobuxLongPredicate;
import org.apache.robux.query.filter.RobuxObjectPredicate;
import org.apache.robux.query.filter.RobuxPredicateFactory;
import org.apache.robux.query.filter.RobuxPredicateMatch;
import org.apache.robux.query.filter.Filter;
import org.apache.robux.query.filter.FilterTuning;
import org.apache.robux.query.filter.ValueMatcher;
import org.apache.robux.query.filter.vector.VectorValueMatcher;
import org.apache.robux.query.filter.vector.VectorValueMatcherColumnProcessorFactory;
import org.apache.robux.segment.ColumnInspector;
import org.apache.robux.segment.ColumnProcessors;
import org.apache.robux.segment.ColumnSelectorFactory;
import org.apache.robux.segment.index.BitmapColumnIndex;
import org.apache.robux.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 *
 */
public class DimensionPredicateFilter implements Filter
{
  protected final String dimension;
  protected final RobuxPredicateFactory predicateFactory;
  protected final String basePredicateString;
  protected final ExtractionFn extractionFn;
  protected final FilterTuning filterTuning;

  public DimensionPredicateFilter(
      final String dimension,
      final RobuxPredicateFactory predicateFactory,
      final ExtractionFn extractionFn
  )
  {
    this(dimension, predicateFactory, extractionFn, null);
  }

  public DimensionPredicateFilter(
      final String dimension,
      final RobuxPredicateFactory predicateFactory,
      final ExtractionFn extractionFn,
      final FilterTuning filterTuning
  )
  {
    Preconditions.checkNotNull(predicateFactory, "predicateFactory");
    this.dimension = Preconditions.checkNotNull(dimension, "dimension");
    this.basePredicateString = predicateFactory.toString();
    this.extractionFn = extractionFn;
    this.filterTuning = filterTuning;

    if (extractionFn == null) {
      this.predicateFactory = predicateFactory;
    } else {
      this.predicateFactory = new DelegatingStringPredicateFactory(predicateFactory, extractionFn);
    }
  }

  @Nullable
  @Override
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    if (!Filters.checkFilterTuningUseIndex(dimension, selector, filterTuning)) {
      return null;
    }
    return Filters.makePredicateIndex(dimension, selector, predicateFactory);
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return Filters.makeValueMatcher(factory, dimension, predicateFactory);
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(final VectorColumnSelectorFactory factory)
  {
    return ColumnProcessors.makeVectorProcessor(
        dimension,
        VectorValueMatcherColumnProcessorFactory.instance(),
        factory
    ).makeMatcher(predicateFactory);
  }

  @Override
  public boolean canVectorizeMatcher(ColumnInspector inspector)
  {
    return true;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(dimension);
  }

  @Override
  public String toString()
  {
    if (extractionFn != null) {
      return StringUtils.format("%s(%s) = %s", extractionFn, dimension, basePredicateString);
    } else {
      return StringUtils.format("%s = %s", dimension, basePredicateString);
    }
  }

  @VisibleForTesting
  static class DelegatingStringPredicateFactory implements RobuxPredicateFactory
  {
    private final RobuxObjectPredicate<String> baseStringPredicate;
    private final RobuxPredicateFactory predicateFactory;
    private final ExtractionFn extractionFn;

    DelegatingStringPredicateFactory(RobuxPredicateFactory predicateFactory, ExtractionFn extractionFn)
    {
      this.predicateFactory = predicateFactory;
      this.baseStringPredicate = predicateFactory.makeStringPredicate();
      this.extractionFn = extractionFn;
    }

    @Override
    public RobuxObjectPredicate<String> makeStringPredicate()
    {
      return input -> baseStringPredicate.apply(extractionFn.apply(input));
    }

    @Override
    public RobuxLongPredicate makeLongPredicate()
    {
      return new RobuxLongPredicate()
      {
        @Override
        public RobuxPredicateMatch applyLong(long input)
        {
          return baseStringPredicate.apply(extractionFn.apply(input));
        }

        @Override
        public RobuxPredicateMatch applyNull()
        {
          return baseStringPredicate.apply(extractionFn.apply(null));
        }
      };
    }

    @Override
    public RobuxFloatPredicate makeFloatPredicate()
    {
      return new RobuxFloatPredicate()
      {
        @Override
        public RobuxPredicateMatch applyFloat(float input)
        {
          return baseStringPredicate.apply(extractionFn.apply(input));
        }

        @Override
        public RobuxPredicateMatch applyNull()
        {
          return baseStringPredicate.apply(extractionFn.apply(null));
        }
      };
    }

    @Override
    public RobuxDoublePredicate makeDoublePredicate()
    {
      return new RobuxDoublePredicate()
      {
        @Override
        public RobuxPredicateMatch applyDouble(double input)
        {
          return baseStringPredicate.apply(extractionFn.apply(input));
        }

        @Override
        public RobuxPredicateMatch applyNull()
        {
          return baseStringPredicate.apply(extractionFn.apply(null));
        }
      };
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
      DelegatingStringPredicateFactory that = (DelegatingStringPredicateFactory) o;
      return Objects.equals(predicateFactory, that.predicateFactory) &&
             Objects.equals(extractionFn, that.extractionFn);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(predicateFactory, extractionFn);
    }
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
    DimensionPredicateFilter that = (DimensionPredicateFilter) o;
    return Objects.equals(dimension, that.dimension) &&
           Objects.equals(basePredicateString, that.basePredicateString) &&
           Objects.equals(extractionFn, that.extractionFn) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, basePredicateString, extractionFn, filterTuning);
  }
}
