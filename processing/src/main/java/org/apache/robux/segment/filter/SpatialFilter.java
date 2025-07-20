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
import org.apache.robux.collections.bitmap.ImmutableBitmap;
import org.apache.robux.collections.spatial.search.Bound;
import org.apache.robux.query.BitmapResultFactory;
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
import org.apache.robux.segment.column.ColumnIndexCapabilities;
import org.apache.robux.segment.column.ColumnIndexSupplier;
import org.apache.robux.segment.column.SimpleColumnIndexCapabilities;
import org.apache.robux.segment.index.AllUnknownBitmapColumnIndex;
import org.apache.robux.segment.index.BitmapColumnIndex;
import org.apache.robux.segment.index.semantic.SpatialIndex;
import org.apache.robux.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

/**
 */
public class SpatialFilter implements Filter
{
  private final String dimension;
  private final Bound bound;
  private final FilterTuning filterTuning;

  public SpatialFilter(
      String dimension,
      Bound bound,
      FilterTuning filterTuning
  )
  {
    this.dimension = Preconditions.checkNotNull(dimension, "dimension");
    this.bound = Preconditions.checkNotNull(bound, "bound");
    this.filterTuning = filterTuning;
  }

  @Nullable
  @Override
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    if (!Filters.checkFilterTuningUseIndex(dimension, selector, filterTuning)) {
      return null;
    }
    final ColumnIndexSupplier indexSupplier = selector.getIndexSupplier(dimension);
    if (indexSupplier == null) {
      return new AllUnknownBitmapColumnIndex(selector);
    }
    final SpatialIndex spatialIndex = indexSupplier.as(SpatialIndex.class);
    if (spatialIndex == null) {
      return null;
    }
    return new BitmapColumnIndex()
    {
      @Override
      public ColumnIndexCapabilities getIndexCapabilities()
      {
        return new SimpleColumnIndexCapabilities(true, true);
      }

      @Override
      public int estimatedComputeCost()
      {
        return Integer.MAX_VALUE;
      }

      @Override
      public <T> T computeBitmapResult(BitmapResultFactory<T> bitmapResultFactory, boolean includeUnknown)
      {
        Iterable<ImmutableBitmap> search = spatialIndex.getRTree().search(bound);
        return bitmapResultFactory.unionDimensionValueBitmaps(search);
      }
    };
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return Filters.makeValueMatcher(
        factory,
        dimension,
        new BoundRobuxPredicateFactory(bound)
    );
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(VectorColumnSelectorFactory factory)
  {
    return ColumnProcessors.makeVectorProcessor(
        dimension,
        VectorValueMatcherColumnProcessorFactory.instance(),
        factory
    ).makeMatcher(new BoundRobuxPredicateFactory(bound));
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
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SpatialFilter that = (SpatialFilter) o;
    return Objects.equals(dimension, that.dimension) &&
           Objects.equals(bound, that.bound) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, bound, filterTuning);
  }

  @VisibleForTesting
  static class BoundRobuxPredicateFactory implements RobuxPredicateFactory
  {
    private final Bound bound;

    BoundRobuxPredicateFactory(Bound bound)
    {
      this.bound = bound;
    }

    @Override
    public RobuxObjectPredicate<String> makeStringPredicate()
    {
      return input -> {
        if (input == null) {
          return RobuxPredicateMatch.UNKNOWN;
        }
        return RobuxPredicateMatch.of(bound.containsObj(input));
      };
    }

    @Override
    public RobuxObjectPredicate<Object> makeObjectPredicate()
    {
      return input -> {
        if (input == null) {
          return RobuxPredicateMatch.UNKNOWN;
        }
        return RobuxPredicateMatch.of(bound.containsObj(input));
      };
    }

    @Override
    public RobuxLongPredicate makeLongPredicate()
    {
      // SpatialFilter does not currently support longs
      return RobuxLongPredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
    }

    @Override
    public RobuxFloatPredicate makeFloatPredicate()
    {
      // SpatialFilter does not currently support floats
      return RobuxFloatPredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
    }

    @Override
    public RobuxDoublePredicate makeDoublePredicate()
    {
      // SpatialFilter does not currently support doubles
      return RobuxDoublePredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
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
      BoundRobuxPredicateFactory that = (BoundRobuxPredicateFactory) o;
      return Objects.equals(bound, that.bound);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(bound);
    }
  }
}
