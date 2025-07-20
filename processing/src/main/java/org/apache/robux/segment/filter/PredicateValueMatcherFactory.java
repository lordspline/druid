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

import org.apache.robux.data.input.Rows;
import org.apache.robux.math.expr.ExprEval;
import org.apache.robux.query.filter.RobuxDoublePredicate;
import org.apache.robux.query.filter.RobuxFloatPredicate;
import org.apache.robux.query.filter.RobuxLongPredicate;
import org.apache.robux.query.filter.RobuxObjectPredicate;
import org.apache.robux.query.filter.RobuxPredicateFactory;
import org.apache.robux.query.filter.RobuxPredicateMatch;
import org.apache.robux.query.filter.ValueMatcher;
import org.apache.robux.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.robux.segment.BaseDoubleColumnValueSelector;
import org.apache.robux.segment.BaseFloatColumnValueSelector;
import org.apache.robux.segment.BaseLongColumnValueSelector;
import org.apache.robux.segment.BaseObjectColumnValueSelector;
import org.apache.robux.segment.ColumnProcessorFactory;
import org.apache.robux.segment.DimensionSelector;
import org.apache.robux.segment.NilColumnValueSelector;
import org.apache.robux.segment.column.ColumnCapabilities;
import org.apache.robux.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Creates {@link ValueMatcher} that apply a predicate to each value.
 */
public class PredicateValueMatcherFactory implements ColumnProcessorFactory<ValueMatcher>
{
  private final RobuxPredicateFactory predicateFactory;

  public PredicateValueMatcherFactory(RobuxPredicateFactory predicateFactory)
  {
    this.predicateFactory = predicateFactory;
  }

  @Override
  public ColumnType defaultType()
  {
    // Set default type to COMPLEX, so when the underlying type is unknown, we go into "makeComplexProcessor", which
    // uses per-row type detection.
    return ColumnType.UNKNOWN_COMPLEX;
  }

  @Override
  public ValueMatcher makeDimensionProcessor(DimensionSelector selector, boolean multiValue)
  {
    return ValueMatchers.makeStringValueMatcher(selector, predicateFactory, multiValue);
  }

  @Override
  public ValueMatcher makeFloatProcessor(BaseFloatColumnValueSelector selector)
  {
    return ValueMatchers.makeFloatValueMatcher(selector, predicateFactory);
  }

  @Override
  public ValueMatcher makeDoubleProcessor(BaseDoubleColumnValueSelector selector)
  {
    return ValueMatchers.makeDoubleValueMatcher(selector, predicateFactory);
  }

  @Override
  public ValueMatcher makeLongProcessor(BaseLongColumnValueSelector selector)
  {
    return ValueMatchers.makeLongValueMatcher(selector, predicateFactory);
  }

  @Override
  public ValueMatcher makeArrayProcessor(
      BaseObjectColumnValueSelector<?> selector,
      @Nullable ColumnCapabilities columnCapabilities
  )
  {
    if (selector instanceof NilColumnValueSelector) {
      // Column does not exist, or is unfilterable. Treat it as all nulls.

      final RobuxPredicateMatch match = predicateFactory.makeArrayPredicate(columnCapabilities).apply(null);
      if (match == RobuxPredicateMatch.TRUE) {
        return ValueMatchers.allTrue();
      }
      if (match == RobuxPredicateMatch.UNKNOWN) {
        return ValueMatchers.makeAlwaysFalseWithNullUnknownObjectMatcher(selector);
      }
      // predicate matches null as false, there are no unknowns
      return ValueMatchers.allFalse();
    } else {
      // use the array predicate
      final RobuxObjectPredicate<Object[]> predicate = predicateFactory.makeArrayPredicate(columnCapabilities);
      return new ValueMatcher()
      {
        @Override
        public boolean matches(boolean includeUnknown)
        {
          Object o = selector.getObject();
          if (o == null || o instanceof Object[]) {
            return predicate.apply((Object[]) o).matches(includeUnknown);
          }
          if (o instanceof List) {
            ExprEval<?> oEval = ExprEval.bestEffortArray((List<?>) o);
            return predicate.apply(oEval.asArray()).matches(includeUnknown);
          }
          // upcast non-array to a single element array to behave consistently with expressions.. idk if this is cool
          return predicate.apply(new Object[]{o}).matches(includeUnknown);
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("selector", selector);
          inspector.visit("predicate", predicate);
        }
      };
    }
  }

  @Override
  public ValueMatcher makeComplexProcessor(BaseObjectColumnValueSelector<?> selector)
  {
    if (selector instanceof NilColumnValueSelector) {
      // Column does not exist, or is unfilterable. Treat it as all nulls.
      final RobuxPredicateMatch match = predicateFactory.makeStringPredicate().apply(null);
      if (match == RobuxPredicateMatch.TRUE) {
        return ValueMatchers.allTrue();
      }
      if (match == RobuxPredicateMatch.UNKNOWN) {
        return ValueMatchers.makeAlwaysFalseWithNullUnknownObjectMatcher(selector);
      }
      // predicate matches null as false, there are no unknowns
      return ValueMatchers.allFalse();
    } else if (!isNumberOrString(selector.classOfObject())) {
      // if column is definitely not a number of string, use the object predicate
      final RobuxObjectPredicate<Object> predicate = predicateFactory.makeObjectPredicate();
      return new ValueMatcher()
      {
        @Override
        public boolean matches(boolean includeUnknown)
        {
          final Object val = selector.getObject();
          return predicate.apply(val).matches(includeUnknown);
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("selector", selector);
          inspector.visit("predicate", predicate);
        }
      };
    } else {
      // Column exists but the type of value is unknown (we might have got here because "defaultType" is COMPLEX).
      // Return a ValueMatcher that inspects the object and does type-based comparison.

      return new ValueMatcher()
      {
        private RobuxObjectPredicate<String> stringPredicate;
        private RobuxLongPredicate longPredicate;
        private RobuxFloatPredicate floatPredicate;
        private RobuxDoublePredicate doublePredicate;
        private RobuxObjectPredicate<Object[]> arrayPredicate;

        @Override
        public boolean matches(boolean includeUnknown)
        {
          final Object rowValue = selector.getObject();

          if (rowValue == null) {
            return getStringPredicate().apply(null).matches(includeUnknown);
          } else if (rowValue instanceof Integer) {
            return getLongPredicate().applyLong((int) rowValue).matches(includeUnknown);
          } else if (rowValue instanceof Long) {
            return getLongPredicate().applyLong((long) rowValue).matches(includeUnknown);
          } else if (rowValue instanceof Float) {
            return getFloatPredicate().applyFloat((float) rowValue).matches(includeUnknown);
          } else if (rowValue instanceof Number) {
            // Double or some other non-int, non-long, non-float number.
            return getDoublePredicate().applyDouble(((Number) rowValue).doubleValue()).matches(includeUnknown);
          } else if (rowValue instanceof Object[]) {
            return getArrayPredicate().apply((Object[]) rowValue).matches(includeUnknown);
          } else {
            // Other types. Cast to list of strings and evaluate them as strings.
            // Boolean values are handled here as well since it is not a known type in Robux.
            final List<String> rowValueStrings = Rows.objectToStrings(rowValue);

            if (rowValueStrings.isEmpty()) {
              // Empty list is equivalent to null.
              return getStringPredicate().apply(null).matches(includeUnknown);
            }

            for (String rowValueString : rowValueStrings) {
              if (getStringPredicate().apply(rowValueString).matches(includeUnknown)) {
                return true;
              }
            }

            return false;
          }
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("selector", selector);
          inspector.visit("factory", predicateFactory);
        }

        private RobuxObjectPredicate<String> getStringPredicate()
        {
          if (stringPredicate == null) {
            stringPredicate = predicateFactory.makeStringPredicate();
          }

          return stringPredicate;
        }

        private RobuxLongPredicate getLongPredicate()
        {
          if (longPredicate == null) {
            longPredicate = predicateFactory.makeLongPredicate();
          }

          return longPredicate;
        }

        private RobuxFloatPredicate getFloatPredicate()
        {
          if (floatPredicate == null) {
            floatPredicate = predicateFactory.makeFloatPredicate();
          }

          return floatPredicate;
        }

        private RobuxDoublePredicate getDoublePredicate()
        {
          if (doublePredicate == null) {
            doublePredicate = predicateFactory.makeDoublePredicate();
          }

          return doublePredicate;
        }

        private RobuxObjectPredicate<Object[]> getArrayPredicate()
        {
          if (arrayPredicate == null) {
            arrayPredicate = predicateFactory.makeArrayPredicate(null);
          }
          return arrayPredicate;
        }
      };
    }
  }

  /**
   * Returns whether a {@link BaseObjectColumnValueSelector} with object class {@code clazz} might be filterable, i.e.,
   * whether it might return numbers or strings.
   *
   * @param clazz class of object
   */
  private static <T> boolean isNumberOrString(final Class<T> clazz)
  {
    if (Number.class.isAssignableFrom(clazz) || String.class.isAssignableFrom(clazz)) {
      // clazz is a Number or String.
      return true;
    } else if (clazz.isAssignableFrom(Number.class) || clazz.isAssignableFrom(String.class)) {
      // clazz is a superclass of Number or String.
      return true;
    } else {
      // Instances of clazz cannot possibly be Numbers or Strings.
      return false;
    }
  }
}
