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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import org.apache.robux.java.util.common.UOE;
import org.apache.robux.math.expr.Evals;
import org.apache.robux.math.expr.Expr;
import org.apache.robux.math.expr.ExprEval;
import org.apache.robux.math.expr.ExpressionType;
import org.apache.robux.math.expr.InputBindings;
import org.apache.robux.query.filter.ColumnIndexSelector;
import org.apache.robux.query.filter.DimFilter;
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
import org.apache.robux.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.robux.segment.ColumnInspector;
import org.apache.robux.segment.ColumnSelectorFactory;
import org.apache.robux.segment.ColumnValueSelector;
import org.apache.robux.segment.column.ColumnCapabilities;
import org.apache.robux.segment.column.ColumnCapabilitiesImpl;
import org.apache.robux.segment.column.ColumnType;
import org.apache.robux.segment.column.TypeSignature;
import org.apache.robux.segment.column.ValueType;
import org.apache.robux.segment.index.AllFalseBitmapColumnIndex;
import org.apache.robux.segment.index.AllTrueBitmapColumnIndex;
import org.apache.robux.segment.index.AllUnknownBitmapColumnIndex;
import org.apache.robux.segment.index.BitmapColumnIndex;
import org.apache.robux.segment.vector.VectorColumnSelectorFactory;
import org.apache.robux.segment.virtual.ExpressionSelectors;
import org.apache.robux.segment.virtual.ExpressionVectorSelectors;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

public class ExpressionFilter implements Filter
{
  private final Supplier<Expr> expr;
  private final Supplier<Expr.BindingAnalysis> bindingDetails;
  private final FilterTuning filterTuning;

  public ExpressionFilter(final Supplier<Expr> expr, final FilterTuning filterTuning)
  {
    this.expr = expr;
    this.bindingDetails = Suppliers.memoize(() -> expr.get().analyzeInputs());
    this.filterTuning = filterTuning;
  }

  @Override
  public boolean canVectorizeMatcher(ColumnInspector inspector)
  {
    return expr.get().canVectorize(inspector);
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(VectorColumnSelectorFactory factory)
  {
    final Expr theExpr = expr.get();

    RobuxPredicateFactory predicateFactory = getPredicateFactory();
    final ExpressionType outputType = theExpr.getOutputType(factory);

    // for all vectorizable expressions, outputType will only ever be null in cases where there is absolutely no
    // input type information, so composed entirely of null constants or missing columns. the expression is
    // effectively constant
    if (outputType == null) {
      // evaluate the expression, just in case it does actually match nulls
      final ExprEval<?> constantEval = theExpr.eval(InputBindings.nilBindings());
      final ConstantMatcherType constantMatcherType;
      if (constantEval.valueOrDefault() == null) {
        constantMatcherType = ConstantMatcherType.ALL_UNKNOWN;
      } else {
        constantMatcherType = constantEval.asBoolean() ? ConstantMatcherType.ALL_TRUE : ConstantMatcherType.ALL_FALSE;
      }
      return constantMatcherType.asVectorMatcher(factory.getReadableVectorInspector());
    }

    // if we got here, we really have to evaluate the expressions to match
    switch (outputType.getType()) {
      case LONG:
        return VectorValueMatcherColumnProcessorFactory.instance().makeLongProcessor(
            ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG),
            ExpressionVectorSelectors.makeVectorValueSelector(factory, theExpr)
        ).makeMatcher(predicateFactory);
      case DOUBLE:
        return VectorValueMatcherColumnProcessorFactory.instance().makeDoubleProcessor(
            ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.DOUBLE),
            ExpressionVectorSelectors.makeVectorValueSelector(factory, theExpr)
        ).makeMatcher(predicateFactory);
      case STRING:
        return VectorValueMatcherColumnProcessorFactory.instance().makeObjectProcessor(
            ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities(),
            ExpressionVectorSelectors.makeVectorObjectSelector(factory, theExpr, null)
        ).makeMatcher(predicateFactory);
      case ARRAY:
        return VectorValueMatcherColumnProcessorFactory.instance().makeObjectProcessor(
            ColumnCapabilitiesImpl.createDefault().setType(ExpressionType.toColumnType(outputType)).setHasNulls(true),
            ExpressionVectorSelectors.makeVectorObjectSelector(factory, theExpr, null)
        ).makeMatcher(predicateFactory);
      default:
        if (ExpressionType.NESTED_DATA.equals(outputType)) {
          return VectorValueMatcherColumnProcessorFactory.instance().makeObjectProcessor(
              ColumnCapabilitiesImpl.createDefault().setType(ExpressionType.toColumnType(outputType)).setHasNulls(true),
              ExpressionVectorSelectors.makeVectorObjectSelector(factory, theExpr, null)
          ).makeMatcher(predicateFactory);
        }
        throw new UOE("Vectorized expression matchers not implemented for type: [%s]", outputType);
    }
  }

  @Override
  public ValueMatcher makeMatcher(final ColumnSelectorFactory factory)
  {
    final ColumnValueSelector<ExprEval> selector = ExpressionSelectors.makeExprEvalSelector(factory, expr.get());
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        final ExprEval eval = selector.getObject();

        if (includeUnknown && eval.value() == null) {
          return true;
        }

        if (eval.type().isArray()) {
          final Object[] result = eval.asArray();
          if (result == null) {
            // if value was null and includeUnknown was true we would have already returned before getting here so
            // is safe to return false
            return false;
          }
          switch (eval.elementType().getType()) {
            case LONG:
              if (includeUnknown) {
                return Arrays.stream(result).anyMatch(o -> o == null || Evals.asBoolean((long) o));
              }

              return Arrays.stream(result).filter(Objects::nonNull).anyMatch(o -> Evals.asBoolean((long) o));
            case STRING:
              if (includeUnknown) {
                return Arrays.stream(result).anyMatch(o -> o == null || Evals.asBoolean((String) o));
              }

              return Arrays.stream(result).anyMatch(o -> Evals.asBoolean((String) o));
            case DOUBLE:
              if (includeUnknown) {
                return Arrays.stream(result).anyMatch(o -> o == null || Evals.asBoolean((double) o));
              }

              return Arrays.stream(result).filter(Objects::nonNull).anyMatch(o -> Evals.asBoolean((double) o));
          }
        }
        return eval.asBoolean();
      }

      @Override
      public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", selector);
      }
    };
  }

  @Nullable
  @Override
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    final Expr.BindingAnalysis details = bindingDetails.get();
    if (details.getRequiredBindings().isEmpty()) {
      // Constant expression.
      final ExprEval<?> eval = expr.get().eval(InputBindings.nilBindings());
      if (eval.valueOrDefault() == null) {
        return new AllUnknownBitmapColumnIndex(selector);
      }
      if (eval.asBoolean()) {
        return new AllTrueBitmapColumnIndex(selector);
      }
      return new AllFalseBitmapColumnIndex(selector.getBitmapFactory());
    } else if (details.getRequiredBindings().size() == 1) {
      // Single-column expression. We can use bitmap indexes if this column has an index and the expression can
      // map over the values of the index.
      final String column = Iterables.getOnlyElement(details.getRequiredBindings());

      // we use a default 'all false' capabilities here because if the column has a bitmap index, but the capabilities
      // are null, it means that the column is missing and should take the single valued path, while truly unknown
      // things will not have a bitmap index available
      final ColumnCapabilities capabilities = selector.getColumnCapabilities(column);
      if (ExpressionSelectors.canMapOverDictionary(details, capabilities)) {
        if (!Filters.checkFilterTuningUseIndex(column, selector, filterTuning)) {
          return null;
        }
        return Filters.makePredicateIndex(
            column,
            selector,
            getBitmapPredicateFactory(capabilities)
        );
      }
    }
    return null;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return bindingDetails.get().getRequiredBindings();
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    // We could support this, but need a good approach to rewriting the identifiers within an expression.
    return false;
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
    ExpressionFilter that = (ExpressionFilter) o;
    return Objects.equals(expr, that.expr) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(expr, filterTuning);
  }

  @Override
  public String toString()
  {
    return new DimFilter.DimFilterToStringBuilder().append(expr.get().stringify())
                                                   .appendFilterTuning(filterTuning)
                                                   .build();
  }



  /**
   * {@link RobuxPredicateFactory} which pipes already evaluated expression values through to {@link Evals#asBoolean},
   * used for a value matcher on top of expression selectors
   */
  private RobuxPredicateFactory getPredicateFactory()
  {
    return new RobuxPredicateFactory()
    {
      @Override
      public RobuxObjectPredicate<String> makeStringPredicate()
      {
        return input -> {
          if (input == null) {
            return RobuxPredicateMatch.UNKNOWN;
          }
          return RobuxPredicateMatch.of(Evals.asBoolean(input));
        };
      }

      @Override
      public RobuxLongPredicate makeLongPredicate()
      {
        return input -> RobuxPredicateMatch.of(Evals.asBoolean(input));
      }

      @Override
      public RobuxFloatPredicate makeFloatPredicate()
      {
        return input -> RobuxPredicateMatch.of(Evals.asBoolean(input));
      }

      @Override
      public RobuxDoublePredicate makeDoublePredicate()
      {
        return input -> RobuxPredicateMatch.of(Evals.asBoolean(input));
      }

      @Override
      public RobuxObjectPredicate<Object> makeObjectPredicate()
      {
        return input -> {
          if (input == null) {
            return RobuxPredicateMatch.UNKNOWN;
          }
          return RobuxPredicateMatch.of(Evals.objectAsBoolean(input));
        };
      }

      // The hashcode and equals are to make SubclassesMustOverrideEqualsAndHashCodeTest stop complaining..
      // RobuxPredicateFactory currently doesn't really need equals or hashcode since 'toString' method that is actually
      // called when testing equality of DimensionPredicateFilter, so it's the truly required method, but that seems
      // a bit strange. DimensionPredicateFilter should probably be reworked to use equals from RobuxPredicateFactory
      // instead of using toString.
      @Override
      public int hashCode()
      {
        return super.hashCode();
      }

      @Override
      public boolean equals(Object obj)
      {
        return super.equals(obj);
      }
    };
  }

  /**
   * {@link RobuxPredicateFactory} which evaluates the expression using the value as input, used for building predicate
   * indexes where the raw column values will be checked against this predicate
   */
  private RobuxPredicateFactory getBitmapPredicateFactory(@Nullable ColumnCapabilities inputCapabilites)
  {
    return new RobuxPredicateFactory()
    {
      @Override
      public RobuxObjectPredicate<String> makeStringPredicate()
      {
        return value -> {
          final ExprEval<?> eval = expr.get().eval(
              InputBindings.forInputSupplier(
                  ExpressionType.STRING,
                  () -> value
              )
          );
          if (eval.value() == null) {
            return RobuxPredicateMatch.UNKNOWN;
          }
          return RobuxPredicateMatch.of(eval.asBoolean());
        };
      }

      @Override
      public RobuxLongPredicate makeLongPredicate()
      {
        return new RobuxLongPredicate()
        {
          @Override
          public RobuxPredicateMatch applyLong(long input)
          {
            final ExprEval<?> eval = expr.get().eval(InputBindings.forInputSupplier(ExpressionType.LONG, () -> input));
            if (eval.isNumericNull()) {
              return RobuxPredicateMatch.UNKNOWN;
            }
            return RobuxPredicateMatch.of(eval.asBoolean());
          }

          @Override
          public RobuxPredicateMatch applyNull()
          {
            final ExprEval<?> eval = expr.get().eval(InputBindings.nilBindings());
            if (eval.isNumericNull()) {
              return RobuxPredicateMatch.UNKNOWN;
            }
            return RobuxPredicateMatch.of(eval.asBoolean());
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
            final ExprEval<?> eval = expr.get().eval(
                InputBindings.forInputSupplier(ExpressionType.DOUBLE, () -> input)
            );
            if (eval.isNumericNull()) {
              return RobuxPredicateMatch.UNKNOWN;
            }
            return RobuxPredicateMatch.of(eval.asBoolean());
          }

          @Override
          public RobuxPredicateMatch applyNull()
          {

            final ExprEval<?> eval = expr.get().eval(InputBindings.nilBindings());
            if (eval.isNumericNull()) {
              return RobuxPredicateMatch.UNKNOWN;
            }
            return RobuxPredicateMatch.of(eval.asBoolean());
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
            final ExprEval<?> eval = expr.get().eval(
                InputBindings.forInputSupplier(ExpressionType.DOUBLE, () -> input)
            );
            if (eval.isNumericNull()) {
              return RobuxPredicateMatch.UNKNOWN;
            }
            return RobuxPredicateMatch.of(eval.asBoolean());
          }

          @Override
          public RobuxPredicateMatch applyNull()
          {
            final ExprEval<?> eval = expr.get().eval(InputBindings.nilBindings());
            if (eval.isNumericNull()) {
              return RobuxPredicateMatch.UNKNOWN;
            }
            return RobuxPredicateMatch.of(eval.asBoolean());
          }
        };
      }

      @Override
      public RobuxObjectPredicate<Object[]> makeArrayPredicate(@Nullable TypeSignature<ValueType> arrayType)
      {
        if (inputCapabilites == null) {
          return input -> {
            final ExprEval<?> eval = expr.get()
                                         .eval(
                                             InputBindings.forInputSupplier(ExpressionType.STRING_ARRAY, () -> input)
                                         );
            if (eval.value() == null) {
              return RobuxPredicateMatch.UNKNOWN;
            }
            return RobuxPredicateMatch.of(eval.asBoolean());
          };
        }
        return input -> {
          ExprEval<?> eval = expr.get().eval(
              InputBindings.forInputSupplier(ExpressionType.fromColumnType(inputCapabilites), () -> input)
          );
          if (eval.value() == null) {
            return RobuxPredicateMatch.UNKNOWN;
          }
          return RobuxPredicateMatch.of(eval.asBoolean());
        };
      }

      // The hashcode and equals are to make SubclassesMustOverrideEqualsAndHashCodeTest stop complaining..
      // RobuxPredicateFactory currently doesn't really need equals or hashcode since 'toString' method that is actually
      // called when testing equality of DimensionPredicateFilter, so it's the truly required method, but that seems
      // a bit strange. DimensionPredicateFilter should probably be reworked to use equals from RobuxPredicateFactory
      // instead of using toString.
      @Override
      public int hashCode()
      {
        return super.hashCode();
      }

      @Override
      public boolean equals(Object obj)
      {
        return super.equals(obj);
      }
    };
  }
}
