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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.robux.error.InvalidInput;
import org.apache.robux.java.util.common.IAE;
import org.apache.robux.math.expr.ExprEval;
import org.apache.robux.math.expr.ExprType;
import org.apache.robux.math.expr.ExpressionType;
import org.apache.robux.query.cache.CacheKeyBuilder;
import org.apache.robux.query.filter.vector.VectorValueMatcher;
import org.apache.robux.query.filter.vector.VectorValueMatcherColumnProcessorFactory;
import org.apache.robux.segment.BaseDoubleColumnValueSelector;
import org.apache.robux.segment.BaseFloatColumnValueSelector;
import org.apache.robux.segment.BaseLongColumnValueSelector;
import org.apache.robux.segment.BaseObjectColumnValueSelector;
import org.apache.robux.segment.ColumnInspector;
import org.apache.robux.segment.ColumnProcessorFactory;
import org.apache.robux.segment.ColumnProcessors;
import org.apache.robux.segment.ColumnSelectorFactory;
import org.apache.robux.segment.DimensionHandlerUtils;
import org.apache.robux.segment.DimensionSelector;
import org.apache.robux.segment.column.ColumnCapabilities;
import org.apache.robux.segment.column.ColumnIndexSupplier;
import org.apache.robux.segment.column.ColumnType;
import org.apache.robux.segment.column.TypeSignature;
import org.apache.robux.segment.column.TypeStrategy;
import org.apache.robux.segment.column.Types;
import org.apache.robux.segment.column.ValueType;
import org.apache.robux.segment.filter.Filters;
import org.apache.robux.segment.filter.PredicateValueMatcherFactory;
import org.apache.robux.segment.filter.ValueMatchers;
import org.apache.robux.segment.index.AllUnknownBitmapColumnIndex;
import org.apache.robux.segment.index.BitmapColumnIndex;
import org.apache.robux.segment.index.semantic.RobuxPredicateIndexes;
import org.apache.robux.segment.index.semantic.StringValueSetIndexes;
import org.apache.robux.segment.index.semantic.ValueIndexes;
import org.apache.robux.segment.nested.StructuredData;
import org.apache.robux.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class EqualityFilter extends AbstractOptimizableDimFilter implements Filter
{
  private final String column;
  private final ColumnType matchValueType;
  private final Object matchValue;
  private final ExprEval<?> matchValueEval;

  @Nullable
  private final FilterTuning filterTuning;
  private final RobuxPredicateFactory predicateFactory;

  @JsonCreator
  public EqualityFilter(
      @JsonProperty("column") String column,
      @JsonProperty("matchValueType") ColumnType matchValueType,
      @JsonProperty("matchValue") Object matchValue,
      @JsonProperty("filterTuning") @Nullable FilterTuning filterTuning
  )
  {
    if (column == null) {
      throw InvalidInput.exception("Invalid equality filter, column cannot be null");
    }
    this.column = column;
    if (matchValueType == null) {
      throw InvalidInput.exception("Invalid equality filter on column [%s], matchValueType cannot be null", column);
    }
    this.matchValueType = matchValueType;
    this.matchValue = matchValue;
    this.matchValueEval = ExprEval.ofType(ExpressionType.fromColumnTypeStrict(matchValueType), matchValue);
    if (matchValueEval.value() == null) {
      throw InvalidInput.exception("Invalid equality filter on column [%s], matchValue cannot be null", column);
    }
    this.filterTuning = filterTuning;
    this.predicateFactory = new EqualityPredicateFactory(matchValueEval);
  }

  @Override
  public byte[] getCacheKey()
  {
    final TypeStrategy<Object> typeStrategy = matchValueEval.type().getStrategy();
    final int size = typeStrategy.estimateSizeBytes(matchValueEval.value());
    final ByteBuffer valueBuffer = ByteBuffer.allocate(size);
    typeStrategy.write(valueBuffer, matchValueEval.value(), size);
    return new CacheKeyBuilder(DimFilterUtils.EQUALS_CACHE_ID)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendString(column)
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendString(matchValueType.asTypeString())
        .appendByte(DimFilterUtils.STRING_SEPARATOR)
        .appendByteArray(valueBuffer.array())
        .build();
  }

  @Override
  public Filter toFilter()
  {
    return this;
  }

  @JsonProperty
  public String getColumn()
  {
    return column;
  }

  @JsonProperty
  public ColumnType getMatchValueType()
  {
    return matchValueType;
  }

  @JsonProperty
  public Object getMatchValue()
  {
    return matchValue;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public FilterTuning getFilterTuning()
  {
    return filterTuning;
  }

  @Override
  public String toString()
  {
    DimFilter.DimFilterToStringBuilder bob =
        new DimFilter.DimFilterToStringBuilder().appendDimension(column, null)
                                                .append(" = ")
                                                .append(
                                                    matchValueEval.isArray()
                                                    ? Arrays.deepToString(matchValueEval.asArray())
                                                    : matchValueEval.value()
                                                );

    if (!ColumnType.STRING.equals(matchValueType)) {
      bob.append(" (" + matchValueType.asTypeString() + ")");
    }
    return bob.appendFilterTuning(filterTuning).build();
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
    EqualityFilter that = (EqualityFilter) o;
    if (!column.equals(that.column)) {
      return false;
    }
    if (!Objects.equals(matchValueType, that.matchValueType)) {
      return false;
    }
    if (!Objects.equals(filterTuning, that.filterTuning)) {
      return false;
    }
    if (matchValueType.isArray()) {
      return Arrays.deepEquals(matchValueEval.asArray(), that.matchValueEval.asArray());
    } else {
      return Objects.equals(matchValueEval.value(), that.matchValueEval.value());
    }
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(column, matchValueType, matchValueEval.value(), filterTuning);
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    if (!Objects.equals(getColumn(), dimension)) {
      return null;
    }
    RangeSet<String> retSet = TreeRangeSet.create();
    if (matchValueEval.isArray()) {
      retSet.add(Range.singleton(Arrays.deepToString(matchValueEval.asArray())));
    } else {
      retSet.add(Range.singleton(matchValueEval.asString()));
    }
    return retSet;
  }

  @Nullable
  @Override
  public BitmapColumnIndex getBitmapColumnIndex(ColumnIndexSelector selector)
  {
    if (!Filters.checkFilterTuningUseIndex(column, selector, filterTuning)) {
      return null;
    }
    return getEqualityIndex(column, matchValueEval, matchValueType, selector, predicateFactory);
  }

  @Override
  public ValueMatcher makeMatcher(ColumnSelectorFactory factory)
  {
    return ColumnProcessors.makeProcessor(
        column,
        new TypedConstantValueMatcherFactory(matchValueEval, predicateFactory),
        factory
    );
  }

  @Override
  public VectorValueMatcher makeVectorMatcher(VectorColumnSelectorFactory factory)
  {
    final ColumnCapabilities capabilities = factory.getColumnCapabilities(column);
    final boolean primitiveMatch = matchValueType.isPrimitive() && (capabilities == null || capabilities.isPrimitive());
    if (primitiveMatch && useSimpleEquality(capabilities, matchValueType)) {
      // if possible, use simplified value matcher instead of predicate
      return ColumnProcessors.makeVectorProcessor(
          column,
          VectorValueMatcherColumnProcessorFactory.instance(),
          factory
      ).makeMatcher(matchValueEval.value(), matchValueType);
    }
    return ColumnProcessors.makeVectorProcessor(
        column,
        VectorValueMatcherColumnProcessorFactory.instance(),
        factory
    ).makeMatcher(new EqualityPredicateFactory(matchValueEval));
  }

  @Override
  public boolean canVectorizeMatcher(ColumnInspector inspector)
  {
    return true;
  }

  @Override
  public Set<String> getRequiredColumns()
  {
    return ImmutableSet.of(column);
  }

  @Override
  public boolean supportsRequiredColumnRewrite()
  {
    return true;
  }

  @Override
  public Filter rewriteRequiredColumns(Map<String, String> columnRewrites)
  {
    String rewriteDimensionTo = columnRewrites.get(column);

    if (rewriteDimensionTo == null) {
      throw new IAE(
          "Received a non-applicable rewrite: %s, filter's dimension: %s",
          columnRewrites,
          columnRewrites
      );
    }

    return new EqualityFilter(
        rewriteDimensionTo,
        matchValueType,
        matchValue,
        filterTuning
    );
  }

  /**
   * Can the match value type be cast directly to column type for equality comparison? For non-numeric match types, we
   * just use exact string equality regardless of the column type. For numeric match value types against string columns,
   * we instead cast the string to the match value type number for matching equality.
   */
  public static boolean useSimpleEquality(TypeSignature<ValueType> columnType, ColumnType matchValueType)
  {
    if (Types.is(columnType, ValueType.STRING)) {
      return !matchValueType.isNumeric();
    }
    return true;
  }

  @Nullable
  public static BitmapColumnIndex getEqualityIndex(
      String column,
      ExprEval<?> matchValueEval,
      ColumnType matchValueType,
      ColumnIndexSelector selector,
      RobuxPredicateFactory predicateFactory
  )
  {
    final ColumnIndexSupplier indexSupplier = selector.getIndexSupplier(column);
    if (indexSupplier == null) {
      return new AllUnknownBitmapColumnIndex(selector);
    }

    if (useSimpleEquality(selector.getColumnCapabilities(column), matchValueType)) {
      final ValueIndexes valueIndexes = indexSupplier.as(ValueIndexes.class);
      if (valueIndexes != null) {
        // matchValueEval.value() cannot be null here due to check in the constructor
        //noinspection DataFlowIssue
        return valueIndexes.forValue(matchValueEval.value(), matchValueType);
      }
      if (matchValueType.isPrimitive()) {
        final StringValueSetIndexes stringValueSetIndexes = indexSupplier.as(StringValueSetIndexes.class);
        if (stringValueSetIndexes != null) {

          return stringValueSetIndexes.forValue(matchValueEval.asString());
        }
      }
    }

    // fall back to predicate based index if it is available
    final RobuxPredicateIndexes predicateIndexes = indexSupplier.as(RobuxPredicateIndexes.class);
    if (predicateIndexes != null) {
      return predicateIndexes.forPredicate(predicateFactory);
    }

    // column exists, but has no indexes we can use
    return null;
  }

  public static class EqualityPredicateFactory implements RobuxPredicateFactory
  {
    private final ExprEval<?> matchValue;
    private final Supplier<RobuxObjectPredicate<String>> stringPredicateSupplier;
    private final Supplier<RobuxLongPredicate> longPredicateSupplier;
    private final Supplier<RobuxFloatPredicate> floatPredicateSupplier;
    private final Supplier<RobuxDoublePredicate> doublePredicateSupplier;
    private final ConcurrentHashMap<TypeSignature<ValueType>, RobuxObjectPredicate<Object[]>> arrayPredicates;
    private final Supplier<RobuxObjectPredicate<Object[]>> typeDetectingArrayPredicateSupplier;
    private final Supplier<RobuxObjectPredicate<Object>> objectPredicateSupplier;

    public EqualityPredicateFactory(ExprEval<?> matchValue)
    {
      this.matchValue = matchValue;
      this.stringPredicateSupplier = makeStringPredicateSupplier();
      this.longPredicateSupplier = makeLongPredicateSupplier();
      this.floatPredicateSupplier = makeFloatPredicateSupplier();
      this.doublePredicateSupplier = makeDoublePredicateSupplier();
      this.objectPredicateSupplier = makeObjectPredicateSupplier();
      this.arrayPredicates = new ConcurrentHashMap<>();
      this.typeDetectingArrayPredicateSupplier = makeTypeDetectingArrayPredicate();
    }

    @Override
    public RobuxObjectPredicate<String> makeStringPredicate()
    {
      return stringPredicateSupplier.get();
    }

    @Override
    public RobuxLongPredicate makeLongPredicate()
    {
      return longPredicateSupplier.get();
    }

    @Override
    public RobuxFloatPredicate makeFloatPredicate()
    {
      return floatPredicateSupplier.get();
    }

    @Override
    public RobuxDoublePredicate makeDoublePredicate()
    {
      return doublePredicateSupplier.get();
    }

    @Override
    public RobuxObjectPredicate<Object[]> makeArrayPredicate(@Nullable TypeSignature<ValueType> arrayType)
    {
      if (!matchValue.isArray()) {
        return RobuxObjectPredicate.alwaysFalseWithNullUnknown();
      }
      if (arrayType == null) {
        // fall back to per row detection if input array type is unknown
        return typeDetectingArrayPredicateSupplier.get();
      }

      return new FallbackPredicate<>(
          arrayPredicates.computeIfAbsent(arrayType, (existing) -> makeArrayPredicateInternal(arrayType)),
          ExpressionType.fromColumnTypeStrict(arrayType)
      );
    }

    @Override
    public RobuxObjectPredicate<Object> makeObjectPredicate()
    {
      return objectPredicateSupplier.get();
    }

    private Supplier<RobuxObjectPredicate<String>> makeStringPredicateSupplier()
    {
      return Suppliers.memoize(() -> {
        // when matching strings to numeric match values, use numeric comparator to implicitly cast the string to number
        if (matchValue.type().isNumeric()) {
          if (matchValue.type().is(ExprType.LONG)) {
            return value -> {
              if (value == null) {
                return RobuxPredicateMatch.UNKNOWN;
              }
              final Long l = DimensionHandlerUtils.convertObjectToLong(value);
              if (l == null) {
                return RobuxPredicateMatch.FALSE;
              }
              return RobuxPredicateMatch.of(matchValue.asLong() == l);
            };
          } else {
            return value -> {
              if (value == null) {
                return RobuxPredicateMatch.UNKNOWN;
              }
              final Double d = DimensionHandlerUtils.convertObjectToDouble(value);
              if (d == null) {
                return RobuxPredicateMatch.FALSE;
              }
              return RobuxPredicateMatch.of(matchValue.asDouble() == d);
            };
          }
        } else {
          final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(matchValue, ExpressionType.STRING);
          if (castForComparison == null) {
            return RobuxObjectPredicate.alwaysFalseWithNullUnknown();
          }
          return RobuxObjectPredicate.equalTo(castForComparison.asString());
        }
      });
    }

    private Supplier<RobuxLongPredicate> makeLongPredicateSupplier()
    {
      return Suppliers.memoize(() -> {
        final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(matchValue, ExpressionType.LONG);
        if (castForComparison == null) {
          return RobuxLongPredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
        } else {
          // store the primitive, so we don't unbox for every comparison
          final long unboxedLong = castForComparison.asLong();
          return input -> RobuxPredicateMatch.of(input == unboxedLong);
        }
      });
    }

    private Supplier<RobuxFloatPredicate> makeFloatPredicateSupplier()
    {
      return Suppliers.memoize(() -> {
        final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(matchValue, ExpressionType.DOUBLE);
        if (castForComparison == null) {
          return RobuxFloatPredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
        } else {
          // Compare with floatToIntBits instead of == to canonicalize NaNs.
          final int floatBits = Float.floatToIntBits((float) castForComparison.asDouble());
          return input -> RobuxPredicateMatch.of(Float.floatToIntBits(input) == floatBits);
        }
      });
    }

    private Supplier<RobuxDoublePredicate> makeDoublePredicateSupplier()
    {
      return Suppliers.memoize(() -> {
        final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(matchValue, ExpressionType.DOUBLE);
        if (castForComparison == null) {
          return RobuxDoublePredicate.ALWAYS_FALSE_WITH_NULL_UNKNOWN;
        } else {
          // Compare with doubleToLongBits instead of == to canonicalize NaNs.
          final long bits = Double.doubleToLongBits(castForComparison.asDouble());
          return input -> RobuxPredicateMatch.of(Double.doubleToLongBits(input) == bits);
        }
      });
    }

    private Supplier<RobuxObjectPredicate<Object>> makeObjectPredicateSupplier()
    {
      return Suppliers.memoize(() -> {
        if (matchValue.type().equals(ExpressionType.NESTED_DATA)) {
          return input -> input == null ? RobuxPredicateMatch.UNKNOWN : RobuxPredicateMatch.of(Objects.equals(StructuredData.unwrap(input), StructuredData.unwrap(matchValue.value())));
        }
        return RobuxObjectPredicate.equalTo(matchValue.valueOrDefault());
      });
    }

    private Supplier<RobuxObjectPredicate<Object[]>> makeTypeDetectingArrayPredicate()
    {
      return Suppliers.memoize(() -> input -> {
        if (input == null) {
          return RobuxPredicateMatch.UNKNOWN;
        }
        final ExprEval<?> eval = ExprEval.bestEffortOf(input);
        final Comparator<Object[]> arrayComparator = eval.type().getNullableStrategy();
        final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(matchValue, eval.type());
        if (castForComparison == null) {
          return RobuxPredicateMatch.UNKNOWN;
        }
        final Object[] matchArray = castForComparison.asArray();
        return RobuxPredicateMatch.of(arrayComparator.compare(input, matchArray) == 0);
      });
    }

    private RobuxObjectPredicate<Object[]> makeArrayPredicateInternal(TypeSignature<ValueType> arrayType)
    {
      final ExpressionType expressionType = ExpressionType.fromColumnTypeStrict(arrayType);
      final Comparator<Object[]> arrayComparator = arrayType.getNullableStrategy();

      final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(matchValue, expressionType);
      if (castForComparison == null) {
        return RobuxObjectPredicate.alwaysFalseWithNullUnknown();
      }
      final Object[] matchArray = castForComparison.asArray();
      return input -> input == null ? RobuxPredicateMatch.UNKNOWN : RobuxPredicateMatch.of(arrayComparator.compare(input, matchArray) == 0);
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
      EqualityPredicateFactory that = (EqualityPredicateFactory) o;
      if (!Objects.equals(matchValue.type(), that.matchValue.type())) {
        return false;
      }
      if (matchValue.isArray()) {
        return Arrays.deepEquals(matchValue.asArray(), that.matchValue.asArray());
      }
      return Objects.equals(matchValue.value(), that.matchValue.value());
    }


    @Override
    public int hashCode()
    {
      return Objects.hash(matchValue);
    }
  }

  public static class TypedConstantValueMatcherFactory implements ColumnProcessorFactory<ValueMatcher>
  {
    protected final ExprEval<?> matchValue;
    protected final PredicateValueMatcherFactory predicateMatcherFactory;

    public TypedConstantValueMatcherFactory(
        ExprEval<?> matchValue,
        RobuxPredicateFactory predicateFactory
    )
    {
      this.matchValue = matchValue;
      this.predicateMatcherFactory = new PredicateValueMatcherFactory(predicateFactory);
    }

    @Override
    public ColumnType defaultType()
    {
      return ColumnType.UNKNOWN_COMPLEX;
    }

    @Override
    public ValueMatcher makeDimensionProcessor(DimensionSelector selector, boolean multiValue)
    {
      // use the predicate matcher when matching numeric values since it casts the strings to numeric types
      if (matchValue.type().isNumeric()) {
        return predicateMatcherFactory.makeDimensionProcessor(selector, multiValue);
      }
      final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(matchValue, ExpressionType.STRING);
      if (castForComparison == null) {
        return ValueMatchers.makeAlwaysFalseWithNullUnknownDimensionMatcher(selector, multiValue);
      }
      return ValueMatchers.makeStringValueMatcher(selector, castForComparison.asString(), multiValue);
    }

    @Override
    public ValueMatcher makeFloatProcessor(BaseFloatColumnValueSelector selector)
    {
      final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(matchValue, ExpressionType.DOUBLE);
      if (castForComparison == null) {
        return ValueMatchers.makeAlwaysFalseWithNullUnknownNumericMatcher(selector);
      }
      return ValueMatchers.makeFloatValueMatcher(selector, (float) castForComparison.asDouble());
    }

    @Override
    public ValueMatcher makeDoubleProcessor(BaseDoubleColumnValueSelector selector)
    {
      final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(matchValue, ExpressionType.DOUBLE);
      if (castForComparison == null) {
        return ValueMatchers.makeAlwaysFalseWithNullUnknownNumericMatcher(selector);
      }
      return ValueMatchers.makeDoubleValueMatcher(selector, castForComparison.asDouble());
    }

    @Override
    public ValueMatcher makeLongProcessor(BaseLongColumnValueSelector selector)
    {
      final ExprEval<?> castForComparison = ExprEval.castForEqualityComparison(matchValue, ExpressionType.LONG);
      if (castForComparison == null) {
        return ValueMatchers.makeAlwaysFalseWithNullUnknownNumericMatcher(selector);
      }
      return ValueMatchers.makeLongValueMatcher(selector, castForComparison.asLong());
    }

    @Override
    public ValueMatcher makeArrayProcessor(
        BaseObjectColumnValueSelector<?> selector,
        @Nullable ColumnCapabilities columnCapabilities
    )
    {
      return predicateMatcherFactory.makeArrayProcessor(selector, columnCapabilities);
    }

    @Override
    public ValueMatcher makeComplexProcessor(BaseObjectColumnValueSelector<?> selector)
    {
      return predicateMatcherFactory.makeComplexProcessor(selector);
    }
  }
}
