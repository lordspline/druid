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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.RangeSet;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.js.JavaScriptConfig;
import org.apache.robux.query.extraction.ExtractionFn;
import org.apache.robux.segment.filter.JavaScriptFilter;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.ScriptRuntime;
import org.mozilla.javascript.ScriptableObject;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;

public class JavaScriptDimFilter extends AbstractOptimizableDimFilter implements DimFilter
{
  private final String dimension;
  private final String function;
  @Nullable
  private final ExtractionFn extractionFn;
  @Nullable
  private final FilterTuning filterTuning;
  private final JavaScriptConfig config;

  /**
   * The field is declared volatile in order to ensure safe publication of the object
   * in {@link JavaScriptPredicateFactory(String, ExtractionFn)} without worrying about final modifiers
   * on the fields of the created object
   *
   * @see <a href="https://github.com/apache/robux/pull/6662#discussion_r237013157">
   *     https://github.com/apache/robux/pull/6662#discussion_r237013157</a>
   */
  @MonotonicNonNull
  private volatile JavaScriptPredicateFactory predicateFactory;

  @JsonCreator
  public JavaScriptDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("function") String function,
      @JsonProperty("extractionFn") @Nullable ExtractionFn extractionFn,
      @JsonProperty("filterTuning") @Nullable FilterTuning filterTuning,
      @JacksonInject JavaScriptConfig config
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");
    Preconditions.checkArgument(function != null, "function must not be null");
    this.dimension = dimension;
    this.function = function;
    this.extractionFn = extractionFn;
    this.filterTuning = filterTuning;
    this.config = config;
  }

  @VisibleForTesting
  public JavaScriptDimFilter(
      String dimension,
      String function,
      @Nullable ExtractionFn extractionFn,
      JavaScriptConfig config
  )
  {
    this(dimension, function, extractionFn, null, config);
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getFunction()
  {
    return function;
  }

  @Nullable
  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty
  public FilterTuning getFilterTuning()
  {
    return filterTuning;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    final byte[] functionBytes = StringUtils.toUtf8(function);
    byte[] extractionFnBytes = extractionFn == null ? new byte[0] : extractionFn.getCacheKey();

    return ByteBuffer.allocate(3 + dimensionBytes.length + functionBytes.length + extractionFnBytes.length)
                     .put(DimFilterUtils.JAVASCRIPT_CACHE_ID)
                     .put(dimensionBytes)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(functionBytes)
                     .put(DimFilterUtils.STRING_SEPARATOR)
                     .put(extractionFnBytes)
                     .array();
  }

  @Override
  public Filter toFilter()
  {
    JavaScriptPredicateFactory predicateFactory = getPredicateFactory();
    return new JavaScriptFilter(dimension, predicateFactory, filterTuning);
  }

  /**
   * This class can be used by multiple threads, so this function should be thread-safe to avoid extra
   * script compilation.
   */
  @EnsuresNonNull("predicateFactory")
  @VisibleForTesting
  JavaScriptPredicateFactory getPredicateFactory()
  {
    // JavaScript configuration should be checked when it's actually used because someone might still want Robux
    // nodes to be able to deserialize JavaScript-based objects even though JavaScript is disabled.
    Preconditions.checkState(config.isEnabled(), "JavaScript is disabled");

    JavaScriptPredicateFactory syncedFnPredicateFactory = predicateFactory;
    if (syncedFnPredicateFactory == null) {
      synchronized (config) {
        syncedFnPredicateFactory = predicateFactory;
        if (syncedFnPredicateFactory == null) {
          syncedFnPredicateFactory = new JavaScriptPredicateFactory(function, extractionFn);
          predicateFactory = syncedFnPredicateFactory;
        }
      }
    }
    return syncedFnPredicateFactory;
  }

  @Override
  public RangeSet<String> getDimensionRangeSet(String dimension)
  {
    return null;
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
    JavaScriptDimFilter that = (JavaScriptDimFilter) o;
    return dimension.equals(that.dimension) &&
           function.equals(that.function) &&
           Objects.equals(extractionFn, that.extractionFn) &&
           Objects.equals(filterTuning, that.filterTuning);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimension, function, extractionFn, filterTuning);
  }

  @Override
  public String toString()
  {
    return "JavaScriptDimFilter{" +
           "dimension='" + dimension + '\'' +
           ", function='" + function + '\'' +
           ", extractionFn=" + extractionFn +
           ", filterTuning=" + filterTuning +
           '}';
  }

  public static class JavaScriptPredicateFactory implements RobuxPredicateFactory
  {
    final ScriptableObject scope;
    final Function fnApply;
    final String script;
    final ExtractionFn extractionFn;

    public JavaScriptPredicateFactory(final String script, final ExtractionFn extractionFn)
    {
      Preconditions.checkNotNull(script, "script must not be null");
      this.script = script;
      this.extractionFn = extractionFn;

      final Context cx = Context.enter();
      try {
        cx.setOptimizationLevel(9);
        scope = cx.initStandardObjects();

        fnApply = cx.compileFunction(scope, script, "script", 1, null);
      }
      finally {
        Context.exit();
      }
    }

    @Override
    public RobuxObjectPredicate<String> makeStringPredicate()
    {
      return this::applyObject;
    }

    @Override
    public RobuxLongPredicate makeLongPredicate()
    {
      // Can't avoid boxing here because the Mozilla JS Function.call() only accepts Object[]
      return new RobuxLongPredicate()
      {
        @Override
        public RobuxPredicateMatch applyLong(long input)
        {
          return JavaScriptPredicateFactory.this.applyObject(input);
        }

        @Override
        public RobuxPredicateMatch applyNull()
        {
          return JavaScriptPredicateFactory.this.applyObject(null);
        }
      };
    }

    @Override
    public RobuxFloatPredicate makeFloatPredicate()
    {
      // Can't avoid boxing here because the Mozilla JS Function.call() only accepts Object[]
      return new RobuxFloatPredicate()
      {
        @Override
        public RobuxPredicateMatch applyFloat(float input)
        {
          return JavaScriptPredicateFactory.this.applyObject(input);
        }

        @Override
        public RobuxPredicateMatch applyNull()
        {
          return JavaScriptPredicateFactory.this.applyObject(null);
        }
      };
    }

    @Override
    public RobuxDoublePredicate makeDoublePredicate()
    {
      // Can't avoid boxing here because the Mozilla JS Function.call() only accepts Object[]
      return new RobuxDoublePredicate()
      {
        @Override
        public RobuxPredicateMatch applyDouble(double input)
        {
          return JavaScriptPredicateFactory.this.applyObject(input);
        }

        @Override
        public RobuxPredicateMatch applyNull()
        {
          return JavaScriptPredicateFactory.this.applyObject(null);
        }
      };
    }

    public RobuxPredicateMatch applyObject(final Object input)
    {
      // one and only one context per thread
      final Context cx = Context.enter();
      try {
        return RobuxPredicateMatch.of(applyInContext(cx, input));
      }
      finally {
        Context.exit();
      }
    }

    public boolean applyInContext(Context cx, Object input)
    {
      if (extractionFn != null) {
        input = extractionFn.apply(input);
      }
      Object fnResult = fnApply.call(cx, scope, scope, new Object[]{input});
      if (fnResult instanceof ScriptableObject) {
        // Direct return js function result (like arr.includes) will return org.mozilla.javascript.NativeBoolean,
        // Context.toBoolean always treat it as true, even if it is false. Convert it to java.lang.Boolean first to fix this mistake.
        return Context.toBoolean(((ScriptableObject) fnResult).getDefaultValue(ScriptRuntime.BooleanClass));
      } else {
        return Context.toBoolean(fnResult);
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

      JavaScriptPredicateFactory that = (JavaScriptPredicateFactory) o;

      if (!script.equals(that.script)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      return script.hashCode();
    }
  }
}
