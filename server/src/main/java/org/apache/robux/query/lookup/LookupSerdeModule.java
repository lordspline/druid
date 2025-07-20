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

package org.apache.robux.query.lookup;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.robux.guice.ExpressionModule;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.query.dimension.LookupDimensionSpec;
import org.apache.robux.query.expression.LookupExprMacro;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Variant of {@link LookupModule} that only supports serde of {@link org.apache.robux.query.Query} objects, to allow
 * a service to examine queries that might contain for example a {@link RegisteredLookupExtractionFn} or a
 * {@link LookupExprMacro}, but without requiring the service to load the actual lookups.
 */
public class LookupSerdeModule implements RobuxModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.<Module>of(
        new SimpleModule("RobuxLookupModule").registerSubtypes(MapLookupExtractorFactory.class),
        new SimpleModule().registerSubtypes(
            new NamedType(LookupDimensionSpec.class, "lookup"),
            new NamedType(RegisteredLookupExtractionFn.class, "registeredLookup")
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, LookupModule.PROPERTY_BASE, LookupConfig.class);
    binder.bind(LookupExtractorFactoryContainerProvider.class).to(NoopLookupExtractorFactoryContainerProvider.class);
    ExpressionModule.addExprMacro(binder, LookupExprMacro.class);
  }

  /**
   * Anything using this module doesn't actually need lookups, but the objects that get materialized during
   * deserialization expect a {@link LookupExtractorFactoryContainerProvider} to exist, so this one returns nulls.
   */
  private static class NoopLookupExtractorFactoryContainerProvider implements LookupExtractorFactoryContainerProvider
  {
    @Override
    public Set<String> getAllLookupNames()
    {
      return Collections.emptySet();
    }

    @Override
    public Optional<LookupExtractorFactoryContainer> get(String lookupName)
    {
      return Optional.empty();
    }

    @Override
    public String getCanonicalLookupName(String lookupName)
    {
      return lookupName;
    }
  }
}
