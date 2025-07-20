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

package org.apache.robux.server.lookup.namespace;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.PolyBind;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.query.lookup.NamespaceLookupExtractorFactory;
import org.apache.robux.query.lookup.namespace.CacheGenerator;
import org.apache.robux.query.lookup.namespace.ExtractionNamespace;
import org.apache.robux.query.lookup.namespace.JdbcExtractionNamespace;
import org.apache.robux.query.lookup.namespace.StaticMapExtractionNamespace;
import org.apache.robux.query.lookup.namespace.UriExtractionNamespace;
import org.apache.robux.server.lookup.namespace.cache.NamespaceExtractionCacheManager;
import org.apache.robux.server.lookup.namespace.cache.OffHeapNamespaceExtractionCacheManager;
import org.apache.robux.server.lookup.namespace.cache.OnHeapNamespaceExtractionCacheManager;

import java.util.List;

/**
 *
 */
public class NamespaceExtractionModule implements RobuxModule
{
  public static final String TYPE_PREFIX = "robux.lookup.namespace.cache.type";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.<Module>of(
        new SimpleModule("RobuxNamespacedCachedExtractionModule")
            .registerSubtypes(
                NamespaceLookupExtractorFactory.class
            )
    );
  }

  public static MapBinder<Class<? extends ExtractionNamespace>, CacheGenerator<?>> getNamespaceFactoryMapBinder(
      final Binder binder
  )
  {
    return MapBinder.newMapBinder(
        binder,
        new TypeLiteral<>() {},
        new TypeLiteral<>() {}
    );
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "robux.lookup.namespace", NamespaceExtractionConfig.class);

    PolyBind
        .createChoiceWithDefault(binder, TYPE_PREFIX, Key.get(NamespaceExtractionCacheManager.class), "onHeap")
        .in(LazySingleton.class);

    PolyBind
        .optionBinder(binder, Key.get(NamespaceExtractionCacheManager.class))
        .addBinding("onHeap")
        .to(OnHeapNamespaceExtractionCacheManager.class)
        .in(LazySingleton.class);

    PolyBind
        .optionBinder(binder, Key.get(NamespaceExtractionCacheManager.class))
        .addBinding("offHeap")
        .to(OffHeapNamespaceExtractionCacheManager.class)
        .in(LazySingleton.class);

    getNamespaceFactoryMapBinder(binder)
        .addBinding(JdbcExtractionNamespace.class)
        .to(JdbcCacheGenerator.class)
        .in(LazySingleton.class);
    getNamespaceFactoryMapBinder(binder)
        .addBinding(UriExtractionNamespace.class)
        .to(UriCacheGenerator.class)
        .in(LazySingleton.class);
    getNamespaceFactoryMapBinder(binder)
        .addBinding(StaticMapExtractionNamespace.class)
        .to(StaticMapCacheGenerator.class)
        .in(LazySingleton.class);
  }
}
