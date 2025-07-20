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

package org.apache.robux.server.lookup.namespace.cache;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.robux.guice.GuiceInjectors;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.initialization.Initialization;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.lookup.namespace.NamespaceExtractionModule;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class OnHeapNamespaceExtractionCacheManagerTest
{
  @Test
  public void testInjection()
  {
    final NamespaceExtractionCacheManager manager = getCacheManager();
    Assert.assertEquals(OnHeapNamespaceExtractionCacheManager.class, manager.getClass());
  }

  @Test
  public void testImmutableAfterAttach()
  {
    NamespaceExtractionCacheManager manager = getCacheManager();
    CacheHandler handler = manager.allocateCache();
    handler.getCache().put("some key", "some value");
    CacheHandler immutableHandler = manager.attachCache(handler);
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> immutableHandler.getCache().put("other key", "other value")
    );
  }

  @Test
  public void testIllegalToDoubleAttach()
  {
    NamespaceExtractionCacheManager manager = getCacheManager();
    CacheHandler handler = manager.createCache();
    handler.getCache().put("some key", "some value");
    Assert.assertThrows(
        ISE.class,
        () -> manager.attachCache(handler)
    );
  }

  private NamespaceExtractionCacheManager getCacheManager()
  {
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder,
                    Key.get(RobuxNode.class, Self.class),
                    new RobuxNode("test-inject", null, false, null, null, true, false)
                );
              }
            }
        )
    );
    final Properties properties = injector.getInstance(Properties.class);
    properties.clear();
    properties.put(NamespaceExtractionModule.TYPE_PREFIX, "onHeap");
    final NamespaceExtractionCacheManager manager = injector.getInstance(NamespaceExtractionCacheManager.class);
    return manager;
  }
}
