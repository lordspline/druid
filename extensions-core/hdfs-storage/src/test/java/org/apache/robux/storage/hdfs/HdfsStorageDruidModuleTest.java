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

package org.apache.robux.storage.hdfs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.robux.guice.RobuxGuiceExtensions;
import org.apache.robux.guice.JsonConfigurator;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.LifecycleModule;
import org.apache.robux.inputsource.hdfs.HdfsInputSourceConfig;
import org.apache.robux.segment.loading.OmniDataSegmentKiller;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;

public class HdfsStorageRobuxModuleTest
{
  @Test
  public void testHdfsInputSourceConfigDefaultAllowedProtocols()
  {
    Properties props = new Properties();
    Injector injector = makeInjectorWithProperties(props);
    HdfsInputSourceConfig instance = injector.getInstance(HdfsInputSourceConfig.class);
    Assert.assertEquals(
        ImmutableSet.of("hdfs"),
        instance.getAllowedProtocols()
    );
  }

  @Test
  public void testHdfsInputSourceConfigCustomAllowedProtocols()
  {
    Properties props = new Properties();
    props.setProperty("robux.ingestion.hdfs.allowedProtocols", "[\"webhdfs\"]");
    Injector injector = makeInjectorWithProperties(props);
    HdfsInputSourceConfig instance = injector.getInstance(HdfsInputSourceConfig.class);
    Assert.assertEquals(
        ImmutableSet.of("webhdfs"),
        instance.getAllowedProtocols()
    );
  }

  @Test
  public void testSegmentKillerBoundAndMemoized()
  {
    Injector injector = makeInjectorWithProperties(new Properties());
    OmniDataSegmentKiller killer = injector.getInstance(OmniDataSegmentKiller.class);
    Assert.assertTrue(killer.getKillers().containsKey(HdfsStorageRobuxModule.SCHEME));
    Assert.assertSame(
        killer.getKillers().get(HdfsStorageRobuxModule.SCHEME).get(),
        killer.getKillers().get(HdfsStorageRobuxModule.SCHEME).get()
    );
  }

  private Injector makeInjectorWithProperties(final Properties props)
  {
    return Guice.createInjector(
        ImmutableList.of(
            new RobuxGuiceExtensions(),
            new LifecycleModule(),
            binder -> {
              binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
              binder.bind(JsonConfigurator.class).in(LazySingleton.class);
              binder.bind(Properties.class).toInstance(props);
            },
            new HdfsStorageRobuxModule()
        )
    );
  }
}
