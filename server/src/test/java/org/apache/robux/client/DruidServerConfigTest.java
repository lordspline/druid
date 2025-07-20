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

package org.apache.robux.client;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.robux.guice.GuiceInjectors;
import org.apache.robux.initialization.Initialization;
import org.apache.robux.jackson.DefaultObjectMapper;
import org.apache.robux.segment.loading.SegmentLoaderConfig;
import org.apache.robux.segment.loading.StorageLocationConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class RobuxServerConfigTest
{
  private File testSegmentCacheDir1;
  private File testSegmentCacheDir2;

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

  public ObjectMapper mapper = new DefaultObjectMapper();

  private static final Module SERVER_CONFIG_MODULE = (binder) -> {
    binder.bindConstant().annotatedWith(Names.named("serviceName")).to("robux/test");
    binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
    binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
  };

  @Before
  public void setUp() throws Exception
  {
    testSegmentCacheDir1 = tmpFolder.newFolder("segment_cache_folder1");
    testSegmentCacheDir2 = tmpFolder.newFolder("segment_cache_folder2");
  }

  @Test
  public void testBasicInjection()
  {
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.of(SERVER_CONFIG_MODULE)
    );
    final RobuxServerConfig robuxServerConfig = injector.getInstance(RobuxServerConfig.class);

    Assert.assertNotNull(robuxServerConfig);
    Assert.assertEquals(RobuxServerConfig.class, robuxServerConfig.getClass());
  }

  @Test
  public void testCombinedSize()
  {
    final List<StorageLocationConfig> locations = new ArrayList<>();
    final StorageLocationConfig locationConfig1 = new StorageLocationConfig(testSegmentCacheDir1, 10000000000L, null);
    final StorageLocationConfig locationConfig2 = new StorageLocationConfig(testSegmentCacheDir2, 20000000000L, null);
    locations.add(locationConfig1);
    locations.add(locationConfig2);
    RobuxServerConfig robuxServerConfig = new RobuxServerConfig(new SegmentLoaderConfig().withLocations(locations));
    Assert.assertEquals(30000000000L, robuxServerConfig.getMaxSize());
  }

  @Test
  public void testServerMaxSizePrecedence() throws Exception
  {
    String serverConfigWithDefaultSizeStr = "{\"maxSize\":0,\"tier\":\"_default_tier\",\"priority\":0,"
                                            + "\"hiddenProperties\":[\"robux.metadata.storage.connector.password\","
                                            + "\"robux.s3.accessKey\",\"robux.s3.secretKey\"]}\n";

    String serverConfigWithNonDefaultSizeStr = "{\"maxSize\":123456,\"tier\":\"_default_tier\",\"priority\":0,"
                                               + "\"hiddenProperties\":[\"robux.metadata.storage.connector.password\","
                                               + "\"robux.s3.accessKey\",\"robux.s3.secretKey\"]}\n";

    final List<StorageLocationConfig> locations = new ArrayList<>();
    final StorageLocationConfig locationConfig1 = new StorageLocationConfig(testSegmentCacheDir1, 10000000000L, null);
    locations.add(locationConfig1);
    mapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, new DefaultObjectMapper())
                                                         .addValue(
                                                             SegmentLoaderConfig.class,
                                                             new SegmentLoaderConfig().withLocations(locations)
                                                         ));

    RobuxServerConfig serverConfigWithDefaultSize = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(serverConfigWithDefaultSizeStr, RobuxServerConfig.class)
        ),
        RobuxServerConfig.class
    );

    RobuxServerConfig serverConfigWithNonDefaultSize = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(serverConfigWithNonDefaultSizeStr, RobuxServerConfig.class)
        ),
        RobuxServerConfig.class
    );

    Assert.assertEquals(serverConfigWithDefaultSize.getMaxSize(), 10000000000L);
    Assert.assertEquals(serverConfigWithNonDefaultSize.getMaxSize(), 123456L);
  }
}

