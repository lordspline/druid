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

package org.apache.robux.storage.aliyun;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.robux.guice.ConfigModule;
import org.apache.robux.guice.RobuxGuiceExtensions;
import org.apache.robux.jackson.JacksonModule;
import org.apache.robux.segment.loading.OmniDataSegmentArchiver;
import org.apache.robux.segment.loading.OmniDataSegmentKiller;
import org.apache.robux.segment.loading.OmniDataSegmentMover;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class OssStorageRobuxModuleTest
{
  @Test
  public void testSegmentKillerBoundSingleton()
  {
    Injector injector = createInjector();
    OmniDataSegmentKiller killer = injector.getInstance(OmniDataSegmentKiller.class);
    Assert.assertTrue(killer.getKillers().containsKey(OssStorageRobuxModule.SCHEME_ZIP));
    Assert.assertSame(
        killer.getKillers().get(OssStorageRobuxModule.SCHEME_ZIP).get(),
        killer.getKillers().get(OssStorageRobuxModule.SCHEME_ZIP).get()
    );
  }

  @Test
  public void testSegmentArchiverBoundSingleton()
  {
    Injector injector = createInjector();
    OmniDataSegmentArchiver archiver = injector.getInstance(OmniDataSegmentArchiver.class);
    Assert.assertTrue(archiver.getArchivers().containsKey(OssStorageRobuxModule.SCHEME_ZIP));
    Assert.assertSame(
        archiver.getArchivers().get(OssStorageRobuxModule.SCHEME_ZIP).get(),
        archiver.getArchivers().get(OssStorageRobuxModule.SCHEME_ZIP).get()
    );
  }

  @Test
  public void testSegmentMoverBoundSingleton()
  {
    Injector injector = createInjector();
    OmniDataSegmentMover mover = injector.getInstance(OmniDataSegmentMover.class);
    Assert.assertTrue(mover.getMovers().containsKey(OssStorageRobuxModule.SCHEME_ZIP));
    Assert.assertSame(
        mover.getMovers().get(OssStorageRobuxModule.SCHEME_ZIP).get(),
        mover.getMovers().get(OssStorageRobuxModule.SCHEME_ZIP).get()
    );
  }

  private static Injector createInjector()
  {
    return Guice.createInjector(
        ImmutableList.of(
            new RobuxGuiceExtensions(),
            new JacksonModule(),
            new ConfigModule(),
            new OssStorageRobuxModule(),
            binder -> {
              final Properties properties = new Properties();
              properties.setProperty("robux.oss.accessKey", "accessKey");
              properties.setProperty("robux.oss.secretKey", "secretKey");
              properties.setProperty("robux.oss.endpoint", "http://localhost/");
              binder.bind(Properties.class).toInstance(properties);
            }
        )
    );
  }
}
