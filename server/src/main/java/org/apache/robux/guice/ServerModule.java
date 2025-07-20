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

package org.apache.robux.guice;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.jackson.RobuxServiceSerializerModifier;
import org.apache.robux.jackson.StringObjectPairList;
import org.apache.robux.jackson.ToStringObjectPairListDeserializer;
import org.apache.robux.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.robux.java.util.common.concurrent.ScheduledExecutors;
import org.apache.robux.java.util.common.lifecycle.Lifecycle;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.initialization.ZkPathsConfig;

import java.util.List;

/**
 */
public class ServerModule implements RobuxModule
{
  public static final String ZK_PATHS_PROPERTY_BASE = "robux.zk.paths";

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, ZK_PATHS_PROPERTY_BASE, ZkPathsConfig.class);
    JsonConfigProvider.bind(binder, "robux", RobuxNode.class, Self.class);
  }

  @Provides @LazySingleton
  public ScheduledExecutorFactory getScheduledExecutorFactory(Lifecycle lifecycle)
  {
    return ScheduledExecutors.createFactory(lifecycle);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule()
            .addDeserializer(StringObjectPairList.class, new ToStringObjectPairListDeserializer())
            .setSerializerModifier(new RobuxServiceSerializerModifier())
    );
  }
}
