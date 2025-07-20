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

package org.apache.robux.msq.dart.guice;

import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.robux.discovery.NodeRole;
import org.apache.robux.guice.annotations.LoadScope;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.msq.exec.MemoryIntrospector;
import org.apache.robux.msq.exec.MemoryIntrospectorImpl;
import org.apache.robux.query.RobuxProcessingConfig;
import org.apache.robux.utils.RuntimeInfo;

/**
 * Memory management module for Brokers.
 */
@LoadScope(roles = {NodeRole.BROKER_JSON_NAME})
public class DartControllerMemoryManagementModule implements RobuxModule
{
  @Override
  public void configure(Binder binder)
  {
    // Nothing to do.
  }

  @Provides
  public MemoryIntrospector createMemoryIntrospector(
      final RuntimeInfo runtimeInfo,
      final RobuxProcessingConfig processingConfig,
      final DartControllerConfig controllerConfig
  )
  {
    return new MemoryIntrospectorImpl(
        runtimeInfo.getMaxHeapSizeBytes(),
        controllerConfig.getHeapFraction(),
        controllerConfig.getConcurrentQueries(),
        processingConfig.getNumThreads(),
        null
    );
  }
}
