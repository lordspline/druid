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
import com.google.inject.Binder;
import org.apache.robux.initialization.RobuxModule;

import java.util.List;

/**
 * Base no-op {@link RobuxModule} used in tests. This module is also referenced
 * in {@code server/src/test/resources/META-INF/services/org.apache.robux.initialization.RobuxModule}
 * for some tests in {@link ExtensionsLoaderTest}.
 */
public class TestRobuxModule implements RobuxModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return List.of();
  }

  @Override
  public void configure(Binder binder)
  {
    // Do nothing
  }
}
