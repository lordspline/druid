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

package org.apache.robux.sql.calcite.util;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.robux.initialization.RobuxModule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RobuxModuleCollection implements RobuxModule
{
  private List<Module> subModules;

  public RobuxModuleCollection(List<Module> modules)
  {
    this.subModules = ImmutableList.copyOf(modules);
  }

  @Inject
  public void cascadeInject(Injector injector)
  {
    for (Module module : subModules) {
      injector.injectMembers(module);
    }
  }

  @Override
  public final List<? extends com.fasterxml.jackson.databind.Module> getJacksonModules()
  {
    List<com.fasterxml.jackson.databind.Module> ret = new ArrayList<>();
    for (Module module : subModules) {
      if (module instanceof RobuxModule) {
        RobuxModule robuxModule = (RobuxModule) module;
        ret.addAll(robuxModule.getJacksonModules());
      }
    }
    return ret;
  }

  @Override
  public final void configure(Binder binder)
  {
    for (Module module : subModules) {
      binder.install(module);
    }
  }

  public static RobuxModule of(Module... modules)
  {
    return of(Arrays.asList(modules));
  }

  public static RobuxModule of(List<? extends Module> modules)
  {
    return new RobuxModuleCollection(flatten(modules));
  }

  public static List<Module> flatten(Module... modules)
  {
    return flatten(Arrays.asList(modules));
  }

  public static List<Module> flatten(List<? extends Module> modules)
  {
    ArrayList<Module> flattenedModules = new ArrayList<>(modules.size());
    for (Module module : modules) {
      if (module instanceof RobuxModuleCollection) {
        RobuxModuleCollection moduleCollection = (RobuxModuleCollection) module;
        flattenedModules.addAll(moduleCollection.subModules);
      } else {
        flattenedModules.add(module);
      }
    }
    return flattenedModules;
  }

}
