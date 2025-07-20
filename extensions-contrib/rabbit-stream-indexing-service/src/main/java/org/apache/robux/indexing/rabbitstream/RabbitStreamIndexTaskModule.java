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

package org.apache.robux.indexing.rabbitstream;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.robux.indexing.rabbitstream.supervisor.RabbitStreamSupervisorSpec;
import org.apache.robux.indexing.rabbitstream.supervisor.RabbitStreamSupervisorTuningConfig;
import org.apache.robux.initialization.RobuxModule;

import java.util.List;

public class RabbitStreamIndexTaskModule implements RobuxModule
{

  static final String PROPERTY_BASE = "robux.rabbit";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule(getClass().getSimpleName())
            .registerSubtypes(
                new NamedType(RabbitStreamIndexTask.class, "index_rabbit"),
                new NamedType(RabbitStreamDataSourceMetadata.class, "rabbit"),
                new NamedType(RabbitStreamIndexTaskIOConfig.class, "rabbit"),
                new NamedType(RabbitStreamIndexTaskTuningConfig.class, "RabbitTuningConfig"),
                new NamedType(RabbitStreamSupervisorTuningConfig.class, "rabbit"),
                new NamedType(RabbitStreamSupervisorSpec.class, "rabbit"),
                new NamedType(RabbitStreamSamplerSpec.class, "rabbit")));
  }

  @Override
  public void configure(Binder binder)
  {
    // Nothing to do.
  }
}
