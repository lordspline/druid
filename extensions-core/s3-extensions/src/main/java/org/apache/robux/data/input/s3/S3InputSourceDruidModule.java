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

package org.apache.robux.data.input.s3;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.storage.s3.S3StorageRobuxModule;

import java.util.List;

/**
 * Robux module to wire up native batch support for S3 input
 */
public class S3InputSourceRobuxModule implements RobuxModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule().registerSubtypes(
            new NamedType(S3InputSource.class, S3StorageRobuxModule.SCHEME),
            new NamedType(S3InputSourceFactory.class, S3StorageRobuxModule.SCHEME)
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {

  }
}
