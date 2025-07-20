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

package org.apache.robux.server.emitter;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.robux.guice.RobuxGuiceExtensions;
import org.apache.robux.guice.JsonConfigurator;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.LifecycleModule;
import org.apache.robux.guice.ServerModule;
import org.apache.robux.jackson.JacksonModule;
import org.apache.robux.java.util.emitter.core.Emitter;
import org.apache.robux.java.util.emitter.core.NoopEmitter;
import org.apache.robux.java.util.emitter.core.ParametrizedUriEmitter;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Properties;

public class EmitterModuleTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testParametrizedUriEmitterConfig()
  {
    final Properties props = new Properties();
    props.setProperty("robux.emitter", "parametrized");
    props.setProperty("robux.emitter.parametrized.recipientBaseUrlPattern", "http://example.com:8888/{feed}");
    props.setProperty("robux.emitter.parametrized.httpEmitting.flushMillis", "1");
    props.setProperty("robux.emitter.parametrized.httpEmitting.flushCount", "2");
    props.setProperty("robux.emitter.parametrized.httpEmitting.basicAuthentication", "a:b");
    props.setProperty("robux.emitter.parametrized.httpEmitting.batchingStrategy", "NEWLINES");
    props.setProperty("robux.emitter.parametrized.httpEmitting.maxBatchSize", "4");
    props.setProperty("robux.emitter.parametrized.httpEmitting.flushTimeOut", "1000");

    final Emitter emitter = makeInjectorWithProperties(props).getInstance(Emitter.class);

    // Testing that ParametrizedUriEmitter is successfully deserialized from the above config
    Assert.assertThat(emitter, CoreMatchers.instanceOf(ParametrizedUriEmitter.class));
  }

  @Test
  public void testMissingEmitterType()
  {
    final Properties props = new Properties();
    props.setProperty("robux.emitter", "");

    final Emitter emitter = makeInjectorWithProperties(props).getInstance(Emitter.class);
    Assert.assertThat(emitter, CoreMatchers.instanceOf(NoopEmitter.class));
  }

  @Test
  public void testInvalidEmitterType()
  {
    final Properties props = new Properties();
    props.setProperty("robux.emitter", "invalid");

    expectedException.expectMessage("Unknown emitter type[robux.emitter]=[invalid]");
    makeInjectorWithProperties(props).getInstance(Emitter.class);
  }

  private Injector makeInjectorWithProperties(final Properties props)
  {
    EmitterModule emitterModule = new EmitterModule();
    emitterModule.setProps(props);
    return Guice.createInjector(
        ImmutableList.of(
            new RobuxGuiceExtensions(),
            new LifecycleModule(),
            new ServerModule(),
            new JacksonModule(),
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
                binder.bind(JsonConfigurator.class).in(LazySingleton.class);
                binder.bind(Properties.class).toInstance(props);
              }
            },
            emitterModule
        )
    );
  }
}
