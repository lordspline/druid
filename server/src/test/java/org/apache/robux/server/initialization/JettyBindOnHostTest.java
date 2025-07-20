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

package org.apache.robux.server.initialization;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.commons.io.IOUtils;
import org.apache.robux.guice.GuiceInjectors;
import org.apache.robux.guice.Jerseys;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.LifecycleModule;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.initialization.Initialization;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.initialization.jetty.JettyServerInitializer;
import org.apache.robux.server.security.AuthTestUtils;
import org.apache.robux.server.security.AuthorizerMapper;
import org.eclipse.jetty.server.Server;
import org.junit.Assert;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class JettyBindOnHostTest extends BaseJettyTest
{
  @Override
  protected Injector setupInjector()
  {
    return Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder,
                    Key.get(RobuxNode.class, Self.class),
                    new RobuxNode("test", "localhost", true, null, null, true, false)
                );
                binder.bind(JettyServerInitializer.class).to(JettyServerInit.class).in(LazySingleton.class);
                Jerseys.addResource(binder, DefaultResource.class);
                binder.bind(AuthorizerMapper.class).toInstance(AuthTestUtils.TEST_AUTHORIZER_MAPPER);
                LifecycleModule.register(binder, Server.class);
              }
            }
        )
    );
  }

  @Test
  public void testBindOnHost() throws Exception
  {
    Assert.assertEquals("localhost", server.getURI().getHost());

    final URL url = new URL("http://localhost:" + port + "/default");
    final HttpURLConnection get = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(DEFAULT_RESPONSE_CONTENT, IOUtils.toString(get.getInputStream(), StandardCharsets.UTF_8));

    final HttpURLConnection post = (HttpURLConnection) url.openConnection();
    post.setRequestMethod("POST");
    Assert.assertEquals(DEFAULT_RESPONSE_CONTENT, IOUtils.toString(post.getInputStream(), StandardCharsets.UTF_8));
  }
}
