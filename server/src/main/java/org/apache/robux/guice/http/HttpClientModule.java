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

package org.apache.robux.guice.http;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Module;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.annotations.EscalatedClient;
import org.apache.robux.guice.annotations.EscalatedGlobal;
import org.apache.robux.guice.annotations.Global;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.http.client.HttpClient;
import org.apache.robux.java.util.http.client.HttpClientConfig;
import org.apache.robux.java.util.http.client.HttpClientInit;
import org.apache.robux.server.security.Escalator;

import javax.net.ssl.SSLContext;
import java.lang.annotation.Annotation;
import java.util.Set;

/**
 *
 */
public class HttpClientModule implements Module
{
  public static HttpClientModule global()
  {
    return new HttpClientModule("robux.global.http", Global.class, false);
  }

  public static HttpClientModule escalatedGlobal()
  {
    return new HttpClientModule("robux.global.http", EscalatedGlobal.class, false);
  }

  private static final Set<Class<? extends Annotation>> ESCALATING_ANNOTATIONS =
      ImmutableSet.of(EscalatedGlobal.class, EscalatedClient.class);

  private final String propertyPrefix;
  private final Class<? extends Annotation> annotationClazz;
  private final boolean isEscalated;
  private final boolean eagerByDefault;

  public HttpClientModule(String propertyPrefix, Class<? extends Annotation> annotationClazz, boolean eagerByDefault)
  {
    this.propertyPrefix = Preconditions.checkNotNull(propertyPrefix, "propertyPrefix");
    this.annotationClazz = Preconditions.checkNotNull(annotationClazz, "annotationClazz");
    this.eagerByDefault = eagerByDefault;

    isEscalated = ESCALATING_ANNOTATIONS.contains(this.annotationClazz);
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, propertyPrefix, RobuxHttpClientConfig.class, annotationClazz);
    binder.bind(HttpClient.class)
          .annotatedWith(annotationClazz)
          .toProvider(new HttpClientProvider(annotationClazz, isEscalated, eagerByDefault))
          .in(LazySingleton.class);
  }

  public static class HttpClientProvider extends AbstractHttpClientProvider<HttpClient>
  {
    private final boolean isEscalated;
    private final boolean eagerByDefault;
    private Escalator escalator;

    public HttpClientProvider(Class<? extends Annotation> annotationClazz, boolean isEscalated, boolean eagerByDefault)
    {
      super(annotationClazz);
      this.isEscalated = isEscalated;
      this.eagerByDefault = eagerByDefault;
    }

    @Inject
    public void inject(Escalator escalator)
    {
      this.escalator = escalator;
    }

    @Override
    public HttpClient get()
    {
      final RobuxHttpClientConfig config = getConfigProvider().get().get();

      final HttpClientConfig.Builder builder = HttpClientConfig
          .builder()
          .withNumConnections(config.getNumConnections())
          .withEagerInitialization(config.isEagerInitialization(eagerByDefault))
          .withReadTimeout(config.getReadTimeout())
          .withWorkerCount(config.getNumMaxThreads())
          .withCompressionCodec(
              HttpClientConfig.CompressionCodec.valueOf(StringUtils.toUpperCase(config.getCompressionCodec()))
          )
          .withUnusedConnectionTimeoutDuration(config.getUnusedConnectionTimeout());

      final Binding<SSLContext> sslContextBinding = getSslContextBinding();

      if (sslContextBinding != null) {
        builder.withSslContext(sslContextBinding.getProvider().get());
      }

      HttpClient client = HttpClientInit.createClient(
          builder.build(),
          getLifecycleProvider().get()
      );

      if (isEscalated) {
        return escalator.createEscalatedClient(client);
      } else {
        return client;
      }
    }
  }
}
