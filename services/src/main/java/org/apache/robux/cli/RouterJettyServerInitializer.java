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

package org.apache.robux.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.robux.guice.annotations.Global;
import org.apache.robux.guice.annotations.Json;
import org.apache.robux.guice.http.RobuxHttpClientConfig;
import org.apache.robux.server.AsyncManagementForwardingServlet;
import org.apache.robux.server.AsyncQueryForwardingServlet;
import org.apache.robux.server.initialization.ServerConfig;
import org.apache.robux.server.initialization.jetty.JettyServerInitUtils;
import org.apache.robux.server.initialization.jetty.JettyServerInitializer;
import org.apache.robux.server.router.ManagementProxyConfig;
import org.apache.robux.server.router.Router;
import org.apache.robux.server.security.AuthConfig;
import org.apache.robux.server.security.AuthenticationUtils;
import org.apache.robux.server.security.Authenticator;
import org.apache.robux.server.security.AuthenticatorMapper;
import org.apache.robux.sql.avatica.RobuxAvaticaJsonHandler;
import org.apache.robux.sql.avatica.RobuxAvaticaProtobufHandler;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.Servlet;
import java.util.List;

public class RouterJettyServerInitializer implements JettyServerInitializer
{
  private static final List<String> UNSECURED_PATHS = ImmutableList.of(
      "/status/health",
      // JDBC authentication uses the JDBC connection context instead of HTTP headers, skip the normal auth checks.
      // The router will keep the connection context in the forwarded message, and the broker is responsible for
      // performing the auth checks.
      RobuxAvaticaJsonHandler.AVATICA_PATH,
      RobuxAvaticaJsonHandler.AVATICA_PATH_NO_TRAILING_SLASH,
      RobuxAvaticaProtobufHandler.AVATICA_PATH,
      RobuxAvaticaProtobufHandler.AVATICA_PATH_NO_TRAILING_SLASH
  );

  private final RobuxHttpClientConfig routerHttpClientConfig;
  private final RobuxHttpClientConfig globalHttpClientConfig;
  private final ManagementProxyConfig managementProxyConfig;
  private final AsyncQueryForwardingServlet asyncQueryForwardingServlet;
  private final AsyncManagementForwardingServlet asyncManagementForwardingServlet;
  private final AuthConfig authConfig;
  private final ServerConfig serverConfig;

  @Inject
  public RouterJettyServerInitializer(
      @Router RobuxHttpClientConfig routerHttpClientConfig,
      @Global RobuxHttpClientConfig globalHttpClientConfig,
      ManagementProxyConfig managementProxyConfig,
      AsyncQueryForwardingServlet asyncQueryForwardingServlet,
      AsyncManagementForwardingServlet asyncManagementForwardingServlet,
      AuthConfig authConfig,
      ServerConfig serverConfig
  )
  {
    this.routerHttpClientConfig = routerHttpClientConfig;
    this.globalHttpClientConfig = globalHttpClientConfig;
    this.managementProxyConfig = managementProxyConfig;
    this.asyncQueryForwardingServlet = asyncQueryForwardingServlet;
    this.asyncManagementForwardingServlet = asyncManagementForwardingServlet;
    this.authConfig = authConfig;
    this.serverConfig = serverConfig;
  }

  @Override
  public void initialize(Server server, Injector injector)
  {
    final ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
    root.setInitParameter("org.eclipse.jetty.servlet.Default.dirAllowed", "false");

    root.addServlet(new ServletHolder(new DefaultServlet()), "/*");

    ServletHolder queryServletHolder = buildServletHolder(asyncQueryForwardingServlet, routerHttpClientConfig);
    root.addServlet(queryServletHolder, "/robux/v2/*");
    root.addServlet(queryServletHolder, "/robux/v1/lookups/*");

    if (managementProxyConfig.isEnabled()) {
      ServletHolder managementForwardingServletHolder = buildServletHolder(
          asyncManagementForwardingServlet,
          globalHttpClientConfig
      );
      root.addServlet(managementForwardingServletHolder, "/robux/coordinator/*");
      root.addServlet(managementForwardingServletHolder, "/robux/indexer/*");
      root.addServlet(managementForwardingServletHolder, "/proxy/*");
    }


    final ObjectMapper jsonMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    final AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);

    JettyServerInitUtils.addQosFilters(root, injector);
    AuthenticationUtils.addSecuritySanityCheckFilter(root, jsonMapper);

    // perform no-op authorization/authentication for these resources
    AuthenticationUtils.addNoopAuthenticationAndAuthorizationFilters(root, UNSECURED_PATHS);
    WebConsoleJettyServerInitializer.intializeServerForWebConsoleRoot(root);
    AuthenticationUtils.addNoopAuthenticationAndAuthorizationFilters(root, authConfig.getUnsecuredPaths());

    final List<Authenticator> authenticators = authenticatorMapper.getAuthenticatorChain();
    AuthenticationUtils.addAuthenticationFilterChain(root, authenticators);

    AuthenticationUtils.addAllowOptionsFilter(root, authConfig.isAllowUnauthenticatedHttpOptions());
    JettyServerInitUtils.addAllowHttpMethodsFilter(root, serverConfig.getAllowedHttpMethods());

    JettyServerInitUtils.addExtensionFilters(root, injector);

    // Check that requests were authorized before sending responses
    AuthenticationUtils.addPreResponseAuthorizationCheckFilter(
        root,
        authenticators,
        jsonMapper
    );

    // Can't use '/*' here because of Guice conflicts with AsyncQueryForwardingServlet path
    final FilterHolder guiceFilterHolder = JettyServerInitUtils.getGuiceFilterHolder(injector);
    root.addFilter(guiceFilterHolder, "/status/*", null);
    root.addFilter(guiceFilterHolder, "/robux/router/*", null);
    root.addFilter(guiceFilterHolder, "/robux-ext/*", null);

    RewriteHandler rewriteHandler = WebConsoleJettyServerInitializer.createWebConsoleRewriteHandler();
    JettyServerInitUtils.maybeAddHSTSPatternRule(serverConfig, rewriteHandler);

    final HandlerList handlerList = new HandlerList();
    handlerList.setHandlers(
        new Handler[]{
            rewriteHandler,
            JettyServerInitUtils.getJettyRequestLogHandler(),
            JettyServerInitUtils.wrapWithDefaultGzipHandler(
                root,
                serverConfig.getInflateBufferSize(),
                serverConfig.getCompressionLevel()
            )
        }
    );
    server.setHandler(handlerList);
  }

  private ServletHolder buildServletHolder(Servlet servlet, RobuxHttpClientConfig httpClientConfig)
  {
    ServletHolder sh = new ServletHolder(servlet);

    //NOTE: explicit maxThreads to workaround https://tickets.puppetlabs.com/browse/TK-152
    sh.setInitParameter("maxThreads", Integer.toString(httpClientConfig.getNumMaxThreads()));

    //Needs to be set in servlet config or else overridden to default value in AbstractProxyServlet.createHttpClient()
    sh.setInitParameter("maxConnections", Integer.toString(httpClientConfig.getNumConnections()));
    sh.setInitParameter("idleTimeout", Long.toString(httpClientConfig.getReadTimeout().getMillis()));
    sh.setInitParameter("timeout", Long.toString(httpClientConfig.getReadTimeout().getMillis()));
    sh.setInitParameter("requestBufferSize", Integer.toString(httpClientConfig.getRequestBuffersize()));

    return sh;
  }
}
