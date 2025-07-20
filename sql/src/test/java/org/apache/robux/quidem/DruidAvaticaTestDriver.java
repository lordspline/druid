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

package org.apache.robux.quidem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import org.apache.calcite.avatica.server.AbstractAvaticaHandler;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.java.util.common.FileUtils;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.io.Closer;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.robux.sql.avatica.AvaticaMonitor;
import org.apache.robux.sql.avatica.RobuxAvaticaJsonHandler;
import org.apache.robux.sql.avatica.RobuxMeta;
import org.apache.robux.sql.calcite.SqlTestFrameworkConfig;
import org.apache.robux.sql.calcite.SqlTestFrameworkConfig.ConfigurationInstance;
import org.apache.robux.sql.calcite.SqlTestFrameworkConfig.SqlTestFrameworkConfigStore;
import org.apache.robux.sql.calcite.util.RobuxModuleCollection;
import org.apache.robux.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.apache.robux.sql.calcite.util.SqlTestFramework.QueryComponentSupplierDelegate;
import org.apache.robux.sql.hook.RobuxHookDispatcher;
import org.apache.http.client.utils.URIBuilder;
import org.eclipse.jetty.server.Server;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public class RobuxAvaticaTestDriver implements Driver
{
  static {
    new RobuxAvaticaTestDriver().register();
  }

  public static final String SCHEME = "robuxtest";
  public static final String URI_PREFIX = SCHEME + "://";
  public static final String DEFAULT_URI = URI_PREFIX + "/";

  static final SqlTestFrameworkConfigStore CONFIG_STORE = new SqlTestFrameworkConfigStore(
      x -> new AvaticaBasedTestConnectionSupplier(x)
  );

  public RobuxAvaticaTestDriver()
  {
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException
  {
    if (!acceptsURL(url)) {
      return null;
    }
    try {
      SqlTestFrameworkConfig config = SqlTestFrameworkConfig.fromURL(url);
      ConfigurationInstance ci = CONFIG_STORE.getConfigurationInstance(config);
      AvaticaJettyServer server = ci.framework.injector().getInstance(AvaticaJettyServer.class);
      return server.getConnection(info);
    }
    catch (Exception e) {
      if (e instanceof SQLException) {
        throw (SQLException) e;
      }
      // We create an Error here so that the exception is certain to make it out of the Quidem runner because it
      // captures SqlExceptions and makes the messages hard to find sometimes.
      throw new Error("Can't create testconnection", e);
    }
  }

  static class AvaticaBasedConnectionModule implements RobuxModule, Closeable
  {
    Closer closer = Closer.create();

    @Provides
    @LazySingleton
    public RobuxConnectionExtras getConnectionExtras(
        ObjectMapper objectMapper,
        RobuxHookDispatcher robuxHookDispatcher,
        @Named("isExplainSupported") Boolean isExplainSupported,
        SpecificSegmentsQuerySegmentWalker walker,
        Injector injector
    )
    {
      return new RobuxConnectionExtras.RobuxConnectionExtrasImpl(
          objectMapper,
          robuxHookDispatcher,
          isExplainSupported,
          walker,
          injector
      );
    }

    @Provides
    @LazySingleton
    public AvaticaJettyServer getAvaticaServer(RobuxMeta robuxMeta, RobuxConnectionExtras robuxConnectionExtras)
        throws Exception
    {
      AvaticaJettyServer avaticaJettyServer = new AvaticaJettyServer(robuxMeta, robuxConnectionExtras);
      closer.register(avaticaJettyServer);
      return avaticaJettyServer;
    }

    @Override
    public void configure(Binder binder)
    {
    }

    @Override
    public void close() throws IOException
    {
      closer.close();
    }
  }

  static class AvaticaJettyServer implements Closeable
  {
    final RobuxMeta robuxMeta;
    final Server server;
    final String url;
    final RobuxConnectionExtras connectionExtras;

    AvaticaJettyServer(final RobuxMeta robuxMeta, RobuxConnectionExtras robuxConnectionExtras) throws Exception
    {
      this.robuxMeta = robuxMeta;
      server = new Server(new InetSocketAddress("localhost", 0));
      server.setHandler(getAvaticaHandler(robuxMeta));
      server.start();
      url = StringUtils.format(
          "jdbc:avatica:remote:url=%s",
          new URIBuilder(server.getURI()).setPath(RobuxAvaticaJsonHandler.AVATICA_PATH).build()
      );
      connectionExtras = robuxConnectionExtras;
    }

    public Connection getConnection(Properties info) throws SQLException
    {
      Connection realConnection = DriverManager.getConnection(url, info);
      Connection proxyConnection = DynamicComposite.make(
          realConnection,
          Connection.class,
          connectionExtras,
          RobuxConnectionExtras.class
      );
      return proxyConnection;
    }

    @Override
    public void close()
    {
      robuxMeta.closeAllConnections();
      try {
        server.stop();
      }
      catch (Exception e) {
        throw new RuntimeException("Can't stop server", e);
      }
    }

    protected AbstractAvaticaHandler getAvaticaHandler(final RobuxMeta robuxMeta)
    {
      return new RobuxAvaticaJsonHandler(
          robuxMeta,
          new RobuxNode("dummy", "dummy", false, 1, null, true, false),
          new AvaticaMonitor()
      );
    }
  }

  static class AvaticaBasedTestConnectionSupplier extends QueryComponentSupplierDelegate
  {
    private AvaticaBasedConnectionModule connectionModule;

    public AvaticaBasedTestConnectionSupplier(QueryComponentSupplier delegate)
    {
      super(delegate);
      this.connectionModule = new AvaticaBasedConnectionModule();
    }

    @Override
    public RobuxModule getOverrideModule()
    {
      return RobuxModuleCollection.of(
          super.getOverrideModule(),
          connectionModule
      );
    }

    @Override
    public void close() throws IOException
    {
      connectionModule.close();
      super.close();
    }
  }

  protected File createTempFolder(String prefix)
  {
    File tempDir = FileUtils.createTempDir(prefix);
    Runtime.getRuntime().addShutdownHook(new Thread()
    {
      @Override
      public void run()
      {
        try {
          FileUtils.deleteDirectory(tempDir);
        }
        catch (IOException ex) {
          ex.printStackTrace();
        }
      }
    });
    return tempDir;
  }

  private void register()
  {
    try {
      DriverManager.registerDriver(this);
    }
    catch (SQLException e) {
      System.out.println("Error occurred while registering JDBC driver " + this.getClass().getName() + ": " + e);
    }
  }

  @Override
  public boolean acceptsURL(String url)
  {
    return url.startsWith(URI_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
  {
    throw new RuntimeException("Unimplemented method!");
  }

  @Override
  public int getMajorVersion()
  {
    return 0;
  }

  @Override
  public int getMinorVersion()
  {
    return 0;
  }

  @Override
  public boolean jdbcCompliant()
  {
    return false;
  }

  @Override
  public Logger getParentLogger()
  {
    return Logger.getLogger("");
  }
}
