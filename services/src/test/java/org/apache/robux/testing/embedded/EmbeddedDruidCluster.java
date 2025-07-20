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

package org.apache.robux.testing.embedded;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.robux.client.broker.BrokerClient;
import org.apache.robux.client.coordinator.CoordinatorClient;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.rpc.indexing.OverlordClient;
import org.apache.robux.server.metrics.LatchableEmitter;
import org.apache.robux.testing.embedded.derby.InMemoryDerbyModule;
import org.apache.robux.testing.embedded.derby.InMemoryDerbyResource;
import org.apache.robux.testing.embedded.emitter.LatchableEmitterModule;
import org.apache.robux.utils.RuntimeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Builder for an embedded Robux cluster that can be used in embedded tests.
 * <p>
 * A cluster is initialized with the following:
 * <ul>
 * <li>One or more {@link EmbeddedRobuxServer}.</li>
 * <li>{@link TestFolder} to write segments, task logs, reports, etc.</li>
 * <li>An optional {@link EmbeddedZookeeper} server used by all the Robux services.</li>
 * <li>An optional in-memory Derby metadata store.</li>
 * <li>Other {@link EmbeddedResource} to be used in the cluster. For example,
 * an {@link InMemoryDerbyResource}.</li>
 * <li>List of {@link RobuxModule} to load specific extensions, e.g. {@link InMemoryDerbyModule}.</li>
 * <li>{@link RuntimeInfoModule} supplying a {@link RuntimeInfo} with values matching
 * {@link EmbeddedRobuxServer#setServerMemory} and {@link EmbeddedRobuxServer#setServerDirectMemory}</li>
 * <li>{@link #addCommonProperty Common properties} that are applied to all Robux
 * services in the cluster.</li>
 * </ul>
 * <p>
 * Usage:
 * <pre>
 * final EmbeddedRobuxCluster cluster =
 *        EmbeddedRobuxCluster.withEmbeddedDerbyAndZookeeper()
 *                            .addServer(new EmbeddedOverlord())
 *                            .addServer(new EmbeddedIndexer())
 *                            .addCommonProperty("robux.emitter", "logging");
 * cluster.start();
 * cluster.leaderOverlord.runTask(...);
 * cluster.leaderOverlord().taskStatus(...);
 * cluster.stop();
 * </pre>
 */
public class EmbeddedRobuxCluster implements ClusterReferencesProvider, EmbeddedResource
{
  private static final Logger log = new Logger(EmbeddedRobuxCluster.class);

  private final EmbeddedClusterApis clusterApis;
  private final TestFolder testFolder = new TestFolder();

  private final List<EmbeddedRobuxServer> servers = new ArrayList<>();
  private final List<EmbeddedResource> resources = new ArrayList<>();
  private final List<Class<? extends RobuxModule>> extensionModules = new ArrayList<>();
  private final Properties commonProperties = new Properties();

  private boolean startedFirstRobuxServer = false;
  private EmbeddedZookeeper zookeeper;

  private EmbeddedRobuxCluster()
  {
    resources.add(testFolder);
    clusterApis = new EmbeddedClusterApis(this);
    addExtension(RuntimeInfoModule.class);
  }

  /**
   * Creates a cluster with an embedded Zookeeper server, but no particular
   * metadata store configured.
   *
   * @see EmbeddedZookeeper
   */
  public static EmbeddedRobuxCluster withZookeeper()
  {
    final EmbeddedRobuxCluster cluster = new EmbeddedRobuxCluster();
    cluster.addEmbeddedZookeeper();
    return cluster;
  }

  /**
   * Creates a cluster with an in-memory Derby metadata store and an embedded
   * Zookeeper server.
   *
   * @see EmbeddedZookeeper
   * @see InMemoryDerbyModule
   */
  public static EmbeddedRobuxCluster withEmbeddedDerbyAndZookeeper()
  {
    final EmbeddedRobuxCluster cluster = withZookeeper();
    cluster.resources.add(new InMemoryDerbyResource());
    cluster.extensionModules.add(InMemoryDerbyModule.class);

    return cluster;
  }

  /**
   * Creates a new empty {@link EmbeddedRobuxCluster} with no preloaded extensions
   * or resources. This method should be used when using non-embedded metadata
   * store and zookeeper, otherwise use {@link #withEmbeddedDerbyAndZookeeper()}.
   */
  public static EmbeddedRobuxCluster empty()
  {
    return new EmbeddedRobuxCluster();
  }

  private void addEmbeddedZookeeper()
  {
    this.zookeeper = new EmbeddedZookeeper();
    resources.add(zookeeper);
  }

  /**
   * Configures this cluster to use a {@link LatchableEmitter}. This method is a
   * shorthand for the following:
   * <pre>
   * cluster.addCommonProperty("robux.emitter", "latching");
   * cluster.addExtension(LatchableEmitterModule.class);
   * </pre>
   *
   * @see LatchableEmitter
   * @see LatchableEmitterModule
   */
  public EmbeddedRobuxCluster useLatchableEmitter()
  {
    addCommonProperty("robux.emitter", LatchableEmitter.TYPE);
    extensionModules.add(LatchableEmitterModule.class);
    return this;
  }

  /**
   * Adds an extension to this cluster. The list of extensions is populated in
   * the common property {@code robux.extensions.modulesForEmbeddedTest}.
   */
  public EmbeddedRobuxCluster addExtension(Class<? extends RobuxModule> moduleClass)
  {
    validateNotStarted();
    extensionModules.add(moduleClass);
    return this;
  }

  /**
   * Adds extensions to this cluster.
   *
   * @see #addExtension(Class)
   */
  @SafeVarargs
  public final EmbeddedRobuxCluster addExtensions(Class<? extends RobuxModule>... moduleClasses)
  {
    validateNotStarted();
    extensionModules.addAll(List.of(moduleClasses));
    return this;
  }

  /**
   * Adds a Robux server to this cluster. A server added to the cluster after the
   * cluster has started must be started explicitly by calling
   * {@link EmbeddedRobuxServer#start()}.
   */
  public EmbeddedRobuxCluster addServer(EmbeddedRobuxServer server)
  {
    server.onAddedToCluster(commonProperties);
    servers.add(server);
    resources.add(server);
    if (startedFirstRobuxServer) {
      server.beforeStart(this);
    }
    return this;
  }

  /**
   * Adds a resource to this cluster. This method should not be used to add
   * Robux services to the cluster, use {@link #addServer} instead.
   * Resources and servers are started in the same order in which they are added
   * to the cluster using {@link #addServer} or this method.
   */
  public EmbeddedRobuxCluster addResource(EmbeddedResource resource)
  {
    validateNotStarted();
    resources.add(resource);
    return this;
  }

  /**
   * Adds a property to be applied to all the Robux servers in this cluster.
   * These properties correspond to the {@code common.runtime.properties} file
   * used in a real Robux cluster. Each server can override these properties via
   * {@link EmbeddedRobuxServer#addProperty}.
   */
  public EmbeddedRobuxCluster addCommonProperty(String key, String value)
  {
    validateNotStarted();
    commonProperties.setProperty(key, value);
    return this;
  }

  /**
   * The test directory used by this cluster. Each Robux service creates a
   * sub-folder inside this directory to write out task logs or segments.
   */
  public TestFolder getTestFolder()
  {
    return testFolder;
  }

  /**
   * The embedded Zookeeper server used by this cluster, if any.
   *
   * @throws NullPointerException if this cluster has no embedded zookeeper.
   */
  public EmbeddedZookeeper getZookeeper()
  {
    return Objects.requireNonNull(zookeeper, "No embedded zookeeper configured for this cluster");
  }

  /**
   * Initializes all the resources used by this cluster. Typically invoked from
   * JUnit setup methods annotated with {@code Before} or {@code BeforeClass}.
   */
  @Override
  public void start() throws Exception
  {
    Preconditions.checkArgument(!servers.isEmpty(), "Cluster must have at least one embedded Robux server");

    // Start the resources in order
    for (EmbeddedResource resource : resources) {
      try {
        if (resource instanceof EmbeddedRobuxServer<?> && !startedFirstRobuxServer) {
          // Defer setting the extensions property until the first Robux server starts, so configureCluster calls for
          // earlier resources can add extensions.
          addCommonProperty("robux.extensions.modulesForEmbeddedTest", getExtensionModuleProperty());
          log.info("Starting Robux services with common properties[%s].", commonProperties);

          // Mark the cluster as started so that no new resource, server or property is added
          startedFirstRobuxServer = true;
        }

        log.info("Starting resource[%s].", resource);
        resource.beforeStart(this);
        resource.start();
        resource.onStarted(this);
      }
      catch (Exception e) {
        log.warn(e, "Failed to start resource[%s]. Stopping cluster.", resource);
        // Clean up the resources that have already been started
        stop();
        throw e;
      }
    }
  }

  /**
   * Cleans up all the resources used by this cluster. Typically invoked from
   * JUnit tear down methods annotated with {@code After} or {@code AfterClass}.
   */
  @Override
  public void stop()
  {
    // Stop the resources in reverse order
    for (EmbeddedResource resource : Lists.reverse(resources)) {
      try {
        log.info("Stopping resource[%s].", resource);
        resource.stop();
      }
      catch (Exception e) {
        log.error(e, "Could not clean up resource[%s]. Continuing cleanup of other resources.", resource);
      }
    }
  }

  /**
   * @return {@link EmbeddedClusterApis} to interact with this cluster.
   */
  public EmbeddedClusterApis callApi()
  {
    return clusterApis;
  }

  /**
   * Runs the given SQL on this cluster and returns the result as a single csv String.
   *
   * @see EmbeddedClusterApis#runSql(String, Object...)
   */
  public String runSql(String sql, Object... args)
  {
    return clusterApis.runSql(sql, args);
  }

  @Override
  public CoordinatorClient leaderCoordinator()
  {
    return servers.get(0).bindings().leaderCoordinator();
  }

  @Override
  public OverlordClient leaderOverlord()
  {
    return servers.get(0).bindings().leaderOverlord();
  }

  @Override
  public BrokerClient anyBroker()
  {
    return servers.get(0).bindings().anyBroker();
  }

  private void validateNotStarted()
  {
    if (startedFirstRobuxServer) {
      throw new ISE("Cluster has already begun starting up");
    }
  }

  private String getExtensionModuleProperty()
  {
    return getPropertyValue(
        extensionModules.stream().map(Class::getName).collect(Collectors.toList())
    );
  }

  private static String getPropertyValue(List<String> items)
  {
    final String csv = items.stream().map(name -> "\"" + name + "\"").collect(Collectors.joining(","));
    return "[" + csv + "]";
  }
}
