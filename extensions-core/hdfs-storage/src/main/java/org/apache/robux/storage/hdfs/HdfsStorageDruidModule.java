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

package org.apache.robux.storage.hdfs;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.multibindings.MapBinder;
import org.apache.robux.data.SearchableVersionedDataFinder;
import org.apache.robux.guice.Binders;
import org.apache.robux.guice.Hdfs;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.LazySingleton;
import org.apache.robux.guice.LifecycleModule;
import org.apache.robux.guice.ManageLifecycle;
import org.apache.robux.initialization.RobuxModule;
import org.apache.robux.inputsource.hdfs.HdfsInputSource;
import org.apache.robux.inputsource.hdfs.HdfsInputSourceConfig;
import org.apache.robux.inputsource.hdfs.HdfsInputSourceFactory;
import org.apache.robux.storage.hdfs.tasklog.HdfsTaskLogs;
import org.apache.robux.storage.hdfs.tasklog.HdfsTaskLogsConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 *
 */
public class HdfsStorageRobuxModule implements RobuxModule
{
  public static final String SCHEME = "hdfs";
  private Properties props = null;

  @Inject
  public void setProperties(Properties props)
  {
    this.props = props;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule().registerSubtypes(
            new NamedType(HdfsLoadSpec.class, HdfsStorageRobuxModule.SCHEME),
            new NamedType(HdfsInputSource.class, HdfsStorageRobuxModule.SCHEME),
            new NamedType(HdfsInputSourceFactory.class, HdfsStorageRobuxModule.SCHEME)
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    MapBinder.newMapBinder(binder, String.class, SearchableVersionedDataFinder.class)
             .addBinding(SCHEME)
             .to(HdfsFileTimestampVersionFinder.class)
             .in(LazySingleton.class);

    Binders.dataSegmentPusherBinder(binder).addBinding(SCHEME).to(HdfsDataSegmentPusher.class).in(LazySingleton.class);
    Binders.dataSegmentKillerBinder(binder).addBinding(SCHEME).to(HdfsDataSegmentKiller.class).in(LazySingleton.class);

    final Configuration conf = new Configuration();

    // Set explicit CL. Otherwise it'll try to use thread context CL, which may not have all of our dependencies.
    conf.setClassLoader(getClass().getClassLoader());

    // Ensure that FileSystem class level initialization happens with correct CL
    // See https://github.com/apache/robux/issues/1714
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      FileSystem.get(conf);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }

    if (props != null) {
      for (String propName : props.stringPropertyNames()) {
        if (propName.startsWith("hadoop.")) {
          conf.set(propName.substring("hadoop.".length()), props.getProperty(propName));
        }
      }
    }

    binder.bind(Configuration.class).annotatedWith(Hdfs.class).toInstance(conf);
    JsonConfigProvider.bind(binder, "robux.storage", HdfsDataSegmentPusherConfig.class);

    Binders.taskLogsBinder(binder).addBinding("hdfs").to(HdfsTaskLogs.class);
    JsonConfigProvider.bind(binder, "robux.indexer.logs", HdfsTaskLogsConfig.class);
    binder.bind(HdfsTaskLogs.class).in(LazySingleton.class);
    JsonConfigProvider.bind(binder, "robux.hadoop.security.kerberos", HdfsKerberosConfig.class);
    binder.bind(HdfsStorageAuthentication.class).in(ManageLifecycle.class);
    LifecycleModule.register(binder, HdfsStorageAuthentication.class);
    binder.bind(HdfsStorageAvailabilityChecker.class).in(ManageLifecycle.class);
    LifecycleModule.register(binder, HdfsStorageAvailabilityChecker.class);

    JsonConfigProvider.bind(binder, "robux.ingestion.hdfs", HdfsInputSourceConfig.class);
  }
}
