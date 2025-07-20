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

package org.apache.robux.k8s.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.robux.guice.annotations.EscalatedGlobal;
import org.apache.robux.guice.annotations.Smile;
import org.apache.robux.indexing.overlord.TaskRunnerFactory;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.java.util.http.client.HttpClient;
import org.apache.robux.k8s.overlord.common.RobuxKubernetesClient;
import org.apache.robux.k8s.overlord.common.KubernetesPeonClient;
import org.apache.robux.k8s.overlord.taskadapter.PodTemplateTaskAdapter;
import org.apache.robux.k8s.overlord.taskadapter.TaskAdapter;
import org.apache.robux.tasklogs.TaskLogs;

import java.util.Set;

public class KubernetesTaskRunnerFactory implements TaskRunnerFactory<KubernetesTaskRunner>
{
  public static final String TYPE_NAME = "k8s";
  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final KubernetesTaskRunnerConfig kubernetesTaskRunnerConfig;
  private final TaskLogs taskLogs;
  private final RobuxKubernetesClient robuxKubernetesClient;
  private final ServiceEmitter emitter;
  private KubernetesTaskRunner runner;
  private final TaskAdapter taskAdapter;
  private final Set<String> adapterTypeAllowingTasksInDifferentNamespaces = Set.of(PodTemplateTaskAdapter.TYPE);

  @Inject
  public KubernetesTaskRunnerFactory(
      @Smile ObjectMapper smileMapper,
      @EscalatedGlobal final HttpClient httpClient,
      KubernetesTaskRunnerConfig kubernetesTaskRunnerConfig,
      TaskLogs taskLogs,
      RobuxKubernetesClient robuxKubernetesClient,
      ServiceEmitter emitter,
      TaskAdapter taskAdapter
  )
  {
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.kubernetesTaskRunnerConfig = kubernetesTaskRunnerConfig;
    this.taskLogs = taskLogs;
    this.robuxKubernetesClient = robuxKubernetesClient;
    this.emitter = emitter;
    this.taskAdapter = taskAdapter;
  }

  @Override
  public KubernetesTaskRunner build()
  {
    KubernetesPeonClient peonClient;
    if (adapterTypeAllowingTasksInDifferentNamespaces.contains(taskAdapter.getAdapterType())) {
      peonClient = new KubernetesPeonClient(
          robuxKubernetesClient,
          kubernetesTaskRunnerConfig.getNamespace(),
          kubernetesTaskRunnerConfig.getOverlordNamespace(),
          kubernetesTaskRunnerConfig.isDebugJobs(),
          emitter
      );
    } else {
      peonClient = new KubernetesPeonClient(
          robuxKubernetesClient,
          kubernetesTaskRunnerConfig.getNamespace(),
          kubernetesTaskRunnerConfig.isDebugJobs(),
          emitter
      );
    }

    runner = new KubernetesTaskRunner(
        taskAdapter,
        kubernetesTaskRunnerConfig,
        peonClient,
        httpClient,
        new KubernetesPeonLifecycleFactory(peonClient, taskLogs, smileMapper),
        emitter
    );
    return runner;
  }

  @Override
  public KubernetesTaskRunner get()
  {
    return runner;
  }
}
