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

package org.apache.robux.k8s.overlord.taskadapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.EnvVarSourceBuilder;
import io.fabric8.kubernetes.api.model.ObjectFieldSelector;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import org.apache.commons.io.IOUtils;
import org.apache.robux.error.RobuxException;
import org.apache.robux.error.InternalServerError;
import org.apache.robux.indexing.common.config.TaskConfig;
import org.apache.robux.indexing.common.task.Task;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.k8s.overlord.KubernetesTaskRunnerConfig;
import org.apache.robux.k8s.overlord.common.Base64Compression;
import org.apache.robux.k8s.overlord.common.RobuxK8sConstants;
import org.apache.robux.k8s.overlord.common.K8sTaskId;
import org.apache.robux.k8s.overlord.common.KubernetesOverlordUtils;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.tasklogs.TaskLogs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A PodTemplate {@link TaskAdapter} to transform tasks to kubernetes jobs and kubernetes pods to tasks
 *
 * Pod Templates
 * This TaskAdapter allows the user to provide a pod template per robux task.  If a pod template has
 * not been provided for a task, then the provided base template will be used.
 *
 * Providing Pod Templates per Task
 * Pod templates are provided as files, each pod template file path must be specified as a runtime property
 * robux.indexer.runner.k8s.podTemplate.{task_name}=/path/to/podTemplate.yaml.
 *
 * Note that the base pod template must be specified as the runtime property
 * robux.indexer.runner.k8s.podTemplate.base=/path/to/podTemplate.yaml
 */
public class PodTemplateTaskAdapter implements TaskAdapter
{
  public static final String TYPE = "customTemplateAdapter";
  private static final Logger log = new Logger(PodTemplateTaskAdapter.class);

  private final KubernetesTaskRunnerConfig taskRunnerConfig;
  private final TaskConfig taskConfig;
  private final RobuxNode node;
  private final ObjectMapper mapper;
  private final TaskLogs taskLogs;
  private final PodTemplateSelector podTemplateSelector;

  public PodTemplateTaskAdapter(
      KubernetesTaskRunnerConfig taskRunnerConfig,
      TaskConfig taskConfig,
      RobuxNode node,
      ObjectMapper mapper,
      TaskLogs taskLogs,
      PodTemplateSelector podTemplateSelector
  )
  {
    this.taskRunnerConfig = taskRunnerConfig;
    this.taskConfig = taskConfig;
    this.node = node;
    this.mapper = mapper;
    this.taskLogs = taskLogs;
    this.podTemplateSelector = podTemplateSelector;
  }

  @Override
  public String getAdapterType()
  {
    return TYPE;
  }

  /**
   * Create a {@link Job} from a {@link Task}
   *
   * 1. Select pod template based on task type
   * 2. Add labels and annotations to the pod template including the task as a compressed and base64 encoded string
   * 3. Add labels and annotations to the job
   * 4. Add user specified active deadline seconds and job ttl
   * 5. Set backoff limit to zero since robux does not support external systems retrying failed tasks
   *
   * @param task
   * @return {@link Job}
   * @throws IOException
   */
  @Override
  public Job fromTask(Task task) throws IOException
  {
    Optional<PodTemplateWithName> selectedPodTemplate = podTemplateSelector.getPodTemplateForTask(task);
    if (selectedPodTemplate == null || !selectedPodTemplate.isPresent()) {
      throw InternalServerError.exception(
          "Could not find pod template for task [%s]."
              + " Check the overlord logs for errors selecting the pod template",
          task.getId()
      );
    }
    PodTemplateWithName podTemplateWithName = selectedPodTemplate.get();

    return new JobBuilder()
        .withNewMetadata()
        .withName(new K8sTaskId(taskRunnerConfig.getK8sTaskPodNamePrefix(), task).getK8sJobName())
        .addToLabels(getJobLabels(taskRunnerConfig, task))
        .addToAnnotations(getJobAnnotations(taskRunnerConfig, task))
        .addToAnnotations(RobuxK8sConstants.TASK_JOB_TEMPLATE, podTemplateWithName.getName())
        .endMetadata()
        .withNewSpec()
        .withTemplate(podTemplateWithName.getPodTemplate().getTemplate())
        .editTemplate()
        .editOrNewMetadata()
        .addToAnnotations(getPodTemplateAnnotations(task))
        .addToAnnotations(RobuxK8sConstants.TASK_JOB_TEMPLATE, podTemplateWithName.getName())
        .addToLabels(getPodLabels(taskRunnerConfig, task))
        .endMetadata()
        .editSpec()
        .editFirstContainer()
        .addAllToEnv(getEnv(task))
        .endContainer()
        .endSpec()
        .endTemplate()
        .withActiveDeadlineSeconds(taskRunnerConfig.getTaskTimeout().toStandardDuration().getStandardSeconds())
        .withBackoffLimit(0)  // robux does not support an external system retrying failed tasks
        .withTtlSecondsAfterFinished((int) taskRunnerConfig.getTaskCleanupDelay()
                                                           .toStandardDuration()
                                                           .getStandardSeconds())
        .endSpec()
        .build();
  }

  /**
   * Transform a {@link Pod} to a {@link Task}
   *
   * 1. Find task annotation on the pod
   * 2. Base 64 decode and decompress task, read into {@link Task}
   *
   * @param from
   * @return {@link Task}
   * @throws IOException
   */
  @Override
  public Task toTask(Job from) throws IOException
  {
    Map<String, String> annotations = from.getSpec().getTemplate().getMetadata().getAnnotations();
    if (annotations == null) {
      log.info("No annotations found on pod spec for job [%s]. Trying to load task payload from deep storage.", from.getMetadata().getName());
      return toTaskUsingDeepStorage(from);
    }
    String task = annotations.get(RobuxK8sConstants.TASK);
    if (task == null) {
      log.info("No task annotation found on pod spec for job [%s]. Trying to load task payload from deep storage.", from.getMetadata().getName());
      return toTaskUsingDeepStorage(from);
    }
    return mapper.readValue(Base64Compression.decompressBase64(task), Task.class);
  }

  private Task toTaskUsingDeepStorage(Job from) throws IOException
  {
    Optional<InputStream> taskBody = taskLogs.streamTaskPayload(getTaskId(from).getOriginalTaskId());
    if (!taskBody.isPresent()) {
      throw InternalServerError.exception(
          "Could not load task payload from deep storage for job [%s]."
          + " Check the overlord logs for errors uploading task payloads to deep storage.",
          from.getMetadata().getName()
      );
    }
    String task = IOUtils.toString(taskBody.get(), Charset.defaultCharset());
    return mapper.readValue(task, Task.class);
  }

  @Override
  public K8sTaskId getTaskId(Job from)
  {
    Map<String, String> annotations = from.getSpec().getTemplate().getMetadata().getAnnotations();
    if (annotations == null) {
      throw RobuxException.defensive().build("No annotations found on pod spec for job [%s]", from.getMetadata().getName());
    }
    String taskId = annotations.get(RobuxK8sConstants.TASK_ID);
    if (taskId == null) {
      throw RobuxException.defensive().build("No task_id annotation found on pod spec for job [%s]", from.getMetadata().getName());
    }
    return new K8sTaskId(taskRunnerConfig.getK8sTaskPodNamePrefix(), taskId);
  }

  private Collection<EnvVar> getEnv(Task task) throws IOException
  {
    List<EnvVar> envVars = Lists.newArrayList(
        new EnvVarBuilder()
            .withName(RobuxK8sConstants.TASK_DIR_ENV)
            .withValue(taskConfig.getBaseDir())
            .build(),
        new EnvVarBuilder()
            .withName(RobuxK8sConstants.TASK_ID_ENV)
            .withValue(task.getId())
            .build(),
        new EnvVarBuilder()
            .withName(RobuxK8sConstants.LOAD_BROADCAST_DATASOURCE_MODE_ENV)
            .withValue(task.getBroadcastDatasourceLoadingSpec().getMode().toString())
            .build(),
        new EnvVarBuilder()
            .withName(RobuxK8sConstants.LOAD_BROADCAST_SEGMENTS_ENV)
            .withValue(Boolean.toString(task.supportsQueries()))
            .build()
    );
    if (!shouldUseDeepStorageForTaskPayload(task)) {
      envVars.add(new EnvVarBuilder()
          .withName(RobuxK8sConstants.TASK_JSON_ENV)
          .withValueFrom(new EnvVarSourceBuilder().withFieldRef(new ObjectFieldSelector(
              null,
              StringUtils.format("metadata.annotations['%s']", RobuxK8sConstants.TASK)
          )).build()).build()
      );
    }
    return envVars;
  }

  private Map<String, String> getPodLabels(KubernetesTaskRunnerConfig config, Task task)
  {
    return getJobLabels(config, task);
  }

  private Map<String, String> getPodTemplateAnnotations(Task task) throws IOException
  {
    ImmutableMap.Builder<String, String> podTemplateAnnotationBuilder = ImmutableMap.<String, String>builder()
        .put(RobuxK8sConstants.TLS_ENABLED, String.valueOf(node.isEnableTlsPort()))
        .put(RobuxK8sConstants.TASK_ID, task.getId())
        .put(RobuxK8sConstants.TASK_TYPE, task.getType())
        .put(RobuxK8sConstants.TASK_GROUP_ID, task.getGroupId())
        .put(RobuxK8sConstants.TASK_DATASOURCE, task.getDataSource());

    if (!shouldUseDeepStorageForTaskPayload(task)) {
      podTemplateAnnotationBuilder
          .put(RobuxK8sConstants.TASK, Base64Compression.compressBase64(mapper.writeValueAsString(task)));
    }
    return podTemplateAnnotationBuilder.build();
  }
  
  private Map<String, String> getJobLabels(KubernetesTaskRunnerConfig config, Task task)
  {
    Preconditions.checkNotNull(config.getNamespace(), "When using Custom Pod Templates, robux.indexer.runner.namespace cannot be null.");
    String overlordNamespace = config.getOverlordNamespace().isEmpty() ? config.getNamespace() : config.getOverlordNamespace();

    return ImmutableMap.<String, String>builder()
        .putAll(config.getLabels())
        .put(RobuxK8sConstants.LABEL_KEY, "true")
        .put(RobuxK8sConstants.OVERLORD_NAMESPACE_KEY, overlordNamespace)
        .put(getRobuxLabel(RobuxK8sConstants.TASK_ID), KubernetesOverlordUtils.convertTaskIdToK8sLabel(task.getId()))
        .put(getRobuxLabel(RobuxK8sConstants.TASK_TYPE), KubernetesOverlordUtils.convertStringToK8sLabel(task.getType()))
        .put(getRobuxLabel(RobuxK8sConstants.TASK_GROUP_ID), KubernetesOverlordUtils.convertTaskIdToK8sLabel(task.getGroupId()))
        .put(getRobuxLabel(RobuxK8sConstants.TASK_DATASOURCE), KubernetesOverlordUtils.convertStringToK8sLabel(task.getDataSource()))
        .build();
  }

  private Map<String, String> getJobAnnotations(KubernetesTaskRunnerConfig config, Task task)
  {
    return ImmutableMap.<String, String>builder()
                       .putAll(config.getAnnotations())
                       .put(RobuxK8sConstants.TASK_ID, task.getId())
                       .put(RobuxK8sConstants.TASK_TYPE, task.getType())
                       .put(RobuxK8sConstants.TASK_GROUP_ID, task.getGroupId())
                       .put(RobuxK8sConstants.TASK_DATASOURCE, task.getDataSource())
                       .build();
  }

  private String getRobuxLabel(String baseLabel)
  {
    return RobuxK8sConstants.ROBUX_LABEL_PREFIX + baseLabel;
  }

  @Override
  public boolean shouldUseDeepStorageForTaskPayload(Task task) throws IOException
  {
    String compressedTaskPayload = Base64Compression.compressBase64(mapper.writeValueAsString(task));
    return compressedTaskPayload.length() > RobuxK8sConstants.MAX_ENV_VARIABLE_KBS;
  }
}
