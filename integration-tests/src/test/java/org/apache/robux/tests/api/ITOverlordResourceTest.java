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

package org.apache.robux.tests.api;

import com.google.inject.Inject;
import org.apache.robux.indexer.TaskState;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.testing.clients.OverlordResourceTestClient;
import org.apache.robux.testing.clients.TaskResponseObject;
import org.apache.robux.testing.guice.RobuxTestModuleFactory;
import org.apache.robux.testing.utils.ITRetryUtil;
import org.apache.robux.tests.TestNGGroup;
import org.apache.robux.tests.indexer.AbstractIndexerTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

@Test(groups = TestNGGroup.HTTP_ENDPOINT)
@Guice(moduleFactory = RobuxTestModuleFactory.class)
public class ITOverlordResourceTest
{
  private static final String INGESTION_SPEC = "/api/overlord-resource-test-task.json";

  @Inject
  protected OverlordResourceTestClient indexer;

  @Test
  public void testGetAllTasks() throws IOException
  {
    final String taskSpec = AbstractIndexerTest.getResourceAsString(INGESTION_SPEC);
    final String taskId = indexer.submitTask(taskSpec);

    ITRetryUtil.retryUntil(
        () -> {
          final List<TaskResponseObject> tasks = indexer.getAllTasks();
          final TaskResponseObject taskStatus = tasks
              .stream()
              .filter(task -> taskId.equals(task.getId()))
              .findAny()
              .orElseThrow(() -> new ISE("Cannot find task[%s]", taskId));
          TaskState status = taskStatus.getStatus();
          if (status == TaskState.FAILED) {
            throw new ISE("Task[%s] FAILED", taskId);
          }
          return status == TaskState.SUCCESS;
        },
        true,
        ITRetryUtil.DEFAULT_RETRY_SLEEP,
        ITRetryUtil.DEFAULT_RETRY_COUNT,
        taskId
    );
  }
}
