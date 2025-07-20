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

package org.apache.robux.testing.embedded.minio;

import org.apache.robux.testing.embedded.EmbeddedRobuxCluster;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;

public class MinIOStorageResourceTest
{
  @Test
  @Timeout(120)
  public void testMinIOContainerLifecycle()
  {
    final MinIOStorageResource resource = new MinIOStorageResource("test-bucket", "test/base");
    final EmbeddedRobuxCluster cluster = Mockito.mock(EmbeddedRobuxCluster.class);

    resource.start();
    assertTrue(resource.isRunning());

    resource.onStarted(cluster);

    // Verify container properties
    assertEquals("test-bucket", resource.getBucket());
    assertEquals("test/base", resource.getBaseKey());
    assertEquals("minioadmin", resource.getAccessKey());
    assertEquals("minioadmin", resource.getSecretKey());

    // Verify all the required properties are set
    verify(cluster).addCommonProperty("robux.storage.type", "s3");
    verify(cluster).addCommonProperty("robux.indexer.logs.type", "s3");
    verify(cluster).addCommonProperty("robux.s3.enablePathStyleAccess", "true");
    verify(cluster).addCommonProperty("robux.s3.protocol", "http");
    verify(cluster).addCommonProperty("robux.s3.accessKey", "minioadmin");
    verify(cluster).addCommonProperty("robux.s3.secretKey", "minioadmin");
    verify(cluster).addCommonProperty("robux.storage.bucket", "test-bucket");
    verify(cluster).addCommonProperty("robux.storage.baseKey", "test/base");
    verify(cluster).addCommonProperty("robux.indexer.logs.s3Bucket", "test-bucket");
    verify(cluster).addCommonProperty("robux.indexer.logs.s3Prefix", "robux/indexing-logs");

    // Verify endpoint URL is set
    verify(cluster).addCommonProperty(
        ArgumentMatchers.eq("robux.s3.endpoint.url"),
        ArgumentMatchers.argThat(url -> url.startsWith("http://"))
    );

    resource.stop();
    assertFalse(resource.isRunning());
  }
}
