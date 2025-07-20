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

package org.apache.robux.storage.azure;

import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.base.Predicates;
import com.google.inject.Inject;
import org.apache.robux.guice.annotations.Global;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.java.util.common.MapUtils;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.segment.loading.DataSegmentKiller;
import org.apache.robux.segment.loading.SegmentLoadingException;
import org.apache.robux.timeline.DataSegment;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used for removing segment files stored in Azure based deep storage
 */
public class AzureDataSegmentKiller implements DataSegmentKiller
{
  private static final Logger log = new Logger(AzureDataSegmentKiller.class);

  private final AzureDataSegmentConfig segmentConfig;
  private final AzureInputDataConfig inputDataConfig;
  private final AzureAccountConfig accountConfig;
  private final AzureStorage azureStorage;
  private final AzureCloudBlobIterableFactory azureCloudBlobIterableFactory;

  @Inject
  public AzureDataSegmentKiller(
      AzureDataSegmentConfig segmentConfig,
      AzureInputDataConfig inputDataConfig,
      AzureAccountConfig accountConfig,
      @Global final AzureStorage azureStorage,
      AzureCloudBlobIterableFactory azureCloudBlobIterableFactory
  )
  {
    this.segmentConfig = segmentConfig;
    this.inputDataConfig = inputDataConfig;
    this.accountConfig = accountConfig;
    this.azureStorage = azureStorage;
    this.azureCloudBlobIterableFactory = azureCloudBlobIterableFactory;
  }

  @Override
  public void kill(List<DataSegment> segments) throws SegmentLoadingException
  {
    if (segments.isEmpty()) {
      return;
    }
    if (segments.size() == 1) {
      kill(segments.get(0));
      return;
    }

    // create a list of keys to delete
    Map<String, List<String>> containerToKeysToDelete = new HashMap<>();
    for (DataSegment segment : segments) {
      Map<String, Object> loadSpec = segment.getLoadSpec();
      final String containerName = MapUtils.getString(loadSpec, "containerName");
      final String blobPath = MapUtils.getString(loadSpec, "blobPath");
      List<String> keysToDelete = containerToKeysToDelete.computeIfAbsent(
          containerName,
          k -> new ArrayList<>()
      );
      keysToDelete.add(blobPath);
    }

    boolean shouldThrowException = false;
    for (Map.Entry<String, List<String>> containerToKeys : containerToKeysToDelete.entrySet()) {
      boolean batchSuccessful = azureStorage.batchDeleteFiles(
              containerToKeys.getKey(),
              containerToKeys.getValue(),
              null
      );

      if (!batchSuccessful) {
        shouldThrowException = true;
      }
    }

    if (shouldThrowException) {
      throw new SegmentLoadingException(
          "Couldn't delete segments from Azure. See the task logs for more details."
      );
    }
  }


  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    log.info("Killing segment [%s]", segment);

    Map<String, Object> loadSpec = segment.getLoadSpec();
    final String containerName = MapUtils.getString(loadSpec, "containerName");
    final String blobPath = MapUtils.getString(loadSpec, "blobPath");
    final String dirPath = Paths.get(blobPath).getParent().toString();

    try {
      azureStorage.emptyCloudBlobDirectory(containerName, dirPath);
    }
    catch (BlobStorageException e) {
      throw new SegmentLoadingException(e, "Couldn't kill segment[%s]: [%s]", segment.getId(), e.getMessage());
    }
  }

  @Override
  public void killAll() throws IOException
  {
    if (segmentConfig.getContainer() == null || segmentConfig.getPrefix() == null) {
      throw new ISE(
          "Cannot delete all segment files since Azure Deep Storage since robux.azure.container and robux.azure.prefix are not both set.");
    }
    log.info(
        "Deleting all segment files from Azure storage location [bucket: '%s' prefix: '%s']",
        segmentConfig.getContainer(),
        segmentConfig.getPrefix()
    );
    try {
      AzureUtils.deleteObjectsInPath(
          azureStorage,
          inputDataConfig,
          accountConfig,
          azureCloudBlobIterableFactory,
          segmentConfig.getContainer(),
          segmentConfig.getPrefix(),
          Predicates.alwaysTrue()
      );
    }
    catch (Exception e) {
      log.error("Error occurred while deleting segment files from Azure. Error: %s", e.getMessage());
      throw new IOException(e);
    }
  }
}
