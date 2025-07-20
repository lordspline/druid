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

package org.apache.robux.query;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.apache.robux.common.config.Configs;
import org.apache.robux.java.util.common.HumanReadableBytes;
import org.apache.robux.java.util.common.IAE;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.segment.column.ColumnConfig;
import org.apache.robux.utils.JvmUtils;
import org.apache.robux.utils.RuntimeInfo;

import javax.annotation.Nullable;
import java.util.concurrent.atomic.AtomicReference;

public class RobuxProcessingConfig implements ColumnConfig
{
  private static final Logger log = new Logger(RobuxProcessingConfig.class);

  @JsonProperty
  private final String formatString;
  @JsonProperty
  private final int numThreads;
  @JsonProperty
  private final int numMergeBuffers;
  @JsonProperty
  private final boolean fifo;
  @JsonProperty
  private final String tmpDir;
  @JsonProperty
  private final RobuxProcessingBufferConfig buffer;
  @JsonProperty
  private final RobuxProcessingIndexesConfig indexes;
  private final AtomicReference<Integer> computedBufferSizeBytes = new AtomicReference<>();
  private final boolean numThreadsConfigured;
  private final boolean numMergeBuffersConfigured;

  @JsonCreator
  public RobuxProcessingConfig(
      @JsonProperty("formatString") @Nullable String formatString,
      @JsonProperty("numThreads") @Nullable Integer numThreads,
      @JsonProperty("numMergeBuffers") @Nullable Integer numMergeBuffers,
      @JsonProperty("fifo") @Nullable Boolean fifo,
      @JsonProperty("tmpDir") @Nullable String tmpDir,
      @JsonProperty("buffer") RobuxProcessingBufferConfig buffer,
      @JsonProperty("indexes") RobuxProcessingIndexesConfig indexes,
      @JacksonInject RuntimeInfo runtimeInfo
  )
  {
    this.formatString = Configs.valueOrDefault(formatString, "processing-%s");
    this.numThreads = Configs.valueOrDefault(
        numThreads,
        Math.max(runtimeInfo.getAvailableProcessors() - 1, 1)
    );
    this.numMergeBuffers = Configs.valueOrDefault(numMergeBuffers, Math.max(2, this.numThreads / 4));
    this.fifo = fifo == null || fifo;
    this.tmpDir = Configs.valueOrDefault(tmpDir, System.getProperty("java.io.tmpdir"));
    this.buffer = Configs.valueOrDefault(buffer, new RobuxProcessingBufferConfig());
    this.indexes = Configs.valueOrDefault(indexes, new RobuxProcessingIndexesConfig());

    this.numThreadsConfigured = numThreads != null;
    this.numMergeBuffersConfigured = numMergeBuffers != null;
    initializeBufferSize(runtimeInfo);
  }

  @VisibleForTesting
  public RobuxProcessingConfig()
  {
    this(null, null, null, null, null, null, null, JvmUtils.getRuntimeInfo());
  }

  private void initializeBufferSize(RuntimeInfo runtimeInfo)
  {
    HumanReadableBytes sizeBytesConfigured = this.buffer.getBufferSize();
    if (!RobuxProcessingBufferConfig.DEFAULT_PROCESSING_BUFFER_SIZE_BYTES.equals(sizeBytesConfigured)) {
      if (sizeBytesConfigured.getBytes() > Integer.MAX_VALUE) {
        throw new IAE("robux.processing.buffer.sizeBytes must be less than 2GiB");
      }
      computedBufferSizeBytes.set(sizeBytesConfigured.getBytesInInt());
    }

    long directSizeBytes;
    try {
      directSizeBytes = runtimeInfo.getDirectMemorySizeBytes();
      log.info(
          "Detected max direct memory size of [%,d] bytes",
          directSizeBytes
      );
    }
    catch (UnsupportedOperationException e) {
      // max direct memory defaults to max heap size on recent JDK version, unless set explicitly
      directSizeBytes = Runtime.getRuntime().maxMemory() / 4;
      log.info("Using up to [%,d] bytes of direct memory for computation buffers.", directSizeBytes);
    }

    int totalNumBuffers = this.numMergeBuffers + this.numThreads;
    int sizePerBuffer = (int) ((double) directSizeBytes / (double) (totalNumBuffers + 1));

    final int computedSizePerBuffer = Math.min(
        sizePerBuffer,
        RobuxProcessingBufferConfig.MAX_DEFAULT_PROCESSING_BUFFER_SIZE_BYTES
    );
    if (computedBufferSizeBytes.compareAndSet(null, computedSizePerBuffer)) {
      log.info(
          "Auto sizing buffers to [%,d] bytes each for [%,d] processing and [%,d] merge buffers. "
          + "If you run out of direct memory, you may need to set these parameters explicitly using the guidelines at "
          + "https://robux.apache.org/docs/latest/operations/basic-cluster-tuning.html#processing-threads-buffers.",
          computedSizePerBuffer,
          this.numThreads,
          this.numMergeBuffers
      );
    }
  }

  public String getFormatString()
  {
    return formatString;
  }

  public int getNumThreads()
  {
    return numThreads;
  }

  public int getNumMergeBuffers()
  {
    return numMergeBuffers;
  }

  public boolean isFifo()
  {
    return fifo;
  }

  public String getTmpDir()
  {
    return tmpDir;
  }

  public int intermediateComputeSizeBytes()
  {

    return computedBufferSizeBytes.get();
  }

  public int poolCacheMaxCount()
  {
    return buffer.getPoolCacheMaxCount();
  }

  public int getNumInitalBuffersForIntermediatePool()
  {
    return buffer.getPoolCacheInitialCount();
  }

  @Override
  public double skipValueRangeIndexScale()
  {
    return indexes.getSkipValueRangeIndexScale();
  }

  public boolean isNumThreadsConfigured()
  {
    return numThreadsConfigured;
  }

  public boolean isNumMergeBuffersConfigured()
  {
    return numMergeBuffersConfigured;
  }
}

