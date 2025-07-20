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

package org.apache.robux.indexing.rabbitstream;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.robux.data.input.impl.ByteEntity;
import org.apache.robux.indexing.overlord.sampler.InputSourceSampler;
import org.apache.robux.indexing.overlord.sampler.SamplerConfig;
import org.apache.robux.indexing.rabbitstream.supervisor.RabbitStreamSupervisorIOConfig;
import org.apache.robux.indexing.rabbitstream.supervisor.RabbitStreamSupervisorSpec;
import org.apache.robux.indexing.rabbitstream.supervisor.RabbitStreamSupervisorTuningConfig;
import org.apache.robux.indexing.seekablestream.SeekableStreamSamplerSpec;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

public class RabbitStreamSamplerSpec extends SeekableStreamSamplerSpec<String, Long, ByteEntity>
{
  private final ObjectMapper objectMapper;

  @JsonCreator
  public RabbitStreamSamplerSpec(
      @JsonProperty("spec") final RabbitStreamSupervisorSpec ingestionSpec,
      @JsonProperty("samplerConfig") @Nullable final SamplerConfig samplerConfig,
      @JacksonInject InputSourceSampler inputSourceSampler,
      @JacksonInject ObjectMapper objectMapper)
  {
    super(ingestionSpec, samplerConfig, inputSourceSampler);
    this.objectMapper = objectMapper;
  }

  @Override
  protected RabbitStreamRecordSupplier createRecordSupplier()
  {
    RabbitStreamSupervisorIOConfig ioConfig = (RabbitStreamSupervisorIOConfig) RabbitStreamSamplerSpec.this.ioConfig;
    final Map<String, Object> props = new HashMap<>(((RabbitStreamSupervisorIOConfig) ioConfig).getConsumerProperties());

    RabbitStreamSupervisorTuningConfig tuningConfig = ((RabbitStreamSupervisorTuningConfig) RabbitStreamSamplerSpec.this.tuningConfig);


    return new RabbitStreamRecordSupplier(
      props,
      objectMapper,
      ioConfig.getUri(),
      tuningConfig.getRecordBufferSizeOrDefault(Runtime.getRuntime().maxMemory()),
      tuningConfig.getRecordBufferOfferTimeout(),
      tuningConfig.getMaxRecordsPerPollOrDefault()
    );
  }
}
