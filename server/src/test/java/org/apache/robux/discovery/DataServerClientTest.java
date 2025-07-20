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

package org.apache.robux.discovery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.robux.java.util.common.Intervals;
import org.apache.robux.java.util.common.guava.Sequence;
import org.apache.robux.java.util.common.io.Closer;
import org.apache.robux.query.QueryTimeoutException;
import org.apache.robux.query.SegmentDescriptor;
import org.apache.robux.query.context.DefaultResponseContext;
import org.apache.robux.query.context.ResponseContext;
import org.apache.robux.query.scan.ScanQuery;
import org.apache.robux.query.scan.ScanResultValue;
import org.apache.robux.query.spec.MultipleSpecificSegmentSpec;
import org.apache.robux.rpc.MockServiceClient;
import org.apache.robux.rpc.RequestBuilder;
import org.apache.robux.rpc.ServiceClientFactory;
import org.apache.robux.rpc.ServiceLocation;
import org.apache.robux.server.QueryResource;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.apache.robux.query.Robuxs.newScanQueryBuilder;
import static org.mockito.Mockito.mock;

public class DataServerClientTest
{
  private static final SegmentDescriptor SEGMENT_1 = new SegmentDescriptor(Intervals.of("2003/2004"), "v0", 1);
  private MockServiceClient serviceClient;
  private ObjectMapper jsonMapper;
  private ScanQuery query;
  private DataServerClient target;

  @Before
  public void setUp()
  {
    jsonMapper = RobuxServiceTestUtils.newJsonMapper();
    serviceClient = new MockServiceClient();
    ServiceClientFactory serviceClientFactory = (serviceName, serviceLocator, retryPolicy) -> serviceClient;

    query = newScanQueryBuilder()
      .dataSource("dataSource1")
      .intervals(new MultipleSpecificSegmentSpec(ImmutableList.of(SEGMENT_1)))
      .columns("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1")
      .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
      .context(ImmutableMap.of("defaultTimeout", 5000L))
      .build();

    target = new DataServerClient(
        serviceClientFactory,
        mock(ServiceLocation.class),
        jsonMapper
    );
  }

  @Test
  public void testFetchSegmentFromDataServer() throws JsonProcessingException, ExecutionException, InterruptedException
  {
    ScanResultValue scanResultValue = new ScanResultValue(
        null,
        ImmutableList.of("id", "name"),
        ImmutableList.of(
            ImmutableList.of(1, "abc"),
            ImmutableList.of(5, "efg")
        ));

    RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.POST, "/robux/v2/")
        .jsonContent(jsonMapper, query);
    serviceClient.expectAndRespond(
        requestBuilder,
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(Collections.singletonList(scanResultValue))
    );

    ResponseContext responseContext = DefaultResponseContext.createEmpty();
    Sequence<ScanResultValue> result = target.run(
        query,
        responseContext,
        jsonMapper.getTypeFactory().constructType(ScanResultValue.class),
        Closer.create()
    ).get();

    Assert.assertEquals(ImmutableList.of(scanResultValue), result.toList());
  }

  @Test
  public void testMissingSegmentsHeaderShouldAccumulate() throws JsonProcessingException
  {
    DataServerResponse dataServerResponse = new DataServerResponse(ImmutableList.of(SEGMENT_1));
    RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.POST, "/robux/v2/")
        .jsonContent(jsonMapper, query);
    serviceClient.expectAndRespond(
        requestBuilder,
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON, QueryResource.HEADER_RESPONSE_CONTEXT, jsonMapper.writeValueAsString(dataServerResponse)),
        jsonMapper.writeValueAsBytes(null)
    );

    ResponseContext responseContext = new DefaultResponseContext();
    target.run(
        query,
        responseContext,
        jsonMapper.getTypeFactory().constructType(ScanResultValue.class),
        Closer.create()
    );

    Assert.assertEquals(1, responseContext.getMissingSegments().size());
  }

  @Test
  public void testQueryFailure() throws JsonProcessingException
  {
    ScanQuery scanQueryWithTimeout = query.withOverriddenContext(ImmutableMap.of("maxQueuedBytes", 1, "timeout", 0));
    ScanResultValue scanResultValue = new ScanResultValue(
        null,
        ImmutableList.of("id", "name"),
        ImmutableList.of(
            ImmutableList.of(1, "abc"),
            ImmutableList.of(5, "efg")
        ));

    RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.POST, "/robux/v2/")
        .jsonContent(jsonMapper, scanQueryWithTimeout);
    serviceClient.expectAndRespond(
        requestBuilder,
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(Collections.singletonList(scanResultValue))
    );

    ResponseContext responseContext = new DefaultResponseContext();
    Assert.assertThrows(
        QueryTimeoutException.class,
        () -> target.run(
            scanQueryWithTimeout,
            responseContext,
            jsonMapper.getTypeFactory().constructType(ScanResultValue.class),
            Closer.create()
        ).get().toList()
    );
  }

  private static class DataServerResponse
  {
    List<SegmentDescriptor> missingSegments;

    @JsonCreator
    public DataServerResponse(@JsonProperty("missingSegments") List<SegmentDescriptor> missingSegments)
    {
      this.missingSegments = missingSegments;
    }

    @JsonProperty("missingSegments")
    public List<SegmentDescriptor> getMissingSegments()
    {
      return missingSegments;
    }
  }
}
