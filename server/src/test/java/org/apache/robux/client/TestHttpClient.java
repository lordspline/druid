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

package org.apache.robux.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.java.util.common.NonnullPair;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.guava.Sequence;
import org.apache.robux.java.util.http.client.HttpClient;
import org.apache.robux.java.util.http.client.Request;
import org.apache.robux.java.util.http.client.response.ClientResponse;
import org.apache.robux.java.util.http.client.response.HttpResponseHandler;
import org.apache.robux.java.util.http.client.response.HttpResponseHandler.TrafficCop;
import org.apache.robux.query.Query;
import org.apache.robux.query.QueryPlus;
import org.apache.robux.query.QueryRunner;
import org.apache.robux.query.QueryRunnerFactoryConglomerate;
import org.apache.robux.query.ReportTimelineMissingSegmentQueryRunner;
import org.apache.robux.query.SegmentDescriptor;
import org.apache.robux.query.context.ResponseContext;
import org.apache.robux.segment.QueryableIndex;
import org.apache.robux.server.QueryResource;
import org.apache.robux.timeline.DataSegment;
import org.jboss.netty.buffer.HeapChannelBufferFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * An HTTP client for testing which emulates querying data nodes (historicals or realtime tasks).
 */
public class TestHttpClient implements HttpClient
{
  private static final TrafficCop NOOP_TRAFFIC_COP = checkNum -> 0L;
  private static final int RESPONSE_CTX_HEADER_LEN_LIMIT = 7 * 1024;

  private final Map<URL, SimpleServerManager> servers = new HashMap<>();
  private final ObjectMapper objectMapper;

  public TestHttpClient(ObjectMapper objectMapper)
  {
    this.objectMapper = objectMapper;
  }

  public void addServerAndRunner(RobuxServer server, SimpleServerManager serverManager)
  {
    servers.put(computeUrl(server), serverManager);
  }

  @Nullable
  public SimpleServerManager getServerManager(RobuxServer server)
  {
    return servers.get(computeUrl(server));
  }

  public Map<URL, SimpleServerManager> getServers()
  {
    return servers;
  }

  private static URL computeUrl(RobuxServer server)
  {
    try {
      return new URL(StringUtils.format("%s://%s/robux/v2/", server.getScheme(), server.getHost()));
    }
    catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
      Request request,
      HttpResponseHandler<Intermediate, Final> handler
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(
      Request request,
      HttpResponseHandler<Intermediate, Final> handler,
      Duration readTimeout
  )
  {
    try {
      final Query query = objectMapper.readValue(request.getContent().array(), Query.class);
      final QueryRunner queryRunner = servers.get(request.getUrl()).getQueryRunner();
      if (queryRunner == null) {
        throw new ISE("Can't find queryRunner for url[%s]", request.getUrl());
      }
      final ResponseContext responseContext = ResponseContext.createEmpty();
      final Sequence sequence = queryRunner.run(QueryPlus.wrap(query), responseContext);
      final byte[] serializedContent;
      try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
        objectMapper.writeValue(baos, sequence);
        serializedContent = baos.toByteArray();
      }
      final ResponseContext.SerializationResult serializationResult = responseContext.serializeWith(
          objectMapper,
          RESPONSE_CTX_HEADER_LEN_LIMIT
      );
      final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      response.headers().add(QueryResource.HEADER_RESPONSE_CONTEXT, serializationResult.getResult());
      response.setContent(
          HeapChannelBufferFactory.getInstance().getBuffer(serializedContent, 0, serializedContent.length)
      );
      final ClientResponse<Intermediate> intermClientResponse = handler.handleResponse(response, NOOP_TRAFFIC_COP);
      final ClientResponse<Final> finalClientResponse = handler.done(intermClientResponse);
      return Futures.immediateFuture(finalClientResponse.getObj());
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * A simple server manager for testing which you can manually drop a segment. Currently used for
   * testing {@link org.apache.robux.query.RetryQueryRunner}.
   */
  public static class SimpleServerManager
  {
    private final QueryRunnerFactoryConglomerate conglomerate;
    private final DataSegment segment;
    private final QueryableIndex queryableIndex;
    private final boolean throwQueryError;

    private boolean isSegmentDropped = false;

    public SimpleServerManager(
        QueryRunnerFactoryConglomerate conglomerate,
        DataSegment segment,
        QueryableIndex queryableIndex,
        boolean throwQueryError
    )
    {
      this.conglomerate = conglomerate;
      this.segment = segment;
      this.queryableIndex = queryableIndex;
      this.throwQueryError = throwQueryError;
    }

    private QueryRunner getQueryRunner()
    {
      if (throwQueryError) {
        return (queryPlus, responseContext) -> {
          throw new RuntimeException("Exception for testing");
        };
      }
      if (isSegmentDropped) {
        return new ReportTimelineMissingSegmentQueryRunner<>(
            new SegmentDescriptor(segment.getInterval(), segment.getVersion(), segment.getId().getPartitionNum())
        );
      } else {
        return new SimpleQueryRunner(conglomerate, segment.getId(), queryableIndex);
      }
    }

    public NonnullPair<DataSegment, QueryableIndex> dropSegment()
    {
      this.isSegmentDropped = true;
      return new NonnullPair<>(segment, queryableIndex);
    }
  }
}
