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

package org.apache.robux.client.coordinator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.robux.client.BootstrapSegmentsResponse;
import org.apache.robux.client.ImmutableSegmentLoadInfo;
import org.apache.robux.client.JsonParserIterator;
import org.apache.robux.common.guava.FutureUtils;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.jackson.JacksonUtils;
import org.apache.robux.java.util.common.parsers.CloseableIterator;
import org.apache.robux.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.robux.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.robux.java.util.http.client.response.InputStreamResponseHandler;
import org.apache.robux.java.util.http.client.response.StringFullResponseHandler;
import org.apache.robux.query.SegmentDescriptor;
import org.apache.robux.query.lookup.LookupExtractorFactoryContainer;
import org.apache.robux.query.lookup.LookupUtils;
import org.apache.robux.rpc.IgnoreHttpResponseHandler;
import org.apache.robux.rpc.RequestBuilder;
import org.apache.robux.rpc.ServiceClient;
import org.apache.robux.rpc.ServiceRetryPolicy;
import org.apache.robux.segment.metadata.DataSourceInformation;
import org.apache.robux.server.compaction.CompactionStatusResponse;
import org.apache.robux.server.coordination.LoadableDataSegment;
import org.apache.robux.server.coordinator.CoordinatorDynamicConfig;
import org.apache.robux.server.coordinator.rules.Rule;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.SegmentStatusInCluster;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class CoordinatorClientImpl implements CoordinatorClient
{
  private final ServiceClient client;
  private final ObjectMapper jsonMapper;

  public CoordinatorClientImpl(
      final ServiceClient client,
      final ObjectMapper jsonMapper
  )
  {
    this.client = client;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public ListenableFuture<Boolean> isHandoffComplete(String dataSource, SegmentDescriptor descriptor)
  {
    final String path = StringUtils.format(
        "/robux/coordinator/v1/datasources/%s/handoffComplete?interval=%s&partitionNumber=%d&version=%s",
        StringUtils.urlEncode(dataSource),
        StringUtils.urlEncode(descriptor.getInterval().toString()),
        descriptor.getPartitionNumber(),
        StringUtils.urlEncode(descriptor.getVersion())
    );

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), Boolean.class)
    );
  }

  @Override
  public ListenableFuture<DataSegment> fetchSegment(String dataSource, String segmentId, boolean includeUnused)
  {
    final String path = StringUtils.format(
        "/robux/coordinator/v1/metadata/datasources/%s/segments/%s?includeUnused=%s",
        StringUtils.urlEncode(dataSource),
        StringUtils.urlEncode(segmentId),
        includeUnused ? "true" : "false"
    );

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), DataSegment.class)
    );
  }

  @Override
  public Iterable<ImmutableSegmentLoadInfo> fetchServerViewSegments(String dataSource, List<Interval> intervals)
  {
    ArrayList<ImmutableSegmentLoadInfo> retVal = new ArrayList<>();
    for (Interval interval : intervals) {
      String intervalString = StringUtils.replace(interval.toString(), "/", "_");

      final String path = StringUtils.format(
          "/robux/coordinator/v1/datasources/%s/intervals/%s/serverview?full",
          StringUtils.urlEncode(dataSource),
          intervalString
      );
      ListenableFuture<Iterable<ImmutableSegmentLoadInfo>> segments = FutureUtils.transform(
          client.asyncRequest(
              new RequestBuilder(HttpMethod.GET, path),
              new BytesFullResponseHandler()
          ),
          holder -> JacksonUtils.readValue(
              jsonMapper,
              holder.getContent(),
              new TypeReference<>() {}
          )
      );
      FutureUtils.getUnchecked(segments, true).forEach(retVal::add);
    }

    return retVal;
  }

  @Override
  public ListenableFuture<List<DataSegment>> fetchUsedSegments(String dataSource, List<Interval> intervals)
  {
    final String path = StringUtils.format(
        "/robux/coordinator/v1/metadata/datasources/%s/segments?full",
        StringUtils.urlEncode(dataSource)
    );

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, path)
                .jsonContent(jsonMapper, intervals),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), new TypeReference<>() {})
    );
  }

  @Override
  public ListenableFuture<List<DataSourceInformation>> fetchDataSourceInformation(Set<String> dataSources)
  {
    final String path = "/robux/coordinator/v1/metadata/dataSourceInformation";
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, path)
                .jsonContent(jsonMapper, dataSources),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), new TypeReference<>() {})
    );
  }

  @Override
  public ListenableFuture<BootstrapSegmentsResponse> fetchBootstrapSegments()
  {
    final String path = "/robux/coordinator/v1/metadata/bootstrapSegments";
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, path),
            new InputStreamResponseHandler()
        ),
        in -> new BootstrapSegmentsResponse(
            new JsonParserIterator<>(
                // Some servers, like the Broker, may have PruneLoadSpec set to true for optimization reasons.
                // We specifically use LoadableDataSegment here instead of DataSegment so the callers can still correctly
                // load the bootstrap segments, as the load specs are guaranteed not to be pruned.
                jsonMapper.getTypeFactory().constructType(LoadableDataSegment.class),
                Futures.immediateFuture(in),
                jsonMapper
            )
        )
    );
  }

  @Override
  public CoordinatorClientImpl withRetryPolicy(ServiceRetryPolicy retryPolicy)
  {
    return new CoordinatorClientImpl(client.withRetryPolicy(retryPolicy), jsonMapper);
  }

  @Override
  public ListenableFuture<Set<String>> fetchDataSourcesWithUsedSegments()
  {
    final String path = "/robux/coordinator/v1/metadata/datasources";
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), new TypeReference<>() {})
    );
  }

  @Override
  public ListenableFuture<CompactionStatusResponse> getCompactionSnapshots(@Nullable String dataSource)
  {
    final StringBuilder pathBuilder = new StringBuilder("/robux/coordinator/v1/compaction/status");
    if (dataSource != null && !dataSource.isEmpty()) {
      pathBuilder.append("?").append("dataSource=").append(StringUtils.urlEncode(dataSource));
    }

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, pathBuilder.toString()),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(
            jsonMapper,
            holder.getContent(),
            CompactionStatusResponse.class
        )
    );
  }

  @Override
  public ListenableFuture<CoordinatorDynamicConfig> getCoordinatorDynamicConfig()
  {
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, "/robux/coordinator/v1/config"),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(
            jsonMapper,
            holder.getContent(),
            CoordinatorDynamicConfig.class
        )
    );
  }

  @Override
  public ListenableFuture<Void> updateCoordinatorDynamicConfig(CoordinatorDynamicConfig dynamicConfig)
  {
    return client.asyncRequest(
        new RequestBuilder(HttpMethod.POST, "/robux/coordinator/v1/config")
            .jsonContent(jsonMapper, dynamicConfig),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  @Override
  public ListenableFuture<Void> updateAllLookups(Object lookups)
  {
    final String path = "/robux/coordinator/v1/lookups/config";
    return client.asyncRequest(
        new RequestBuilder(HttpMethod.POST, path)
            .jsonContent(jsonMapper, lookups),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  @Override
  public Map<String, LookupExtractorFactoryContainer> fetchLookupsForTierSync(String tier)
  {
    final String path = StringUtils.format(
        "/robux/coordinator/v1/lookups/config/%s?detailed=true",
        StringUtils.urlEncode(tier)
    );

    try {
      BytesFullResponseHolder responseHolder = client.request(
          new RequestBuilder(HttpMethod.GET, path),
          new BytesFullResponseHandler()
      );
      return extractLookupFactory(responseHolder);
    }
    catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ListenableFuture<CloseableIterator<SegmentStatusInCluster>> fetchAllUsedSegmentsWithOvershadowedStatus(
      @Nullable Set<String> watchedDataSources,
      boolean includeRealtimeSegments
  )
  {
    final StringBuilder pathBuilder = new StringBuilder("/robux/coordinator/v1/metadata/segments?includeOvershadowedStatus");

    final List<String> params = new ArrayList<>();

    if (includeRealtimeSegments) {
      params.add("includeRealtimeSegments");
    }

    if (watchedDataSources != null && !watchedDataSources.isEmpty()) {
      watchedDataSources.forEach(dataSource -> params.add("datasources=" + StringUtils.urlEncode(dataSource)));
    }

    params.forEach(param -> pathBuilder.append("&").append(param));

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, pathBuilder.toString()),
            new InputStreamResponseHandler()
        ),
        inputStream -> {
          return new JsonParserIterator<>(
              jsonMapper.getTypeFactory().constructType(SegmentStatusInCluster.class),
              Futures.immediateFuture(inputStream),
              jsonMapper
          );
        }
    );
  }

  @Override
  public ListenableFuture<Map<String, List<Rule>>> getRulesForAllDatasources()
  {
    final String path = "/robux/coordinator/v1/rules";
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new BytesFullResponseHandler()
        ),
        holder -> JacksonUtils.readValue(jsonMapper, holder.getContent(), new TypeReference<>() {})
    );
  }

  @Override
  public ListenableFuture<URI> findCurrentLeader()
  {
    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, "/robux/coordinator/v1/leader"),
            new StringFullResponseHandler(StandardCharsets.UTF_8)
        ),
        holder -> {
          try {
            return new URI(holder.getContent());
          }
          catch (URISyntaxException e) {
            throw new RuntimeException(e);
          }
        }
    );
  }

  @Override
  public ListenableFuture<Void> updateRulesForDatasource(String dataSource, List<Rule> rules)
  {
    final String path = StringUtils.format("/robux/coordinator/v1/rules/%s", StringUtils.urlEncode(dataSource));
    return client.asyncRequest(
        new RequestBuilder(HttpMethod.POST, path)
            .jsonContent(jsonMapper, rules),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  private Map<String, LookupExtractorFactoryContainer> extractLookupFactory(BytesFullResponseHolder holder)
  {
    Map<String, Object> lookupNameToGenericConfig = JacksonUtils.readValue(
        jsonMapper,
        holder.getContent(),
        new TypeReference<>() {}
    );
    return LookupUtils.tryConvertObjectMapToLookupConfigMap(
        lookupNameToGenericConfig,
        jsonMapper
    );
  }
}
