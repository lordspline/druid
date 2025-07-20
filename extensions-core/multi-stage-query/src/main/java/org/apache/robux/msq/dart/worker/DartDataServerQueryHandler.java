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

package org.apache.robux.msq.dart.worker;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.robux.common.guava.FutureUtils;
import org.apache.robux.discovery.DataServerClient;
import org.apache.robux.error.RobuxException;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.guava.Sequence;
import org.apache.robux.java.util.common.guava.Yielder;
import org.apache.robux.java.util.common.io.Closer;
import org.apache.robux.msq.counters.ChannelCounters;
import org.apache.robux.msq.exec.DataServerQueryHandler;
import org.apache.robux.msq.exec.DataServerQueryHandlerUtils;
import org.apache.robux.msq.exec.DataServerQueryResult;
import org.apache.robux.msq.input.table.DataServerRequestDescriptor;
import org.apache.robux.msq.input.table.RichSegmentDescriptor;
import org.apache.robux.query.Queries;
import org.apache.robux.query.Query;
import org.apache.robux.query.QueryToolChest;
import org.apache.robux.query.QueryToolChestWarehouse;
import org.apache.robux.query.SegmentDescriptor;
import org.apache.robux.query.aggregation.MetricManipulatorFns;
import org.apache.robux.query.context.DefaultResponseContext;
import org.apache.robux.query.context.ResponseContext;
import org.apache.robux.rpc.ServiceClientFactory;
import org.apache.robux.rpc.ServiceLocation;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Dart implementation of {@link DataServerQueryHandler}. Issues queries asynchronously, with no retries.
 */
public class DartDataServerQueryHandler implements DataServerQueryHandler
{
  private final String dataSource;
  private final ChannelCounters channelCounters;
  private final ServiceClientFactory serviceClientFactory;
  private final ObjectMapper objectMapper;
  private final QueryToolChestWarehouse warehouse;
  private final DataServerRequestDescriptor requestDescriptor;

  public DartDataServerQueryHandler(
      String dataSource,
      ChannelCounters channelCounters,
      ServiceClientFactory serviceClientFactory,
      ObjectMapper objectMapper,
      QueryToolChestWarehouse warehouse,
      DataServerRequestDescriptor requestDescriptor
  )
  {
    this.dataSource = dataSource;
    this.channelCounters = channelCounters;
    this.serviceClientFactory = serviceClientFactory;
    this.objectMapper = objectMapper;
    this.warehouse = warehouse;
    this.requestDescriptor = requestDescriptor;
  }

  /**
   * {@inheritDoc}
   *
   * This method returns immediately. The returned future resolves when the server has started sending back
   * its response.
   *
   * Queries are issued once, without retries.
   */
  @Override
  public <RowType, QueryType> ListenableFuture<DataServerQueryResult<RowType>> fetchRowsFromDataServer(
      Query<QueryType> query,
      Function<Sequence<QueryType>, Sequence<RowType>> mappingFunction,
      Closer closer
  )
  {
    final Query<QueryType> preparedQuery =
        Queries.withSpecificSegments(
            DataServerQueryHandlerUtils.prepareQuery(query, dataSource),
            requestDescriptor.getSegments()
                             .stream()
                             .map(RichSegmentDescriptor::toPlainDescriptor)
                             .collect(Collectors.toList())
        );

    final ServiceLocation serviceLocation =
        ServiceLocation.fromRobuxServerMetadata(requestDescriptor.getServerMetadata());
    final DataServerClient dataServerClient = makeDataServerClient(serviceLocation);
    final QueryToolChest<QueryType, Query<QueryType>> toolChest = warehouse.getToolChest(query);
    final Function<QueryType, QueryType> preComputeManipulatorFn =
        toolChest.makePreComputeManipulatorFn(query, MetricManipulatorFns.deserializing());
    final JavaType queryResultType = toolChest.getBaseResultType();
    final ResponseContext responseContext = new DefaultResponseContext();

    return FutureUtils.transform(
        dataServerClient.run(preparedQuery, responseContext, queryResultType, closer),
        resultSequence -> {
          final Yielder<RowType> yielder = DataServerQueryHandlerUtils.createYielder(
              resultSequence.map(preComputeManipulatorFn),
              mappingFunction,
              channelCounters
          );

          final List<SegmentDescriptor> missingSegments =
              DataServerQueryHandlerUtils.getMissingSegments(responseContext);

          if (!missingSegments.isEmpty()) {
            throw RobuxException
                .forPersona(RobuxException.Persona.USER)
                .ofCategory(RobuxException.Category.RUNTIME_FAILURE)
                .build(
                    "Segment[%s]%s not found on server[%s]. Please retry your query.",
                    missingSegments.get(0),
                    missingSegments.size() > 1 ? StringUtils.format(" and[%d] others", missingSegments.size() - 1) : "",
                    serviceLocation.getHostAndPort()
                );
          }

          return new DataServerQueryResult<>(
              Collections.singletonList(yielder),
              Collections.emptyList(),
              dataSource
          );
        }
    );
  }

  private DataServerClient makeDataServerClient(ServiceLocation serviceLocation)
  {
    return new DataServerClient(serviceClientFactory, serviceLocation, objectMapper);
  }
}
