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

package org.apache.robux.segment.realtime.appenderator;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.robux.client.cache.CacheConfig;
import org.apache.robux.client.cache.CachePopulatorStats;
import org.apache.robux.client.cache.MapCache;
import org.apache.robux.data.input.impl.DimensionsSpec;
import org.apache.robux.data.input.impl.JSONParseSpec;
import org.apache.robux.data.input.impl.MapInputRowParser;
import org.apache.robux.data.input.impl.TimestampSpec;
import org.apache.robux.guice.BuiltInTypesModule;
import org.apache.robux.indexer.granularity.UniformGranularitySpec;
import org.apache.robux.jackson.AggregatorsModule;
import org.apache.robux.jackson.DefaultObjectMapper;
import org.apache.robux.java.util.common.FileUtils;
import org.apache.robux.java.util.common.concurrent.Execs;
import org.apache.robux.java.util.common.granularity.Granularities;
import org.apache.robux.java.util.emitter.EmittingLogger;
import org.apache.robux.java.util.emitter.core.NoopEmitter;
import org.apache.robux.java.util.emitter.service.ServiceEmitter;
import org.apache.robux.math.expr.ExprMacroTable;
import org.apache.robux.query.DefaultGenericQueryMetricsFactory;
import org.apache.robux.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.robux.query.ForwardingQueryProcessingPool;
import org.apache.robux.query.QueryRunnerTestHelper;
import org.apache.robux.query.aggregation.CountAggregatorFactory;
import org.apache.robux.query.aggregation.LongSumAggregatorFactory;
import org.apache.robux.query.expression.TestExprMacroTable;
import org.apache.robux.query.policy.NoopPolicyEnforcer;
import org.apache.robux.query.policy.PolicyEnforcer;
import org.apache.robux.query.scan.ScanQuery;
import org.apache.robux.query.scan.ScanQueryConfig;
import org.apache.robux.query.scan.ScanQueryEngine;
import org.apache.robux.query.scan.ScanQueryQueryToolChest;
import org.apache.robux.query.scan.ScanQueryRunnerFactory;
import org.apache.robux.query.timeseries.TimeseriesQuery;
import org.apache.robux.query.timeseries.TimeseriesQueryEngine;
import org.apache.robux.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.robux.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.robux.segment.IndexIO;
import org.apache.robux.segment.IndexMerger;
import org.apache.robux.segment.IndexMergerV9;
import org.apache.robux.segment.IndexSpec;
import org.apache.robux.segment.column.ColumnConfig;
import org.apache.robux.segment.incremental.ParseExceptionHandler;
import org.apache.robux.segment.incremental.RowIngestionMeters;
import org.apache.robux.segment.incremental.SimpleRowIngestionMeters;
import org.apache.robux.segment.indexing.DataSchema;
import org.apache.robux.segment.indexing.TuningConfig;
import org.apache.robux.segment.loading.DataSegmentPusher;
import org.apache.robux.segment.loading.SegmentLoaderConfig;
import org.apache.robux.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.robux.segment.realtime.SegmentGenerationMetrics;
import org.apache.robux.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.robux.server.coordination.DataSegmentAnnouncer;
import org.apache.robux.server.coordination.NoopDataSegmentAnnouncer;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.partition.LinearShardSpec;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

public class StreamAppenderatorTester implements AutoCloseable
{
  public static final String DATASOURCE = "foo";

  private final DataSchema schema;
  private final AppenderatorConfig tuningConfig;
  private final SegmentGenerationMetrics metrics;
  private final DataSegmentPusher dataSegmentPusher;
  private final ObjectMapper objectMapper;
  private final Appenderator appenderator;
  private final ExecutorService queryExecutor;
  private final ServiceEmitter emitter;

  private final List<DataSegment> pushedSegments = new CopyOnWriteArrayList<>();

  public StreamAppenderatorTester(
      final int delayInMilli,
      final int maxRowsInMemory,
      final long maxSizeInBytes,
      final File basePersistDirectory,
      final boolean enablePushFailure,
      final RowIngestionMeters rowIngestionMeters,
      final boolean skipBytesInMemoryOverheadCheck,
      final DataSegmentAnnouncer announcer,
      final CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig,
      final ServiceEmitter serviceEmitter,
      final PolicyEnforcer policyEnforcer
  )
  {
    objectMapper = new DefaultObjectMapper();
    objectMapper.registerSubtypes(LinearShardSpec.class);
    objectMapper.registerModules(new AggregatorsModule());
    objectMapper.registerModules(new BuiltInTypesModule().getJacksonModules());
    objectMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), objectMapper)
    );

    final Map<String, Object> parserMap = objectMapper.convertValue(
        new MapInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("ts", "auto", null),
                DimensionsSpec.EMPTY,
                null,
                null,
                null
            )
        ),
        Map.class
    );
    schema = DataSchema.builder()
                       .withDataSource(DATASOURCE)
                       .withParserMap(parserMap)
                       .withAggregators(
                           new CountAggregatorFactory("count"),
                           new LongSumAggregatorFactory("met", "met")
                       )
                       .withGranularity(new UniformGranularitySpec(Granularities.MINUTE, Granularities.NONE, null))
                       .withObjectMapper(objectMapper)
                       .build();
    tuningConfig = new TestAppenderatorConfig(
        TuningConfig.DEFAULT_APPENDABLE_INDEX,
        maxRowsInMemory,
        maxSizeInBytes == 0L ? getDefaultMaxBytesInMemory() : maxSizeInBytes,
        skipBytesInMemoryOverheadCheck,
        IndexSpec.DEFAULT,
        0,
        false,
        0L,
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        IndexMerger.UNLIMITED_MAX_COLUMNS_TO_MERGE,
        basePersistDirectory
    );

    metrics = new SegmentGenerationMetrics();
    queryExecutor = Execs.singleThreaded("queryExecutor(%d)");

    IndexIO indexIO = new IndexIO(
        objectMapper,
        new ColumnConfig()
        {
        }
    );

    IndexMergerV9 indexMerger = new IndexMergerV9(
        objectMapper,
        indexIO,
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );

    emitter = serviceEmitter == null ? new ServiceEmitter(
        "test",
        "test",
        new NoopEmitter()
    ) : serviceEmitter;

    emitter.start();
    EmittingLogger.registerEmitter(emitter);
    dataSegmentPusher = new DataSegmentPusher()
    {
      @Deprecated
      @Override
      public String getPathForHadoop(String dataSource)
      {
        return getPathForHadoop();
      }

      @Override
      public String getPathForHadoop()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public DataSegment push(File file, DataSegment segment, boolean useUniquePath) throws IOException
      {
        if (enablePushFailure) {
          throw new IOException("Push failure test");
        }
        pushedSegments.add(segment);
        return segment;
      }

      @Override
      public Map<String, Object> makeLoadSpec(URI uri)
      {
        throw new UnsupportedOperationException();
      }
    };

    if (delayInMilli <= 0) {
      appenderator = Appenderators.createRealtime(
          null,
          schema.getDataSource(),
          schema,
          tuningConfig,
          metrics,
          dataSegmentPusher,
          objectMapper,
          indexIO,
          indexMerger,
          DefaultQueryRunnerFactoryConglomerate.buildFromQueryRunnerFactories(ImmutableMap.of(
              TimeseriesQuery.class, new TimeseriesQueryRunnerFactory(
                  new TimeseriesQueryQueryToolChest(),
                  new TimeseriesQueryEngine(),
                  QueryRunnerTestHelper.NOOP_QUERYWATCHER
              ),
              ScanQuery.class, new ScanQueryRunnerFactory(
                  new ScanQueryQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
                  new ScanQueryEngine(),
                  new ScanQueryConfig()
              )
          )),
          announcer,
          emitter,
          new ForwardingQueryProcessingPool(queryExecutor),
          MapCache.create(2048),
          new CacheConfig(),
          new CachePopulatorStats(),
          policyEnforcer,
          rowIngestionMeters,
          new ParseExceptionHandler(rowIngestionMeters, false, Integer.MAX_VALUE, 0),
          centralizedDatasourceSchemaConfig
      );
    } else {
      SegmentLoaderConfig segmentLoaderConfig = new SegmentLoaderConfig()
      {
        @Override
        public int getDropSegmentDelayMillis()
        {
          return delayInMilli;
        }
      };
      appenderator = Appenderators.createRealtime(
          segmentLoaderConfig,
          schema.getDataSource(),
          schema,
          tuningConfig,
          metrics,
          dataSegmentPusher,
          objectMapper,
          indexIO,
          indexMerger,
          DefaultQueryRunnerFactoryConglomerate.buildFromQueryRunnerFactories(ImmutableMap.of(
              TimeseriesQuery.class, new TimeseriesQueryRunnerFactory(
                  new TimeseriesQueryQueryToolChest(),
                  new TimeseriesQueryEngine(),
                  QueryRunnerTestHelper.NOOP_QUERYWATCHER
              ),
              ScanQuery.class, new ScanQueryRunnerFactory(
                  new ScanQueryQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
                  new ScanQueryEngine(),
                  new ScanQueryConfig()
              )
          )),
          new NoopDataSegmentAnnouncer(),
          emitter,
          new ForwardingQueryProcessingPool(queryExecutor),
          MapCache.create(2048),
          new CacheConfig(),
          new CachePopulatorStats(),
          NoopPolicyEnforcer.instance(),
          rowIngestionMeters,
          new ParseExceptionHandler(rowIngestionMeters, false, Integer.MAX_VALUE, 0),
          centralizedDatasourceSchemaConfig
      );
    }
  }

  private long getDefaultMaxBytesInMemory()
  {
    return (Runtime.getRuntime().totalMemory()) / 3;
  }

  public DataSchema getSchema()
  {
    return schema;
  }

  public AppenderatorConfig getTuningConfig()
  {
    return tuningConfig;
  }

  public SegmentGenerationMetrics getMetrics()
  {
    return metrics;
  }

  public DataSegmentPusher getDataSegmentPusher()
  {
    return dataSegmentPusher;
  }

  public ObjectMapper getObjectMapper()
  {
    return objectMapper;
  }

  public Appenderator getAppenderator()
  {
    return appenderator;
  }

  public List<DataSegment> getPushedSegments()
  {
    return pushedSegments;
  }

  @Override
  public void close() throws Exception
  {
    appenderator.close();
    queryExecutor.shutdownNow();
    emitter.close();
    FileUtils.deleteDirectory(tuningConfig.getBasePersistDirectory());
  }

  public static class Builder
  {
    private int maxRowsInMemory;
    private long maxSizeInBytes = -1;
    private File basePersistDirectory;
    private boolean enablePushFailure;
    private RowIngestionMeters rowIngestionMeters;
    private boolean skipBytesInMemoryOverheadCheck;
    private int delayInMilli = 0;
    private ServiceEmitter serviceEmitter;
    private PolicyEnforcer policyEnforcer = NoopPolicyEnforcer.instance();

    public Builder maxRowsInMemory(final int maxRowsInMemory)
    {
      this.maxRowsInMemory = maxRowsInMemory;
      return this;
    }

    public Builder maxSizeInBytes(final long maxSizeInBytes)
    {
      this.maxSizeInBytes = maxSizeInBytes;
      return this;
    }

    public Builder basePersistDirectory(final File basePersistDirectory)
    {
      this.basePersistDirectory = basePersistDirectory;
      return this;
    }

    public Builder enablePushFailure(final boolean enablePushFailure)
    {
      this.enablePushFailure = enablePushFailure;
      return this;
    }

    public Builder rowIngestionMeters(final RowIngestionMeters rowIngestionMeters)
    {
      this.rowIngestionMeters = rowIngestionMeters;
      return this;
    }

    public Builder skipBytesInMemoryOverheadCheck(final boolean skipBytesInMemoryOverheadCheck)
    {
      this.skipBytesInMemoryOverheadCheck = skipBytesInMemoryOverheadCheck;
      return this;
    }

    public Builder withSegmentDropDelayInMilli(int delayInMilli)
    {
      this.delayInMilli = delayInMilli;
      return this;
    }

    public Builder withServiceEmitter(ServiceEmitter serviceEmitter)
    {
      this.serviceEmitter = serviceEmitter;
      return this;
    }

    public Builder withPolicyEnforcer(PolicyEnforcer policyEnforcer)
    {
      this.policyEnforcer = policyEnforcer;
      return this;
    }

    public StreamAppenderatorTester build()
    {
      return new StreamAppenderatorTester(
          delayInMilli,
          maxRowsInMemory,
          maxSizeInBytes,
          Preconditions.checkNotNull(basePersistDirectory, "basePersistDirectory"),
          enablePushFailure,
          rowIngestionMeters == null ? new SimpleRowIngestionMeters() : rowIngestionMeters,
          skipBytesInMemoryOverheadCheck,
          new NoopDataSegmentAnnouncer(),
          CentralizedDatasourceSchemaConfig.create(),
          serviceEmitter,
          policyEnforcer
      );
    }

    public StreamAppenderatorTester build(
        DataSegmentAnnouncer dataSegmentAnnouncer,
        CentralizedDatasourceSchemaConfig config
    )
    {
      return new StreamAppenderatorTester(
          delayInMilli,
          maxRowsInMemory,
          maxSizeInBytes,
          Preconditions.checkNotNull(basePersistDirectory, "basePersistDirectory"),
          enablePushFailure,
          rowIngestionMeters == null ? new SimpleRowIngestionMeters() : rowIngestionMeters,
          skipBytesInMemoryOverheadCheck,
          dataSegmentAnnouncer,
          config,
          serviceEmitter,
          policyEnforcer
      );
    }
  }
}
