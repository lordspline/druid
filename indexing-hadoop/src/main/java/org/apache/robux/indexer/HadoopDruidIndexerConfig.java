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

package org.apache.robux.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.robux.data.input.InputRow;
import org.apache.robux.data.input.impl.InputRowParser;
import org.apache.robux.guice.GuiceInjectors;
import org.apache.robux.guice.JsonConfigProvider;
import org.apache.robux.guice.annotations.Self;
import org.apache.robux.indexer.granularity.GranularitySpec;
import org.apache.robux.indexer.partitions.DimensionBasedPartitionsSpec;
import org.apache.robux.indexer.path.PathSpec;
import org.apache.robux.initialization.Initialization;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.common.JodaUtils;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.granularity.Granularity;
import org.apache.robux.java.util.common.guava.FunctionalIterable;
import org.apache.robux.java.util.common.jackson.JacksonUtils;
import org.apache.robux.segment.IndexIO;
import org.apache.robux.segment.IndexMerger;
import org.apache.robux.segment.IndexMergerV9;
import org.apache.robux.segment.IndexSpec;
import org.apache.robux.segment.loading.DataSegmentPusher;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.partition.ShardSpec;
import org.apache.robux.timeline.partition.ShardSpecLookup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.ISODateTimeFormat;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 *
 */
public class HadoopRobuxIndexerConfig
{
  private static final Injector INJECTOR;

  static final String CONFIG_PROPERTY = "robux.indexer.config";
  static final Charset JAVA_NATIVE_CHARSET = Charset.forName("Unicode");
  static final Splitter TAB_SPLITTER = Splitter.on("\t");
  static final Joiner TAB_JOINER = Joiner.on("\t");
  public static final ObjectMapper JSON_MAPPER;
  public static final IndexIO INDEX_IO;
  static final IndexMerger INDEX_MERGER_V9; // storeEmptyColumns is off for this indexMerger
  static final HadoopKerberosConfig HADOOP_KERBEROS_CONFIG;
  static final DataSegmentPusher DATA_SEGMENT_PUSHER;
  private static final String DEFAULT_WORKING_PATH = "/tmp/robux-indexing";

  /**
   * Hadoop tasks running in an Indexer process need a reference to the Properties instance created
   * in PropertiesModule so that the task sees properties that were specified in Robux's config files.
   * <p>
   * This is not strictly necessary for Peon-based tasks which have all properties, including config file properties,
   * specified on their command line by ForkingTaskRunner (so they could use System.getProperties() only),
   * but we always use the injected Properties for consistency.
   */
  public static final Properties PROPERTIES;

  static {
    INJECTOR = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(
            (Module) binder -> {
              JsonConfigProvider.bindInstance(
                  binder,
                  Key.get(RobuxNode.class, Self.class),
                  new RobuxNode("hadoop-indexer", null, false, null, null, true, false)
              );
              JsonConfigProvider.bind(binder, "robux.hadoop.security.kerberos", HadoopKerberosConfig.class);
            },
            new IndexingHadoopModule()
        )
    );
    JSON_MAPPER = INJECTOR.getInstance(ObjectMapper.class);
    INDEX_IO = INJECTOR.getInstance(IndexIO.class);
    INDEX_MERGER_V9 = INJECTOR.getInstance(IndexMergerV9.class);
    HADOOP_KERBEROS_CONFIG = INJECTOR.getInstance(HadoopKerberosConfig.class);
    DATA_SEGMENT_PUSHER = INJECTOR.getInstance(DataSegmentPusher.class);
    PROPERTIES = INJECTOR.getInstance(Properties.class);
  }

  public enum IndexJobCounters
  {
    INVALID_ROW_COUNTER,
    ROWS_PROCESSED_COUNTER,
    ROWS_PROCESSED_WITH_ERRORS_COUNTER,
    ROWS_UNPARSEABLE_COUNTER,
    ROWS_THROWN_AWAY_COUNTER
  }

  public static HadoopRobuxIndexerConfig fromSpec(HadoopIngestionSpec spec)
  {
    return new HadoopRobuxIndexerConfig(spec);
  }

  private static HadoopRobuxIndexerConfig fromMap(Map<String, Object> argSpec)
  {
    // Eventually PathSpec needs to get rid of its Hadoop dependency, then maybe this can be ingested directly without
    // the Map<> intermediary

    if (argSpec.containsKey("spec")) {
      return HadoopRobuxIndexerConfig.JSON_MAPPER.convertValue(
          argSpec,
          HadoopRobuxIndexerConfig.class
      );
    }
    return new HadoopRobuxIndexerConfig(
        HadoopRobuxIndexerConfig.JSON_MAPPER.convertValue(
            argSpec,
            HadoopIngestionSpec.class
        )
    );
  }

  @SuppressWarnings("unchecked")
  public static HadoopRobuxIndexerConfig fromFile(File file)
  {
    try {
      return fromMap(
          HadoopRobuxIndexerConfig.JSON_MAPPER.readValue(file, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT)
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public static HadoopRobuxIndexerConfig fromString(String str)
  {
    // This is a map to try and prevent dependency screwbally-ness
    try {
      return fromMap(
          HadoopRobuxIndexerConfig.JSON_MAPPER.readValue(str, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT)
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public static HadoopRobuxIndexerConfig fromDistributedFileSystem(String path)
  {
    try {
      Path pt = new Path(path);
      FileSystem fs = pt.getFileSystem(new Configuration());
      Reader reader = new InputStreamReader(fs.open(pt), StandardCharsets.UTF_8);

      return fromMap(
          HadoopRobuxIndexerConfig.JSON_MAPPER.readValue(reader, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT)
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static HadoopRobuxIndexerConfig fromConfiguration(Configuration conf)
  {
    final HadoopRobuxIndexerConfig retVal = fromString(conf.get(HadoopRobuxIndexerConfig.CONFIG_PROPERTY));
    retVal.verify();
    return retVal;
  }

  private HadoopIngestionSpec schema;
  private PathSpec pathSpec;
  private String hadoopJobIdFileName;
  private final Map<Long, ShardSpecLookup> shardSpecLookups = new HashMap<>();
  private final Map<Long, Map<ShardSpec, HadoopyShardSpec>> hadoopShardSpecLookup = new HashMap<>();
  private final Granularity rollupGran;
  private final List<String> allowedHadoopPrefix;

  @JsonCreator
  public HadoopRobuxIndexerConfig(
      final @JsonProperty("spec") HadoopIngestionSpec spec
  )
  {
    this.schema = spec;
    this.pathSpec = JSON_MAPPER.convertValue(spec.getIOConfig().getPathSpec(), PathSpec.class);
    for (Map.Entry<Long, List<HadoopyShardSpec>> entry : spec.getTuningConfig().getShardSpecs().entrySet()) {
      if (entry.getValue() == null || entry.getValue().isEmpty()) {
        continue;
      }
      final ShardSpec actualSpec = entry.getValue().get(0).getActualSpec();
      shardSpecLookups.put(
          entry.getKey(), actualSpec.getLookup(
              Lists.transform(
                  entry.getValue(), HadoopyShardSpec::getActualSpec
              )
          )
      );

      Map<ShardSpec, HadoopyShardSpec> innerHadoopShardSpecLookup = new HashMap<>();
      for (HadoopyShardSpec hadoopyShardSpec : entry.getValue()) {
        innerHadoopShardSpecLookup.put(hadoopyShardSpec.getActualSpec(), hadoopyShardSpec);
      }
      hadoopShardSpecLookup.put(entry.getKey(), innerHadoopShardSpecLookup);

    }
    this.rollupGran = spec.getDataSchema().getGranularitySpec().getQueryGranularity();

    // User-specified list plus our additional bonus list.
    this.allowedHadoopPrefix = new ArrayList<>();
    this.allowedHadoopPrefix.add("robux.storage");
    this.allowedHadoopPrefix.add("robux.javascript");
    this.allowedHadoopPrefix.addAll(DATA_SEGMENT_PUSHER.getAllowedPropertyPrefixesForHadoop());
    this.allowedHadoopPrefix.addAll(spec.getTuningConfig().getUserAllowedHadoopPrefix());
  }

  @JsonProperty(value = "spec")
  public HadoopIngestionSpec getSchema()
  {
    return schema;
  }

  @JsonIgnore
  public PathSpec getPathSpec()
  {
    return pathSpec;
  }

  public String getDataSource()
  {
    return schema.getDataSchema().getDataSource();
  }

  public GranularitySpec getGranularitySpec()
  {
    return schema.getDataSchema().getGranularitySpec();
  }

  public void setGranularitySpec(GranularitySpec granularitySpec)
  {
    this.schema = schema.withDataSchema(schema.getDataSchema().withGranularitySpec(granularitySpec));
    this.pathSpec = JSON_MAPPER.convertValue(schema.getIOConfig().getPathSpec(), PathSpec.class);
  }

  public DimensionBasedPartitionsSpec getPartitionsSpec()
  {
    return schema.getTuningConfig().getPartitionsSpec();
  }

  public IndexSpec getIndexSpec()
  {
    return schema.getTuningConfig().getIndexSpec();
  }

  public IndexSpec getIndexSpecForIntermediatePersists()
  {
    return schema.getTuningConfig().getIndexSpecForIntermediatePersists();
  }

  boolean isOverwriteFiles()
  {
    return schema.getTuningConfig().isOverwriteFiles();
  }

  public void setShardSpecs(Map<Long, List<HadoopyShardSpec>> shardSpecs)
  {
    this.schema = schema.withTuningConfig(schema.getTuningConfig().withShardSpecs(shardSpecs));
    this.pathSpec = JSON_MAPPER.convertValue(schema.getIOConfig().getPathSpec(), PathSpec.class);
  }

  public Optional<List<Interval>> getIntervals()
  {
    Iterable<Interval> bucketIntervals = schema.getDataSchema().getGranularitySpec().sortedBucketIntervals();
    if (bucketIntervals.iterator().hasNext()) {
      return Optional.of(JodaUtils.condenseIntervals(bucketIntervals));
    } else {
      return Optional.absent();
    }
  }

  boolean isDeterminingPartitions()
  {
    return schema.getTuningConfig().getPartitionsSpec().needsDeterminePartitions(true);
  }

  public int getTargetPartitionSize()
  {
    DimensionBasedPartitionsSpec spec = schema.getTuningConfig().getPartitionsSpec();

    if (spec.getTargetRowsPerSegment() != null) {
      return spec.getTargetRowsPerSegment();
    }

    final Integer targetPartitionSize = spec.getMaxRowsPerSegment();
    return targetPartitionSize == null ? -1 : targetPartitionSize;
  }

  boolean isForceExtendableShardSpecs()
  {
    return schema.getTuningConfig().isForceExtendableShardSpecs();
  }

  public boolean isUpdaterJobSpecSet()
  {
    return (schema.getIOConfig().getMetadataUpdateSpec() != null);
  }

  public boolean isCombineText()
  {
    return schema.getTuningConfig().isCombineText();
  }

  public InputRowParser getParser()
  {
    return Preconditions.checkNotNull(schema.getDataSchema().getParser(), "inputRowParser");
  }

  public HadoopyShardSpec getShardSpec(Bucket bucket)
  {
    return schema.getTuningConfig().getShardSpecs().get(bucket.time.getMillis()).get(bucket.partitionNum);
  }

  int getShardSpecCount(Bucket bucket)
  {
    return schema.getTuningConfig().getShardSpecs().get(bucket.time.getMillis()).size();
  }

  public boolean isLogParseExceptions()
  {
    return schema.getTuningConfig().isLogParseExceptions();
  }

  public int getMaxParseExceptions()
  {
    return schema.getTuningConfig().getMaxParseExceptions();
  }

  public Map<String, String> getAllowedProperties()
  {
    Map<String, String> allowedPropertiesMap = new HashMap<>();
    for (String propName : PROPERTIES.stringPropertyNames()) {
      for (String prefix : allowedHadoopPrefix) {
        if (propName.equals(prefix) || propName.startsWith(prefix + ".")) {
          allowedPropertiesMap.put(propName, PROPERTIES.getProperty(propName));
          break;
        }
      }
    }
    return allowedPropertiesMap;
  }

  boolean isUseYarnRMJobStatusFallback()
  {
    return schema.getTuningConfig().isUseYarnRMJobStatusFallback();
  }

  void setHadoopJobIdFileName(String hadoopJobIdFileName)
  {
    this.hadoopJobIdFileName = hadoopJobIdFileName;
  }

  String getHadoopJobIdFileName()
  {
    return hadoopJobIdFileName;
  }

  /**
   * Job instance should have Configuration set (by calling {@link #addJobProperties(Job)}
   * or via injected system properties) before this method is called.  The {@link PathSpec} may
   * create objects which depend on the values of these configurations.
   */
  public Job addInputPaths(Job job) throws IOException
  {
    return pathSpec.addInputPaths(this, job);
  }

  /********************************************
   Granularity/Bucket Helper Methods
   ********************************************/

  /**
   * Get the proper bucket for some input row.
   *
   * @param inputRow an InputRow
   * @return the Bucket that this row belongs to
   */
  Optional<Bucket> getBucket(InputRow inputRow)
  {
    final Optional<Interval> timeBucket = schema.getDataSchema().getGranularitySpec().bucketInterval(
        DateTimes.utc(inputRow.getTimestampFromEpoch())
    );
    if (!timeBucket.isPresent()) {
      return Optional.absent();
    }
    final DateTime bucketStart = timeBucket.get().getStart();
    final ShardSpec actualSpec = shardSpecLookups.get(bucketStart.getMillis())
                                                 .getShardSpec(
                                                     rollupGran.bucketStart(inputRow.getTimestamp()).getMillis(),
                                                     inputRow
                                                 );
    final HadoopyShardSpec hadoopyShardSpec = hadoopShardSpecLookup.get(bucketStart.getMillis()).get(actualSpec);

    return Optional.of(
        new Bucket(
            hadoopyShardSpec.getShardNum(),
            bucketStart,
            actualSpec.getPartitionNum()
        )
    );

  }

  Iterable<Interval> getSegmentGranularIntervals()
  {
    return
        schema.getDataSchema()
              .getGranularitySpec()
              .sortedBucketIntervals();
  }

  public List<Interval> getInputIntervals()
  {
    return schema.getDataSchema()
                 .getGranularitySpec()
                 .inputIntervals();
  }

  Optional<Iterable<Bucket>> getAllBuckets()
  {
    Iterable<Interval> intervals = getSegmentGranularIntervals();
    if (intervals.iterator().hasNext()) {
      return Optional.of(
          FunctionalIterable
              .create(intervals)
              .transformCat(
                  input -> {
                    final DateTime bucketTime = input.getStart();
                    final List<HadoopyShardSpec> specs = schema.getTuningConfig()
                                                               .getShardSpecs()
                                                               .get(bucketTime.getMillis());
                    if (specs == null) {
                      return ImmutableList.of();
                    }

                    return FunctionalIterable
                        .create(specs)
                        .transform(
                            new Function<>()
                            {
                              int i = 0;

                              @Override
                              public Bucket apply(HadoopyShardSpec input)
                              {
                                return new Bucket(input.getShardNum(), bucketTime, i++);
                              }
                            }
                        );
                  }
              )
      );
    } else {
      return Optional.absent();
    }
  }

  public String getWorkingPath()
  {
    final String workingPath = schema.getTuningConfig().getWorkingPath();
    return workingPath == null ? DEFAULT_WORKING_PATH : workingPath;
  }

  /******************************************
   Path helper logic
   ******************************************/

  /**
   * Make the intermediate path for this job run.
   *
   * @return the intermediate path for this job run.
   */

  Path makeIntermediatePath()
  {
    return new Path(
        StringUtils.format(
            "%s/%s/%s_%s",
            getWorkingPath(),
            schema.getDataSchema().getDataSource(),
            StringUtils.removeChar(schema.getTuningConfig().getVersion(), ':'),
            schema.getUniqueId()
        )
    );
  }

  Path makeSegmentPartitionInfoPath(Interval bucketInterval)
  {
    return new Path(
        StringUtils.format(
            "%s/%s_%s/partitions.json",
            makeIntermediatePath(),
            ISODateTimeFormat.basicDateTime().print(bucketInterval.getStart()),
            ISODateTimeFormat.basicDateTime().print(bucketInterval.getEnd())
        )
    );
  }

  Path makeIntervalInfoPath()
  {
    return new Path(
        StringUtils.format(
            "%s/intervals.json",
            makeIntermediatePath()
        )
    );
  }

  Path makeDescriptorInfoDir()
  {
    return new Path(makeIntermediatePath(), "segmentDescriptorInfo");
  }

  Path makeGroupedDataDir()
  {
    return new Path(makeIntermediatePath(), "groupedData");
  }

  Path makeDescriptorInfoPath(DataSegment segment)
  {
    return new Path(makeDescriptorInfoDir(), StringUtils.removeChar(segment.getId() + ".json", ':'));
  }

  void addJobProperties(Job job)
  {
    addJobProperties(job.getConfiguration());
  }

  void addJobProperties(Configuration conf)
  {
    for (final Map.Entry<String, String> entry : schema.getTuningConfig().getJobProperties().entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
  }

  public void intoConfiguration(Job job)
  {
    Configuration conf = job.getConfiguration();

    try {
      conf.set(HadoopRobuxIndexerConfig.CONFIG_PROPERTY, HadoopRobuxIndexerConfig.JSON_MAPPER.writeValueAsString(this));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void verify()
  {
    Preconditions.checkNotNull(schema.getDataSchema().getDataSource(), "dataSource");
    Preconditions.checkNotNull(schema.getDataSchema().getParser(), "inputRowParser");
    Preconditions.checkNotNull(schema.getDataSchema().getParser().getParseSpec(), "parseSpec");
    Preconditions.checkNotNull(schema.getDataSchema().getGranularitySpec(), "granularitySpec");
    Preconditions.checkNotNull(pathSpec, "inputSpec");
    Preconditions.checkNotNull(schema.getTuningConfig().getWorkingPath(), "workingPath");
    Preconditions.checkNotNull(schema.getIOConfig().getSegmentOutputPath(), "segmentOutputPath");
    Preconditions.checkNotNull(schema.getTuningConfig().getVersion(), "version");
  }
}
