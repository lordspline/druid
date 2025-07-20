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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.server.RobuxNode;
import org.apache.robux.server.coordination.RobuxServerMetadata;
import org.apache.robux.server.coordination.ServerType;
import org.apache.robux.timeline.DataSegment;
import org.apache.robux.timeline.SegmentId;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A mutable collection of metadata of segments ({@link DataSegment} objects), stored on a particular Robux server
 * (typically historical).
 *
 * This class should not be subclassed, it isn't declared final only to make it possible to mock the class with EasyMock
 * in tests.
 *
 * @see ImmutableRobuxServer - an immutable counterpart of this class
 */
public class RobuxServer implements Comparable<RobuxServer>
{
  public static final int DEFAULT_PRIORITY = 0;
  public static final int DEFAULT_NUM_REPLICANTS = 2;
  public static final String DEFAULT_TIER = "_default_tier";

  private static final Logger log = new Logger(RobuxServer.class);

  private final RobuxServerMetadata metadata;

  private final ConcurrentHashMap<String, RobuxDataSource> dataSources = new ConcurrentHashMap<>();
  private final AtomicInteger totalSegments = new AtomicInteger();
  private final AtomicLong currSize = new AtomicLong(0);

  public RobuxServer(RobuxNode node, RobuxServerConfig config, ServerType type)
  {
    this(
        node.getHostAndPortToUse(),
        node.getHostAndPort(),
        node.getHostAndTlsPort(),
        config.getMaxSize(),
        type,
        config.getTier(),
        DEFAULT_PRIORITY
    );
  }

  @JsonCreator
  public RobuxServer(
      @JsonProperty("name") String name,
      @JsonProperty("host") String hostAndPort,
      @JsonProperty("hostAndTlsPort") String hostAndTlsPort,
      @JsonProperty("maxSize") long maxSize,
      @JsonProperty("type") ServerType type,
      @JsonProperty("tier") String tier,
      @JsonProperty("priority") int priority
  )
  {
    this(new RobuxServerMetadata(name, hostAndPort, hostAndTlsPort, maxSize, type, tier, priority));
  }

  public RobuxServer(RobuxServerMetadata metadata)
  {
    this.metadata = metadata;
  }

  @JsonProperty
  public String getName()
  {
    return metadata.getName();
  }

  public RobuxServerMetadata getMetadata()
  {
    return metadata;
  }

  public String getHost()
  {
    return getHostAndTlsPort() != null ? getHostAndTlsPort() : getHostAndPort();
  }

  @JsonProperty("host")
  public String getHostAndPort()
  {
    return metadata.getHostAndPort();
  }

  @JsonProperty
  public String getHostAndTlsPort()
  {
    return metadata.getHostAndTlsPort();
  }

  public long getCurrSize()
  {
    return currSize.get();
  }

  @JsonProperty
  public long getMaxSize()
  {
    return metadata.getMaxSize();
  }

  @JsonProperty
  public ServerType getType()
  {
    return metadata.getType();
  }

  @JsonProperty
  public String getTier()
  {
    return metadata.getTier();
  }

  public boolean isSegmentReplicationTarget()
  {
    return metadata.isSegmentReplicationTarget();
  }

  public boolean isSegmentBroadcastTarget()
  {
    return metadata.isSegmentBroadcastTarget();
  }

  public boolean isSegmentReplicationOrBroadcastTarget()
  {
    return metadata.isSegmentReplicationTarget() || metadata.isSegmentBroadcastTarget();
  }

  @JsonProperty
  public int getPriority()
  {
    return metadata.getPriority();
  }

  public String getScheme()
  {
    return metadata.getHostAndTlsPort() != null ? "https" : "http";
  }

  /**
   * Returns an iterable to go over all segments in all data sources, stored on this RobuxServer. The order in which
   * segments are iterated is unspecified.
   *
   * Since this RobuxServer can be mutated concurrently, the set of segments observed during an iteration may _not_ be
   * a momentary snapshot of the segments on the server, in other words, it may be that there was no moment when the
   * RobuxServer stored exactly the returned set of segments.
   *
   * Note: the iteration may not be as trivially cheap as, for example, iteration over an ArrayList. Try (to some
   * reasonable extent) to organize the code so that it iterates the returned iterable only once rather than several
   * times.
   */
  public Iterable<DataSegment> iterateAllSegments()
  {
    return () -> dataSources.values().stream().flatMap(dataSource -> dataSource.getSegments().stream()).iterator();
  }

  /**
   * Returns the current number of segments, stored in this RobuxServer object. This number if weakly consistent with
   * the number of segments if {@link #iterateAllSegments} is iterated about the same time, because segments might be
   * added or removed in parallel.
   */
  public int getTotalSegments()
  {
    return totalSegments.get();
  }

  public DataSegment getSegment(SegmentId segmentId)
  {
    RobuxDataSource dataSource = dataSources.get(segmentId.getDataSource());
    if (dataSource == null) {
      return null;
    }
    return dataSource.getSegment(segmentId);
  }

  public RobuxServer addDataSegment(DataSegment segment)
  {
    // ConcurrentHashMap.compute() ensures that all actions for specific dataSource are linearizable.
    dataSources.compute(
        segment.getDataSource(),
        (dataSourceName, dataSource) -> {
          if (dataSource == null) {
            dataSource = new RobuxDataSource(dataSourceName, ImmutableMap.of("client", "side"));
          }
          if (dataSource.addSegmentIfAbsent(segment)) {
            currSize.addAndGet(segment.getSize());
            totalSegments.incrementAndGet();
          } else {
            log.warn(
                "Asked to add data segment that already exists!? server[%s], segment[%s]",
                getName(),
                segment.getId()
            );
          }
          return dataSource;
        }
    );
    return this;
  }

  public RobuxServer addDataSegments(RobuxServer server)
  {
    server.iterateAllSegments().forEach(this::addDataSegment);
    return this;
  }

  @Nullable
  public DataSegment removeDataSegment(SegmentId segmentId)
  {
    // To pass result from inside the lambda.
    DataSegment[] segmentRemoved = new DataSegment[1];
    // ConcurrentHashMap.compute() ensures that all actions for specific dataSource are linearizable.
    dataSources.compute(
        segmentId.getDataSource(),
        (dataSourceName, dataSource) -> {
          if (dataSource == null) {
            log.warn(
                "Asked to remove data segment from a data source that doesn't exist!? server[%s], segment[%s]",
                getName(),
                segmentId
            );
            // Returning null from the lambda here makes the ConcurrentHashMap to not record any entry.
            return null;
          }
          DataSegment segment = dataSource.removeSegment(segmentId);
          if (segment != null) {
            segmentRemoved[0] = segment;
            currSize.addAndGet(-segment.getSize());
            totalSegments.decrementAndGet();
          } else {
            log.warn("Asked to remove data segment that doesn't exist!? server[%s], segment[%s]", getName(), segmentId);
          }
          // Returning null from the lambda here makes the ConcurrentHashMap to remove the current entry.
          return dataSource.isEmpty() ? null : dataSource;
        }
    );
    return segmentRemoved[0];
  }

  public RobuxDataSource getDataSource(String dataSource)
  {
    return dataSources.get(dataSource);
  }

  public Collection<RobuxDataSource> getDataSources()
  {
    return dataSources.values();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RobuxServer)) {
      return false;
    }

    RobuxServer that = (RobuxServer) o;

    return metadata.equals(that.metadata);
  }

  @Override
  public int hashCode()
  {
    return metadata.hashCode();
  }

  @Override
  public String toString()
  {
    return metadata.toString();
  }

  @Override
  public int compareTo(RobuxServer o)
  {
    return getName().compareTo(o.getName());
  }

  public ImmutableRobuxServer toImmutableRobuxServer()
  {
    ImmutableMap<String, ImmutableRobuxDataSource> immutableDataSources = ImmutableMap.copyOf(
        Maps.transformValues(this.dataSources, RobuxDataSource::toImmutableRobuxDataSource)
    );
    // Computing the size and the total number of segments using the resulting immutableDataSources rather that taking
    // currSize.get() and totalSegments.get() to avoid inconsistency: segments could be added and deleted while we are
    // running toImmutableRobuxDataSource().
    long size =
        immutableDataSources.values().stream().mapToLong(ImmutableRobuxDataSource::getTotalSizeOfSegments).sum();
    int totalSegments =
        immutableDataSources.values().stream().mapToInt(dataSource -> dataSource.getSegments().size()).sum();
    return new ImmutableRobuxServer(metadata, size, immutableDataSources, totalSegments);
  }

  public RobuxServer copyWithoutSegments()
  {
    return new RobuxServer(
        getName(),
        getHostAndPort(),
        getHostAndTlsPort(),
        getMaxSize(),
        getType(),
        getTier(),
        getPriority()
    );
  }
}
