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

package org.apache.robux.indexing.common;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import org.apache.robux.data.input.impl.NoopInputFormat;
import org.apache.robux.data.input.impl.NoopInputSource;
import org.apache.robux.guice.RobuxSecondaryModule;
import org.apache.robux.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.robux.indexing.common.task.TestAppenderatorsManager;
import org.apache.robux.jackson.DefaultObjectMapper;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.math.expr.ExprMacroTable;
import org.apache.robux.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.robux.rpc.indexing.NoopOverlordClient;
import org.apache.robux.rpc.indexing.OverlordClient;
import org.apache.robux.segment.IndexIO;
import org.apache.robux.segment.IndexMergerV9;
import org.apache.robux.segment.IndexMergerV9Factory;
import org.apache.robux.segment.TestHelper;
import org.apache.robux.segment.column.ColumnConfig;
import org.apache.robux.segment.incremental.RowIngestionMetersFactory;
import org.apache.robux.segment.loading.LocalDataSegmentPuller;
import org.apache.robux.segment.loading.LocalLoadSpec;
import org.apache.robux.segment.realtime.ChatHandlerProvider;
import org.apache.robux.segment.realtime.NoopChatHandlerProvider;
import org.apache.robux.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.robux.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.robux.server.security.AuthConfig;
import org.apache.robux.server.security.AuthorizerMapper;
import org.apache.robux.timeline.DataSegment.PruneSpecsHolder;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class TestUtils
{
  public static final OverlordClient OVERLORD_SERVICE_CLIENT = new NoopOverlordClient();
  public static final AppenderatorsManager APPENDERATORS_MANAGER = new TestAppenderatorsManager();

  private static final Logger log = new Logger(TestUtils.class);

  private final ObjectMapper jsonMapper;
  private final IndexMergerV9Factory indexMergerV9Factory;
  private final IndexIO indexIO;
  private final RowIngestionMetersFactory rowIngestionMetersFactory;

  public TestUtils()
  {
    this.jsonMapper = new DefaultObjectMapper();
    indexIO = new IndexIO(jsonMapper, ColumnConfig.DEFAULT);
    indexMergerV9Factory = new IndexMergerV9Factory(
        jsonMapper,
        indexIO,
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );

    this.rowIngestionMetersFactory = new DropwizardRowIngestionMetersFactory();

    jsonMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class, LookupEnabledTestExprMacroTable.INSTANCE)
            .addValue(IndexIO.class, indexIO)
            .addValue(ObjectMapper.class, jsonMapper)
            .addValue(ChatHandlerProvider.class, new NoopChatHandlerProvider())
            .addValue(AuthConfig.class, new AuthConfig())
            .addValue(AuthorizerMapper.class, null)
            .addValue(RowIngestionMetersFactory.class, rowIngestionMetersFactory)
            .addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT)
            .addValue(OverlordClient.class, OVERLORD_SERVICE_CLIENT)
            .addValue(AuthorizerMapper.class, new AuthorizerMapper(ImmutableMap.of()))
            .addValue(AppenderatorsManager.class, APPENDERATORS_MANAGER)
            .addValue(LocalDataSegmentPuller.class, new LocalDataSegmentPuller())
    );

    jsonMapper.registerModule(
        new SimpleModule()
        {
          @Override
          public void setupModule(SetupContext context)
          {
            context.registerSubtypes(
                new NamedType(LocalLoadSpec.class, "local"),
                new NamedType(NoopInputSource.class, "noop"),
                new NamedType(NoopInputFormat.class, "noop")
            );
          }
        }
    );
    RobuxSecondaryModule.setupAnnotationIntrospector(jsonMapper, TestHelper.makeAnnotationIntrospector());
  }

  public ObjectMapper getTestObjectMapper()
  {
    return jsonMapper;
  }

  public IndexMergerV9 getTestIndexMergerV9()
  {
    return indexMergerV9Factory.create(true);
  }

  public IndexMergerV9Factory getIndexMergerV9Factory()
  {
    return indexMergerV9Factory;
  }

  public IndexIO getTestIndexIO()
  {
    return indexIO;
  }

  public RowIngestionMetersFactory getRowIngestionMetersFactory()
  {
    return rowIngestionMetersFactory;
  }

  public static boolean conditionValid(IndexingServiceCondition condition)
  {
    return conditionValid(condition, 1000);
  }

  public static boolean conditionValid(IndexingServiceCondition condition, long timeout)
  {
    try {
      Stopwatch stopwatch = Stopwatch.createUnstarted();
      stopwatch.start();
      while (!condition.isValid()) {
        Thread.sleep(100);
        if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > timeout) {
          throw new ISE("Condition[%s] not met", condition);
        }
      }
    }
    catch (Exception e) {
      log.warn(e, "Condition[%s] not met within timeout[%,d]", condition, timeout);
      return false;
    }
    return true;
  }

  /**
   * Converts the given JSON string which uses single quotes for field names and
   * String values to a standard JSON by replacing all occurrences of a single
   * quote with double quotes.
   * <p>
   * Single-quoted JSON is typically easier to read as can be seen below:
   * <pre>
   * final String singleQuotedJson = "{'f1':'value', 'f2':5}";
   *
   * final String doubleQuotedJson = "{\"f1\":\"value\", \"f2\":5}";
   * </pre>
   */
  public static String singleQuoteToStandardJson(String singleQuotedJson)
  {
    return StringUtils.replaceChar(singleQuotedJson, '\'', "\"");
  }
}
