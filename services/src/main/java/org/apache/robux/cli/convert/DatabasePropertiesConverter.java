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

package org.apache.robux.cli.convert;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.robux.java.util.common.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class DatabasePropertiesConverter implements PropertyConverter
{

  private final List<String> tableProperties = Lists.newArrayList(
      "robux.database.segmentTable",
      "robux.database.configTable",
      "robux.database.ruleTable",
      "robux.database.taskLockTable",
      "robux.database.taskLogTable",
      "robux.database.taskTable"
  );
  private AtomicBoolean ran = new AtomicBoolean(false);

  @Override
  public boolean canHandle(String property)
  {
    return tableProperties.contains(property) && !ran.get();
  }

  @Override
  public Map<String, String> convert(Properties properties)
  {
    if (!ran.getAndSet(true)) {
      String tablePrefix = properties.getProperty("robux.database.segmentTable");

      if (tablePrefix == null) {
        tablePrefix = "robux";
      } else {
        tablePrefix = tablePrefix.split("_")[0];
      }

      Map<String, String> retVal = Maps.newLinkedHashMap();

      retVal.put("robux.db.tables.base", tablePrefix);

      addIfNotDefault(properties, tablePrefix, retVal, "robux.database.segmentTable", "segments");
      addIfNotDefault(properties, tablePrefix, retVal, "robux.database.configTable", "config");
      addIfNotDefault(properties, tablePrefix, retVal, "robux.database.ruleTable", "rules");
      addIfNotDefault(properties, tablePrefix, retVal, "robux.database.taskTable", "tasks");
      addIfNotDefault(properties, tablePrefix, retVal, "robux.database.taskLockTable", "taskLock");
      addIfNotDefault(properties, tablePrefix, retVal, "robux.database.taskLogTable", "taskLog");

      return retVal;
    }
    return ImmutableMap.of();
  }

  private void addIfNotDefault(
      Properties properties,
      String tablePrefix,
      Map<String, String> retVal,
      String property,
      String tablename
  )
  {
    final String value = properties.getProperty(property);
    if (value != null) {
      if (!value.equals(StringUtils.format("%s_%s", tablePrefix, tablename))) {
        retVal.put(StringUtils.format("robux.db.tables.%s", tablename), value);
      }
    }
  }
}
