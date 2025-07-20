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
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 */
public class DataSegmentPusherDefaultConverter implements PropertyConverter
{
  Set<String> propertiesHandled = Sets.newHashSet("robux.pusher.local", "robux.pusher.cassandra", "robux.pusher.hdfs");

  @Override
  public boolean canHandle(String property)
  {
    return propertiesHandled.contains(property) || property.startsWith("robux.pusher.s3");
  }

  @Override
  public Map<String, String> convert(Properties props)
  {
    String type = null;
    if (Boolean.parseBoolean(props.getProperty("robux.pusher.local", "false"))) {
      type = "local";
    } else if (Boolean.parseBoolean(props.getProperty("robux.pusher.cassandra", "false"))) {
      type = "c*";
    } else if (Boolean.parseBoolean(props.getProperty("robux.pusher.hdfs", "false"))) {
      type = "hdfs";
    }

    if (type != null) {
      return ImmutableMap.of("robux.storage.type", type);
    }

    // It's an s3 property, which means we need to set the type and convert the other values.
    Map<String, String> retVal = new HashMap<>();

    retVal.put("robux.pusher.type", type);
    retVal.putAll(new Rename("robux.pusher.s3.bucket", "robux.storage.bucket").convert(props));
    retVal.putAll(new Rename("robux.pusher.s3.baseKey", "robux.storage.baseKey").convert(props));
    retVal.putAll(new Rename("robux.pusher.s3.disableAcl", "robux.storage.disableAcl").convert(props));

    return retVal;
  }
}
