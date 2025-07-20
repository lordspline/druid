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

package org.apache.robux.jackson;

import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import org.apache.robux.discovery.RobuxService;

/**
 * A modifier to use a custom serializer for {@link RobuxService}.
 * See {@link RobuxServiceSerializer} for details.
 */
public class RobuxServiceSerializerModifier extends BeanSerializerModifier
{
  @Override
  public JsonSerializer<?> modifySerializer(
      SerializationConfig config,
      BeanDescription beanDesc,
      JsonSerializer<?> serializer
  )
  {
    if (RobuxService.class.isAssignableFrom(beanDesc.getBeanClass())) {
      return new RobuxServiceSerializer((JsonSerializer<Object>) serializer);
    }

    return serializer;
  }
}
