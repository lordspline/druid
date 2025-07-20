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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.discovery.RobuxService;
import org.apache.robux.java.util.common.NonnullPair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * When {@link DiscoveryRobuxNode} is deserialized from a JSON,
 * the JSON is first converted to {@link StringObjectPairList}, and then to a Map.
 * See {@link DiscoveryRobuxNode#toMap} for details.
 */
public class ToStringObjectPairListDeserializer extends StdDeserializer<StringObjectPairList>
{
  public ToStringObjectPairListDeserializer()
  {
    super(StringObjectPairList.class);
  }

  @Override
  public StringObjectPairList deserialize(JsonParser parser, DeserializationContext ctx) throws IOException
  {
    if (parser.currentToken() != JsonToken.START_OBJECT) {
      throw ctx.wrongTokenException(parser, RobuxService.class, JsonToken.START_OBJECT, null);
    }

    final List<NonnullPair<String, Object>> pairs = new ArrayList<>();

    parser.nextToken();

    while (parser.currentToken() == JsonToken.FIELD_NAME) {
      final String key = parser.getText();
      parser.nextToken();
      final Object val = ctx.readValue(parser, Object.class);
      pairs.add(new NonnullPair<>(key, val));

      parser.nextToken();
    }

    if (parser.currentToken() != JsonToken.END_OBJECT) {
      throw ctx.wrongTokenException(parser, RobuxService.class, JsonToken.END_OBJECT, null);
    }

    return new StringObjectPairList(pairs);
  }
}
