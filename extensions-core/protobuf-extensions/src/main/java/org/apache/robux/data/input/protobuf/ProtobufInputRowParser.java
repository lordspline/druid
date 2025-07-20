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

package org.apache.robux.data.input.protobuf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.robux.data.input.ByteBufferInputRowParser;
import org.apache.robux.data.input.InputRow;
import org.apache.robux.data.input.MapBasedInputRow;
import org.apache.robux.data.input.impl.JSONParseSpec;
import org.apache.robux.data.input.impl.ParseSpec;
import org.apache.robux.data.input.impl.TimestampSpec;
import org.apache.robux.java.util.common.parsers.ParseException;
import org.apache.robux.java.util.common.parsers.Parser;
import org.apache.robux.utils.CollectionUtils;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class ProtobufInputRowParser implements ByteBufferInputRowParser
{
  private final ParseSpec parseSpec;
  // timestamp spec to be used for parsing timestamp
  private final TimestampSpec timestampSpec;
  // whether the spec has any fields to flat
  private final boolean isFlatSpec;
  private final ProtobufBytesDecoder protobufBytesDecoder;
  private Parser<String, Object> parser;
  private final List<String> dimensions;

  @JsonCreator
  public ProtobufInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("protoBytesDecoder") ProtobufBytesDecoder protobufBytesDecoder,
      @Deprecated
      @JsonProperty("descriptor") String descriptorFilePath,
      @Deprecated
      @JsonProperty("protoMessageType") String protoMessageType
  )
  {
    this.parseSpec = parseSpec;
    this.dimensions = parseSpec.getDimensionsSpec().getDimensionNames();
    this.isFlatSpec = parseSpec instanceof JSONParseSpec && ((JSONParseSpec) parseSpec).getFlattenSpec().getFields().isEmpty();
    if (descriptorFilePath != null || protoMessageType != null) {
      this.protobufBytesDecoder = new FileBasedProtobufBytesDecoder(descriptorFilePath, protoMessageType);
    } else {
      this.protobufBytesDecoder = protobufBytesDecoder;
    }
    if (isFlatSpec) {
      this.timestampSpec = new ProtobufInputRowSchema.ProtobufTimestampSpec(parseSpec.getTimestampSpec());
    } else {
      this.timestampSpec = parseSpec.getTimestampSpec();
    }

  }

  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public ProtobufInputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new ProtobufInputRowParser(parseSpec, protobufBytesDecoder, null, null);
  }

  @Override
  public List<InputRow> parseBatch(ByteBuffer input)
  {
    if (parser == null) {
      parser = parseSpec.makeParser();
    }
    Map<String, Object> record;
    DateTime timestamp;

    if (isFlatSpec) {
      try {
        DynamicMessage message = protobufBytesDecoder.parse(input);
        record = CollectionUtils.mapKeys(message.getAllFields(), k -> k.getJsonName());
        timestamp = this.timestampSpec.extractTimestamp(record);
      }
      catch (Exception ex) {
        throw new ParseException(null, ex, "Protobuf message could not be parsed");
      }
    } else {
      try {
        DynamicMessage message = protobufBytesDecoder.parse(input);
        String json = JsonFormat.printer().print(message);
        record = parser.parseToMap(json);
        timestamp = this.timestampSpec.extractTimestamp(record);
      }
      catch (InvalidProtocolBufferException e) {
        throw new ParseException(null, e, "Protobuf message could not be parsed");
      }
    }

    final List<String> dimensions;
    if (!this.dimensions.isEmpty()) {
      dimensions = this.dimensions;
    } else {
      dimensions = Lists.newArrayList(
          Sets.difference(record.keySet(), parseSpec.getDimensionsSpec().getDimensionExclusions())
      );
    }
    return ImmutableList.of(new MapBasedInputRow(timestamp, dimensions, record));
  }
}
