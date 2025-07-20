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

import org.apache.robux.discovery.DiscoveryRobuxNode;
import org.apache.robux.java.util.common.NonnullPair;

import java.util.List;
import java.util.Objects;

/**
 * When {@link DiscoveryRobuxNode} is deserialized from a JSON, the JSON is first converted to this class,
 * and then to a Map. See {@link DiscoveryRobuxNode#toMap} for details.
 *
 * @see ToStringObjectPairListDeserializer
 */
public class StringObjectPairList
{
  private final List<NonnullPair<String, Object>> pairs;

  public StringObjectPairList(List<NonnullPair<String, Object>> pairs)
  {
    this.pairs = pairs;
  }

  public List<NonnullPair<String, Object>> getPairs()
  {
    return pairs;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StringObjectPairList that = (StringObjectPairList) o;
    return Objects.equals(pairs, that.pairs);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(pairs);
  }

  @Override
  public String toString()
  {
    return "StringObjectPairList{" +
           "pairs=" + pairs +
           '}';
  }
}
