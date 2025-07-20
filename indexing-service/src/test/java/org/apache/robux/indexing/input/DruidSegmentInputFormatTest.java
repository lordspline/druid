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

package org.apache.robux.indexing.input;

import com.google.common.collect.ImmutableList;
import org.apache.robux.data.input.ColumnsFilter;
import org.apache.robux.data.input.InputEntityReader;
import org.apache.robux.data.input.InputRowSchema;
import org.apache.robux.data.input.impl.DimensionsSpec;
import org.apache.robux.data.input.impl.FileEntity;
import org.apache.robux.data.input.impl.TimestampSpec;
import org.apache.robux.java.util.common.Intervals;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertThrows;


public class RobuxSegmentInputFormatTest
{

  private static final InputRowSchema INPUT_ROW_SCHEMA = new InputRowSchema(
      new TimestampSpec("ts", "auto", null),
      new DimensionsSpec(DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "name"))),
      ColumnsFilter.all()
  );


  @Test
  public void testRobuxSegmentInputEntityReader()
  {
    RobuxSegmentInputFormat format = new RobuxSegmentInputFormat(null, null);
    InputEntityReader reader = format.createReader(
        INPUT_ROW_SCHEMA,
        RobuxSegmentReaderTest.makeInputEntity(Intervals.of("2000/P1D"), null, ImmutableList.of("s", "d"), ImmutableList.of("cnt", "met_s")),
        null
    );
    Assert.assertTrue(reader instanceof RobuxSegmentReader);
  }


  @Test
  public void testRobuxTombstoneSegmentInputEntityReader()
  {
    RobuxSegmentInputFormat format = new RobuxSegmentInputFormat(null, null);
    InputEntityReader reader = format.createReader(
        INPUT_ROW_SCHEMA,
        RobuxSegmentReaderTest.makeTombstoneInputEntity(Intervals.of("2000/P1D")),
        null
    );
    Assert.assertTrue(reader instanceof RobuxTombstoneSegmentReader);
  }

  @Test
  public void testRobuxSegmentInputEntityReaderBadEntity()
  {
    RobuxSegmentInputFormat format = new RobuxSegmentInputFormat(null, null);
    Exception exception = assertThrows(IllegalArgumentException.class, () -> {
      format.createReader(
          INPUT_ROW_SCHEMA,
          new FileEntity(null),
          null
      );
    });
    String expectedMessage =
        "org.apache.robux.indexing.input.RobuxSegmentInputEntity required, but org.apache.robux.data.input.impl.FileEntity provided.";
    String actualMessage = exception.getMessage();
    Assert.assertEquals(expectedMessage, actualMessage);
  }
}
