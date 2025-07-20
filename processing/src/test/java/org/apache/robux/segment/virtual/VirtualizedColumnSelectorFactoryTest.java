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

package org.apache.robux.segment.virtual;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.robux.data.input.MapBasedRow;
import org.apache.robux.math.expr.ExprMacroTable;
import org.apache.robux.query.dimension.DefaultDimensionSpec;
import org.apache.robux.segment.RowAdapters;
import org.apache.robux.segment.RowBasedColumnSelectorFactory;
import org.apache.robux.segment.VirtualColumns;
import org.apache.robux.segment.column.ColumnType;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.segment.column.ValueType;
import org.apache.robux.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

public class VirtualizedColumnSelectorFactoryTest extends InitializedNullHandlingTest
{
  private final VirtualizedColumnSelectorFactory selectorFactory = new VirtualizedColumnSelectorFactory(
      RowBasedColumnSelectorFactory.create(
          RowAdapters.standardRow(),
          () -> new MapBasedRow(0L, ImmutableMap.of("x", 10L, "y", 20.0)),
          RowSignature.builder().add("x", ColumnType.LONG).add("y", ColumnType.DOUBLE).build(),
          false,
          false
      ),
      VirtualColumns.create(
          ImmutableList.of(
              new ExpressionVirtualColumn("v0", "x + 1", null, ExprMacroTable.nil()),
              new ExpressionVirtualColumn("v1", "v0 + y", null, ExprMacroTable.nil())
          )
      )
  );

  @Test
  public void test_getColumnCapabilities_type()
  {
    Assert.assertEquals(ValueType.LONG, selectorFactory.getColumnCapabilities("x").getType());
    Assert.assertEquals(ValueType.DOUBLE, selectorFactory.getColumnCapabilities("y").getType());
    Assert.assertEquals(ValueType.LONG, selectorFactory.getColumnCapabilities("v0").getType());
    Assert.assertEquals(ValueType.DOUBLE, selectorFactory.getColumnCapabilities("v1").getType());
    Assert.assertNull(selectorFactory.getColumnCapabilities("nonexistent"));
  }

  @Test
  public void test_makeColumnValueSelector()
  {
    Assert.assertEquals(10, selectorFactory.makeColumnValueSelector("x").getLong());
    Assert.assertEquals(20, selectorFactory.makeColumnValueSelector("y").getDouble(), 0.0);
    Assert.assertEquals(11, selectorFactory.makeColumnValueSelector("v0").getLong());
    Assert.assertEquals(31, selectorFactory.makeColumnValueSelector("v1").getDouble(), 0.0);

    Assert.assertEquals(10L, selectorFactory.makeColumnValueSelector("x").getObject());
    Assert.assertEquals(20.0, selectorFactory.makeColumnValueSelector("y").getObject());
    Assert.assertEquals(11L, selectorFactory.makeColumnValueSelector("v0").getObject());
    Assert.assertEquals(31.0, selectorFactory.makeColumnValueSelector("v1").getObject());

    Assert.assertNull(selectorFactory.makeColumnValueSelector("nonexistent").getObject());
  }

  @Test
  public void test_makeDimensionSelector()
  {
    Assert.assertEquals("10", selectorFactory.makeDimensionSelector(DefaultDimensionSpec.of("x")).getObject());
    Assert.assertEquals("20.0", selectorFactory.makeDimensionSelector(DefaultDimensionSpec.of("y")).getObject());
    Assert.assertEquals("11", selectorFactory.makeDimensionSelector(DefaultDimensionSpec.of("v0")).getObject());
    Assert.assertEquals("31.0", selectorFactory.makeDimensionSelector(DefaultDimensionSpec.of("v1")).getObject());

    Assert.assertNull(selectorFactory.makeDimensionSelector(DefaultDimensionSpec.of("nonexistent")).getObject());
  }
}
