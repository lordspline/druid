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

package org.apache.robux.segment.nested;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.robux.collections.bitmap.RoaringBitmapFactory;
import org.apache.robux.guice.BuiltInTypesModule;
import org.apache.robux.java.util.common.io.Closer;
import org.apache.robux.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.robux.query.DefaultBitmapResultFactory;
import org.apache.robux.query.filter.SelectorPredicateFactory;
import org.apache.robux.query.filter.StringPredicateRobuxPredicateFactory;
import org.apache.robux.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.robux.segment.ColumnValueSelector;
import org.apache.robux.segment.DimensionSelector;
import org.apache.robux.segment.ObjectColumnSelector;
import org.apache.robux.segment.QueryableIndex;
import org.apache.robux.segment.SimpleAscendingOffset;
import org.apache.robux.segment.TestHelper;
import org.apache.robux.segment.column.ColumnHolder;
import org.apache.robux.segment.column.ColumnIndexSupplier;
import org.apache.robux.segment.column.ColumnType;
import org.apache.robux.segment.index.BitmapColumnIndex;
import org.apache.robux.segment.index.semantic.RobuxPredicateIndexes;
import org.apache.robux.segment.index.semantic.NullValueIndex;
import org.apache.robux.segment.index.semantic.StringValueSetIndexes;
import org.apache.robux.testing.InitializedNullHandlingTest;
import org.apache.robux.utils.CompressionUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class NestedDataColumnSupplierV4Test extends InitializedNullHandlingTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();
  private static final String NO_MATCH = "no";
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();
  DefaultBitmapResultFactory resultFactory = new DefaultBitmapResultFactory(new RoaringBitmapFactory());
  List<Map<String, Object>> data = ImmutableList.of(
      TestHelper.makeMap("x", 1L, "y", 1.0, "z", "a", "v", "100", "nullish", "notnull"),
      TestHelper.makeMap("y", 3.0, "z", "d", "v", 1000L, "nullish", null),
      TestHelper.makeMap("x", 5L, "y", 5.0, "z", "b", "nullish", ""),
      TestHelper.makeMap("x", 3L, "y", 4.0, "z", "c", "v", 3000.333, "nullish", "null"),
      TestHelper.makeMap("x", 2L, "v", "40000"),
      TestHelper.makeMap("x", 4L, "y", 2.0, "z", "e", "v", 11111L, "nullish", null)
  );

  List<Map<String, Object>> arrayTestData = ImmutableList.of(
      TestHelper.makeMap("s", new Object[]{"a", "b", "c"}, "l", new Object[]{1L, 2L, 3L}, "d", new Object[]{1.1, 2.2}),
      TestHelper.makeMap(
          "s",
          new Object[]{null, "b", "c"},
          "l",
          new Object[]{1L, null, 3L},
          "d",
          new Object[]{2.2, 2.2}
      ),
      TestHelper.makeMap(
          "s",
          new Object[]{"b", "c"},
          "l",
          new Object[]{null, null},
          "d",
          new Object[]{1.1, null, 2.2}
      ),
      TestHelper.makeMap("s", new Object[]{"a", "b", "c", "d"}, "l", new Object[]{4L, 2L, 3L}),
      TestHelper.makeMap("s", new Object[]{"d", "b", "c", "a"}, "d", new Object[]{1.1, 2.2}),
      TestHelper.makeMap("l", new Object[]{1L, 2L, 3L}, "d", new Object[]{3.1, 2.2, 1.9})
  );

  Closer closer = Closer.create();

  SmooshedFileMapper fileMapper;

  ByteBuffer baseBuffer;

  SmooshedFileMapper arrayFileMapper;

  ByteBuffer arrayBaseBuffer;

  @BeforeClass
  public static void staticSetup()
  {
    BuiltInTypesModule.registerHandlersAndSerde();
  }

  @After
  public void teardown() throws IOException
  {
    closer.close();
  }

  @Test
  public void testLegacyV3ReaderFormat() throws IOException
  {
    String columnName = "shipTo";
    String firstValue = "Cole";
    File tmpLocation = tempFolder.newFolder();
    CompressionUtils.unzip(
        NestedDataColumnSupplierV4Test.class.getClassLoader().getResourceAsStream("nested_segment_v3/index.zip"),
        tmpLocation
    );
    try (Closer closer = Closer.create()) {
      QueryableIndex theIndex = closer.register(TestHelper.getTestIndexIO().loadIndex(tmpLocation));
      ColumnHolder holder = theIndex.getColumnHolder(columnName);
      Assert.assertNotNull(holder);
      Assert.assertEquals(ColumnType.NESTED_DATA, holder.getCapabilities().toColumnType());
      NestedDataColumnV3<?> v3 = closer.register((NestedDataColumnV3<?>) holder.getColumn());
      Assert.assertNotNull(v3);
      List<NestedPathPart> path = ImmutableList.of(new NestedPathField("lastName"));
      ColumnHolder nestedColumnHolder = v3.getColumnHolder(path);
      Assert.assertNotNull(nestedColumnHolder);
      Assert.assertEquals(ColumnType.STRING, nestedColumnHolder.getCapabilities().toColumnType());
      NestedFieldDictionaryEncodedColumn<?> nestedColumn =
          (NestedFieldDictionaryEncodedColumn<?>) nestedColumnHolder.getColumn();
      Assert.assertNotNull(nestedColumn);
      ColumnValueSelector<?> selector = nestedColumn.makeColumnValueSelector(
          new SimpleAscendingOffset(theIndex.getNumRows())
      );
      ColumnIndexSupplier indexSupplier = v3.getColumnIndexSupplier(path);
      Assert.assertNotNull(indexSupplier);
      StringValueSetIndexes valueSetIndex = indexSupplier.as(StringValueSetIndexes.class);
      Assert.assertNotNull(valueSetIndex);
      BitmapColumnIndex indexForValue = valueSetIndex.forValue(firstValue);
      Assert.assertEquals(firstValue, selector.getObject());
      Assert.assertTrue(indexForValue.computeBitmapResult(resultFactory, false).get(0));
    }
  }
  @Test
  public void testLegacyV4ReaderFormat() throws IOException
  {
    String columnName = "shipTo";
    // i accidentally didn't use same segment granularity for v3 and v4 segments... so they have different first value
    String firstValue = "Beatty";
    File tmpLocation = tempFolder.newFolder();
    CompressionUtils.unzip(
        NestedDataColumnSupplierV4Test.class.getClassLoader().getResourceAsStream("nested_segment_v4/index.zip"),
        tmpLocation
    );
    try (Closer closer = Closer.create()) {
      QueryableIndex theIndex = closer.register(TestHelper.getTestIndexIO().loadIndex(tmpLocation));
      ColumnHolder holder = theIndex.getColumnHolder(columnName);
      Assert.assertNotNull(holder);
      Assert.assertEquals(ColumnType.NESTED_DATA, holder.getCapabilities().toColumnType());
      NestedDataColumnV4<?> v4 = closer.register((NestedDataColumnV4<?>) holder.getColumn());
      Assert.assertNotNull(v4);
      List<NestedPathPart> path = ImmutableList.of(new NestedPathField("lastName"));
      ColumnHolder nestedColumnHolder = v4.getColumnHolder(path);
      Assert.assertNotNull(nestedColumnHolder);
      Assert.assertEquals(ColumnType.STRING, nestedColumnHolder.getCapabilities().toColumnType());
      NestedFieldDictionaryEncodedColumn<?> nestedColumn =
          (NestedFieldDictionaryEncodedColumn<?>) nestedColumnHolder.getColumn();
      Assert.assertNotNull(nestedColumn);
      ColumnValueSelector<?> selector = nestedColumn.makeColumnValueSelector(
          new SimpleAscendingOffset(theIndex.getNumRows())
      );
      ColumnIndexSupplier indexSupplier = v4.getColumnIndexSupplier(path);
      Assert.assertNotNull(indexSupplier);
      StringValueSetIndexes valueSetIndex = indexSupplier.as(StringValueSetIndexes.class);
      Assert.assertNotNull(valueSetIndex);
      BitmapColumnIndex indexForValue = valueSetIndex.forValue(firstValue);
      Assert.assertEquals(firstValue, selector.getObject());
      Assert.assertTrue(indexForValue.computeBitmapResult(resultFactory, false).get(0));
    }
  }

  private void smokeTest(NestedDataComplexColumn column) throws IOException
  {
    SimpleAscendingOffset offset = new SimpleAscendingOffset(data.size());
    ColumnValueSelector<?> rawSelector = column.makeColumnValueSelector(offset);
    final List<NestedPathPart> xPath = NestedPathFinder.parseJsonPath("$.x");
    Assert.assertEquals(ImmutableSet.of(ColumnType.LONG), column.getFieldTypes(xPath));
    Assert.assertEquals(ColumnType.LONG, column.getColumnHolder(xPath).getCapabilities().toColumnType());
    ColumnValueSelector<?> xSelector = column.makeColumnValueSelector(xPath, offset);
    DimensionSelector xDimSelector = column.makeDimensionSelector(xPath, offset, null);
    ColumnIndexSupplier xIndexSupplier = column.getColumnIndexSupplier(xPath);
    Assert.assertNotNull(xIndexSupplier);
    StringValueSetIndexes xValueIndex = xIndexSupplier.as(StringValueSetIndexes.class);
    RobuxPredicateIndexes xPredicateIndex = xIndexSupplier.as(RobuxPredicateIndexes.class);
    NullValueIndex xNulls = xIndexSupplier.as(NullValueIndex.class);
    final List<NestedPathPart> yPath = NestedPathFinder.parseJsonPath("$.y");
    Assert.assertEquals(ImmutableSet.of(ColumnType.DOUBLE), column.getFieldTypes(yPath));
    Assert.assertEquals(ColumnType.DOUBLE, column.getColumnHolder(yPath).getCapabilities().toColumnType());
    ColumnValueSelector<?> ySelector = column.makeColumnValueSelector(yPath, offset);
    DimensionSelector yDimSelector = column.makeDimensionSelector(yPath, offset, null);
    ColumnIndexSupplier yIndexSupplier = column.getColumnIndexSupplier(yPath);
    Assert.assertNotNull(yIndexSupplier);
    StringValueSetIndexes yValueIndex = yIndexSupplier.as(StringValueSetIndexes.class);
    RobuxPredicateIndexes yPredicateIndex = yIndexSupplier.as(RobuxPredicateIndexes.class);
    NullValueIndex yNulls = yIndexSupplier.as(NullValueIndex.class);
    final List<NestedPathPart> zPath = NestedPathFinder.parseJsonPath("$.z");
    Assert.assertEquals(ImmutableSet.of(ColumnType.STRING), column.getFieldTypes(zPath));
    Assert.assertEquals(ColumnType.STRING, column.getColumnHolder(zPath).getCapabilities().toColumnType());
    ColumnValueSelector<?> zSelector = column.makeColumnValueSelector(zPath, offset);
    DimensionSelector zDimSelector = column.makeDimensionSelector(zPath, offset, null);
    ColumnIndexSupplier zIndexSupplier = column.getColumnIndexSupplier(zPath);
    Assert.assertNotNull(zIndexSupplier);
    StringValueSetIndexes zValueIndex = zIndexSupplier.as(StringValueSetIndexes.class);
    RobuxPredicateIndexes zPredicateIndex = zIndexSupplier.as(RobuxPredicateIndexes.class);
    NullValueIndex zNulls = zIndexSupplier.as(NullValueIndex.class);
    final List<NestedPathPart> vPath = NestedPathFinder.parseJsonPath("$.v");
    Assert.assertEquals(
        ImmutableSet.of(ColumnType.STRING, ColumnType.LONG, ColumnType.DOUBLE),
        column.getFieldTypes(vPath)
    );
    Assert.assertEquals(ColumnType.STRING, column.getColumnHolder(vPath).getCapabilities().toColumnType());
    ColumnValueSelector<?> vSelector = column.makeColumnValueSelector(vPath, offset);
    DimensionSelector vDimSelector = column.makeDimensionSelector(vPath, offset, null);
    ColumnIndexSupplier vIndexSupplier = column.getColumnIndexSupplier(vPath);
    Assert.assertNotNull(vIndexSupplier);
    StringValueSetIndexes vValueIndex = vIndexSupplier.as(StringValueSetIndexes.class);
    RobuxPredicateIndexes vPredicateIndex = vIndexSupplier.as(RobuxPredicateIndexes.class);
    NullValueIndex vNulls = vIndexSupplier.as(NullValueIndex.class);
    final List<NestedPathPart> nullishPath = NestedPathFinder.parseJsonPath("$.nullish");
    Assert.assertEquals(ImmutableSet.of(ColumnType.STRING), column.getFieldTypes(nullishPath));
    Assert.assertEquals(ColumnType.STRING, column.getColumnHolder(nullishPath).getCapabilities().toColumnType());
    ColumnValueSelector<?> nullishSelector = column.makeColumnValueSelector(nullishPath, offset);
    DimensionSelector nullishDimSelector = column.makeDimensionSelector(nullishPath, offset, null);
    ColumnIndexSupplier nullishIndexSupplier = column.getColumnIndexSupplier(nullishPath);
    Assert.assertNotNull(nullishIndexSupplier);
    StringValueSetIndexes nullishValueIndex = nullishIndexSupplier.as(StringValueSetIndexes.class);
    RobuxPredicateIndexes nullishPredicateIndex = nullishIndexSupplier.as(RobuxPredicateIndexes.class);
    NullValueIndex nullishNulls = nullishIndexSupplier.as(NullValueIndex.class);
    Assert.assertEquals(ImmutableList.of(nullishPath, vPath, xPath, yPath, zPath), column.getNestedFields());
    for (int i = 0; i < data.size(); i++) {
      Map row = data.get(i);
      Assert.assertEquals(
          JSON_MAPPER.writeValueAsString(row),
          JSON_MAPPER.writeValueAsString(StructuredData.unwrap(rawSelector.getObject()))
      );
      testPath(row, i, "v", vSelector, vDimSelector, vValueIndex, vPredicateIndex, vNulls, null);
      testPath(row, i, "x", xSelector, xDimSelector, xValueIndex, xPredicateIndex, xNulls, ColumnType.LONG);
      testPath(row, i, "y", ySelector, yDimSelector, yValueIndex, yPredicateIndex, yNulls, ColumnType.DOUBLE);
      testPath(row, i, "z", zSelector, zDimSelector, zValueIndex, zPredicateIndex, zNulls, ColumnType.STRING);
      testPath(
          row,
          i,
          "nullish",
          nullishSelector,
          nullishDimSelector,
          nullishValueIndex,
          nullishPredicateIndex,
          nullishNulls,
          ColumnType.STRING
      );
      offset.increment();
    }
  }

  private void testPath(
      Map row,
      int rowNumber,
      String path,
      ColumnValueSelector<?> valueSelector,
      DimensionSelector dimSelector,
      StringValueSetIndexes valueSetIndex,
      RobuxPredicateIndexes predicateIndex,
      NullValueIndex nullValueIndex,
      @Nullable ColumnType singleType
  )
  {
    final Object inputValue = row.get(path);
    if (row.containsKey(path) && inputValue != null) {
      Assert.assertEquals(inputValue, valueSelector.getObject());
      if (ColumnType.LONG.equals(singleType)) {
        Assert.assertEquals(inputValue, valueSelector.getLong());
        Assert.assertFalse(path + " is not null", valueSelector.isNull());
      } else if (ColumnType.DOUBLE.equals(singleType)) {
        Assert.assertEquals((double) inputValue, valueSelector.getDouble(), 0.0);
        Assert.assertFalse(path + " is not null", valueSelector.isNull());
      }
      final String theString = String.valueOf(inputValue);
      Assert.assertEquals(theString, dimSelector.getObject());
      String dimSelectorLookupVal = dimSelector.lookupName(dimSelector.getRow().get(0));
      Assert.assertEquals(theString, dimSelectorLookupVal);
      Assert.assertEquals(dimSelector.idLookup().lookupId(dimSelectorLookupVal), dimSelector.getRow().get(0));
      Assert.assertTrue(valueSetIndex.forValue(theString).computeBitmapResult(resultFactory, false).get(rowNumber));
      Assert.assertTrue(valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of(theString)))
                                     .computeBitmapResult(resultFactory, false)
                                     .get(rowNumber));
      Assert.assertTrue(predicateIndex.forPredicate(new SelectorPredicateFactory(theString))
                                      .computeBitmapResult(resultFactory, false)
                                      .get(rowNumber));
      Assert.assertFalse(valueSetIndex.forValue(NO_MATCH).computeBitmapResult(resultFactory, false).get(rowNumber));
      Assert.assertFalse(valueSetIndex.forSortedValues(new TreeSet<>(ImmutableSet.of(NO_MATCH)))
                                      .computeBitmapResult(resultFactory, false)
                                      .get(rowNumber));
      Assert.assertFalse(predicateIndex.forPredicate(new SelectorPredicateFactory(NO_MATCH))
                                       .computeBitmapResult(resultFactory, false)
                                       .get(rowNumber));
      Assert.assertFalse(nullValueIndex.get().computeBitmapResult(resultFactory, false).get(rowNumber));
      Assert.assertTrue(dimSelector.makeValueMatcher(theString).matches(false));
      Assert.assertFalse(dimSelector.makeValueMatcher(NO_MATCH).matches(false));
      Assert.assertTrue(dimSelector.makeValueMatcher(StringPredicateRobuxPredicateFactory.equalTo(theString)).matches(false));
      Assert.assertFalse(dimSelector.makeValueMatcher(StringPredicateRobuxPredicateFactory.equalTo(NO_MATCH)).matches(false));
    } else {
      Assert.assertNull(valueSelector.getObject());
      Assert.assertTrue(path, valueSelector.isNull());
      Assert.assertEquals(0, dimSelector.getRow().get(0));
      Assert.assertNull(dimSelector.getObject());
      Assert.assertNull(dimSelector.lookupName(dimSelector.getRow().get(0)));
      Assert.assertTrue(valueSetIndex.forValue(null).computeBitmapResult(resultFactory, false).get(rowNumber));
      Assert.assertTrue(nullValueIndex.get().computeBitmapResult(resultFactory, false).get(rowNumber));
      Assert.assertTrue(predicateIndex.forPredicate(new SelectorPredicateFactory(null))
                                      .computeBitmapResult(resultFactory, false)
                                      .get(rowNumber));
      Assert.assertFalse(valueSetIndex.forValue(NO_MATCH).computeBitmapResult(resultFactory, false).get(rowNumber));
      Assert.assertFalse(valueSetIndex.forValue(NO_MATCH).computeBitmapResult(resultFactory, false).get(rowNumber));
      Assert.assertFalse(predicateIndex.forPredicate(new SelectorPredicateFactory(NO_MATCH))
                                       .computeBitmapResult(resultFactory, false)
                                       .get(rowNumber));
      Assert.assertTrue(dimSelector.makeValueMatcher((String) null).matches(false));
      Assert.assertFalse(dimSelector.makeValueMatcher(NO_MATCH).matches(false));
      Assert.assertTrue(dimSelector.makeValueMatcher(StringPredicateRobuxPredicateFactory.equalTo(null)).matches(false));
      Assert.assertFalse(dimSelector.makeValueMatcher(StringPredicateRobuxPredicateFactory.equalTo(NO_MATCH)).matches(false));
    }
  }

  private static class SettableSelector extends ObjectColumnSelector<StructuredData>
  {
    private StructuredData data;

    public void setObject(StructuredData o)
    {
      this.data = o;
    }

    @Nullable
    @Override
    public StructuredData getObject()
    {
      return data;
    }

    @Override
    public Class classOfObject()
    {
      return StructuredData.class;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {

    }
  }
}
