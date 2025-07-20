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

package org.apache.robux.query.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.robux.jackson.DefaultObjectMapper;
import org.apache.robux.query.extraction.SubstringDimExtractionFn;
import org.apache.robux.segment.column.ColumnIndexSupplier;
import org.apache.robux.segment.index.BitmapColumnIndex;
import org.apache.robux.segment.index.semantic.LexicographicalRangeIndexes;
import org.apache.robux.segment.index.semantic.StringValueSetIndexes;
import org.apache.robux.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.util.Arrays;

public class LikeDimFilterTest extends InitializedNullHandlingTest
{
  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    final DimFilter filter = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter2 = objectMapper.readValue(objectMapper.writeValueAsString(filter), DimFilter.class);
    Assert.assertEquals(filter, filter2);
  }

  @Test
  public void testGetCacheKey()
  {
    final DimFilter filter = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter2 = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter3 = new LikeDimFilter("foo", "bar%", null, new SubstringDimExtractionFn(1, 2));
    Assert.assertArrayEquals(filter.getCacheKey(), filter2.getCacheKey());
    Assert.assertFalse(Arrays.equals(filter.getCacheKey(), filter3.getCacheKey()));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    final DimFilter filter = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter2 = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    final DimFilter filter3 = new LikeDimFilter("foo", "bar%", null, new SubstringDimExtractionFn(1, 2));
    Assert.assertEquals(filter, filter2);
    Assert.assertNotEquals(filter, filter3);
    Assert.assertEquals(filter.hashCode(), filter2.hashCode());
    Assert.assertNotEquals(filter.hashCode(), filter3.hashCode());
  }

  @Test
  public void testGetRequiredColumns()
  {
    final DimFilter filter = new LikeDimFilter("foo", "bar%", "@", new SubstringDimExtractionFn(1, 2));
    Assert.assertEquals(filter.getRequiredColumns(), Sets.newHashSet("foo"));
  }

  @Test
  public void testEqualsContractForExtractionFnRobuxPredicateFactory()
  {
    EqualsVerifier.forClass(LikeDimFilter.LikeMatcher.PatternRobuxPredicateFactory.class)
                  .withNonnullFields("pattern")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void test_LikeMatcher_equals()
  {
    EqualsVerifier.forClass(LikeDimFilter.LikeMatcher.class)
                  .usingGetClass()
                  .withNonnullFields("suffixMatch", "prefix", "pattern")
                  .withIgnoredFields("likePattern")
                  .verify();
  }

  @Test
  public void testPrefixMatchUsesRangeIndex()
  {
    // An implementation test.
    // This test confirms that "like" filters with prefix matchers use index-range lookups without matcher predicates.

    final Filter likeFilter = new LikeDimFilter("dim0", "f%", null, null, null).toFilter();

    final ColumnIndexSelector indexSelector = Mockito.mock(ColumnIndexSelector.class);
    final ColumnIndexSupplier indexSupplier = Mockito.mock(ColumnIndexSupplier.class);
    final LexicographicalRangeIndexes rangeIndex = Mockito.mock(LexicographicalRangeIndexes.class);
    final BitmapColumnIndex bitmapColumnIndex = Mockito.mock(BitmapColumnIndex.class);

    Mockito.when(indexSelector.getIndexSupplier("dim0")).thenReturn(indexSupplier);
    Mockito.when(indexSupplier.as(LexicographicalRangeIndexes.class)).thenReturn(rangeIndex);
    Mockito.when(
        // Verify that likeFilter uses forRange without a matcher predicate; it's unnecessary and slows things down
        rangeIndex.forRange("f", false, "f" + Character.MAX_VALUE, false)
    ).thenReturn(bitmapColumnIndex);

    final BitmapColumnIndex retVal = likeFilter.getBitmapColumnIndex(indexSelector);
    Assert.assertSame("likeFilter returns the intended bitmapColumnIndex", bitmapColumnIndex, retVal);
  }

  @Test
  public void testExactMatchUsesValueIndex()
  {
    // An implementation test.
    // This test confirms that "like" filters with exact matchers use index lookups.

    final Filter likeFilter = new LikeDimFilter("dim0", "f", null, null, null).toFilter();

    final ColumnIndexSelector indexSelector = Mockito.mock(ColumnIndexSelector.class);
    final ColumnIndexSupplier indexSupplier = Mockito.mock(ColumnIndexSupplier.class);
    final StringValueSetIndexes valueIndex = Mockito.mock(StringValueSetIndexes.class);
    final BitmapColumnIndex bitmapColumnIndex = Mockito.mock(BitmapColumnIndex.class);

    Mockito.when(indexSelector.getIndexSupplier("dim0")).thenReturn(indexSupplier);
    Mockito.when(indexSupplier.as(StringValueSetIndexes.class)).thenReturn(valueIndex);
    Mockito.when(valueIndex.forValue("f")).thenReturn(bitmapColumnIndex);

    final BitmapColumnIndex retVal = likeFilter.getBitmapColumnIndex(indexSelector);
    Assert.assertSame("likeFilter returns the intended bitmapColumnIndex", bitmapColumnIndex, retVal);
  }

  @Test
  public void testPatternCompilation()
  {
    assertCompilation("", ":[^$]");
    assertCompilation("a", "a:[^a$]");
    assertCompilation("abc", "abc:[^abc$]");
    assertCompilation("a%", "a:[^a]");
    assertCompilation("%a", ":[a$]");
    assertCompilation("%a%", ":[a]");
    assertCompilation("%_a", ":[.a$]");
    assertCompilation("_%a", ":[^., a$]");
    assertCompilation("_%_a", ":[^., .a$]");
    assertCompilation("abc%", "abc:[^abc]");
    assertCompilation("a%b", "a:[^a, b$]");
    assertCompilation("abc%x", "abc:[^abc, x$]");
    assertCompilation("abc%xyz", "abc:[^abc, xyz$]");
    assertCompilation("____", ":[^....$]");
    assertCompilation("%%%%", ":[]");
    assertCompilation("%_%_%%__", ":[., ., ..$]");
    assertCompilation("%_%a_%bc%_d_", ":[., a., bc, .d.$]");
    assertCompilation("%1 _ 5%6", ":[1 . 5, 6$]");
    assertCompilation("\\%_%a_\\%b\\\\c\\___%_%_d_w%x_y_z", "%:[^\\u0025., a.\\u0025b\\u005Cc_.., ., .d.w, x.y.z$]");
  }

  @Test
  public void testPatternEmpty()
  {
    assertMatch("", null, RobuxPredicateMatch.UNKNOWN);
    assertMatch("", "", RobuxPredicateMatch.TRUE);
    assertMatch("", "a", RobuxPredicateMatch.FALSE);
    assertMatch("", "This is a test!", RobuxPredicateMatch.FALSE);
  }

  @Test
  public void testPatternExactMatch()
  {
    assertMatch("a\nb", "a\nb", RobuxPredicateMatch.TRUE);
    assertMatch("a\nb", "a\nc", RobuxPredicateMatch.FALSE);
    assertMatch("This is a test", "This is a test", RobuxPredicateMatch.TRUE);
    assertMatch("This is a test", "this is a test", RobuxPredicateMatch.FALSE);
    assertMatch("This is a test", "This is a tes", RobuxPredicateMatch.FALSE);
    assertMatch("This is a test", "his is a test", RobuxPredicateMatch.FALSE);
    assertMatch("This \\%is a\\_test", "This %is a_test", RobuxPredicateMatch.TRUE);
    assertMatch("This \\%is a\\_test", "This \\%is a_test", RobuxPredicateMatch.FALSE);
  }

  @Test
  public void testPatternTrickySuffixes()
  {
    assertMatch("%xyz", "abcxyzxyz", RobuxPredicateMatch.TRUE);
    assertMatch("ab%bc", "abc", RobuxPredicateMatch.FALSE);
  }

  @Test
  public void testPatternOnlySpecial()
  {
    assertMatch("%", null, RobuxPredicateMatch.UNKNOWN);
    assertMatch("%", "", RobuxPredicateMatch.TRUE);
    assertMatch("%", "abcxyzxyz", RobuxPredicateMatch.TRUE);
    assertMatch("_", null, RobuxPredicateMatch.UNKNOWN);
    assertMatch("_", "", RobuxPredicateMatch.FALSE);
    assertMatch("_", "a", RobuxPredicateMatch.TRUE);
    assertMatch("_", "ab", RobuxPredicateMatch.FALSE);
    assertMatch("____", "abc", RobuxPredicateMatch.FALSE);
    assertMatch("____", "abcd", RobuxPredicateMatch.TRUE);
    assertMatch("____", "abcde", RobuxPredicateMatch.FALSE);
    assertMatch("%____", "abcde", RobuxPredicateMatch.TRUE);
    assertMatch("%____", "abcd", RobuxPredicateMatch.TRUE);
    assertMatch("%____", "abc", RobuxPredicateMatch.FALSE);
    assertMatch("__%_%%_", "abc", RobuxPredicateMatch.FALSE);
    assertMatch("__%_%%_", "abcd", RobuxPredicateMatch.TRUE);
    assertMatch("__%_%%_", "abcdxyz", RobuxPredicateMatch.TRUE);
    assertMatch("%__%_%%_%", "abc", RobuxPredicateMatch.FALSE);
    assertMatch("%__%_%%_%", "abcd", RobuxPredicateMatch.TRUE);
    assertMatch("%__%_%%_%", "abcdxyz", RobuxPredicateMatch.TRUE);
  }

  @Test
  public void testPatternTrailingWildcard()
  {
    assertMatch("ab%", "abc", RobuxPredicateMatch.TRUE);
    assertMatch("ab%", "ab", RobuxPredicateMatch.TRUE);
    assertMatch("ab%", "a", RobuxPredicateMatch.FALSE);
  }

  @Test
  public void testPatternLeadingWildcard()
  {
    assertMatch("%yz", "xyz", RobuxPredicateMatch.TRUE);
    assertMatch("%yz", "yz", RobuxPredicateMatch.TRUE);
    assertMatch("%yz", "z", RobuxPredicateMatch.FALSE);
    assertMatch("%yz", "wxyz", RobuxPredicateMatch.TRUE);
    assertMatch("%yz", "xyza", RobuxPredicateMatch.FALSE);
  }

  @Test
  public void testPatternTrailingAny()
  {
    assertMatch("ab_", "abc", RobuxPredicateMatch.TRUE);
    assertMatch("ab_", "ab", RobuxPredicateMatch.FALSE);
    assertMatch("ab_", "abcd", RobuxPredicateMatch.FALSE);
    assertMatch("ab_", "xabc", RobuxPredicateMatch.FALSE);
  }

  @Test
  public void testPatternLeadingAny()
  {
    assertMatch("_yz", "xyz", RobuxPredicateMatch.TRUE);
    assertMatch("_yz", "yz", RobuxPredicateMatch.FALSE);
    assertMatch("_yz", "wxyz", RobuxPredicateMatch.FALSE);
    assertMatch("_yz", "xyza", RobuxPredicateMatch.FALSE);
  }

  @Test
  public void testPatternLeadingAndTrailing()
  {
    assertMatch("_jkl_", "jkl", RobuxPredicateMatch.FALSE);
    assertMatch("_jkl_", "ijklm", RobuxPredicateMatch.TRUE);
    assertMatch("_jkl_", "ijklmn", RobuxPredicateMatch.FALSE);
    assertMatch("_jkl_", "hijklm", RobuxPredicateMatch.FALSE);
    assertMatch("%jkl%", "jkl", RobuxPredicateMatch.TRUE);
    assertMatch("%jkl%", "ijklm", RobuxPredicateMatch.TRUE);
    assertMatch("%jkl%", "ijklmn", RobuxPredicateMatch.TRUE);
    assertMatch("%jkl%", "hijklm", RobuxPredicateMatch.TRUE);
    assertMatch("_jkl%", "jkl", RobuxPredicateMatch.FALSE);
    assertMatch("_jkl%", "ijklm", RobuxPredicateMatch.TRUE);
    assertMatch("_jkl%", "ijklmn", RobuxPredicateMatch.TRUE);
    assertMatch("_jkl%", "hijklm", RobuxPredicateMatch.FALSE);
    assertMatch("_jkl%", "hijklmn", RobuxPredicateMatch.FALSE);
    assertMatch("%jkl_", "jkl", RobuxPredicateMatch.FALSE);
    assertMatch("%jkl_", "ijklm", RobuxPredicateMatch.TRUE);
    assertMatch("%jkl_", "ijklmn", RobuxPredicateMatch.FALSE);
    assertMatch("%jkl_", "hijklm", RobuxPredicateMatch.TRUE);
    assertMatch("%jkl_", "hijklmn", RobuxPredicateMatch.FALSE);
  }

  @Test
  public void testPatternSuffixWithManyParts()
  {
    assertMatch("%ba_", "foo bar", RobuxPredicateMatch.TRUE);
    assertMatch("%ba_", "foo bar daz", RobuxPredicateMatch.FALSE);
    assertMatch("%ba_%", "foo bar baz", RobuxPredicateMatch.TRUE);
    assertMatch("a%b_d_", "abcde", RobuxPredicateMatch.TRUE);
    assertMatch("a%b_d_", "abcdexyzbcde", RobuxPredicateMatch.TRUE);
    assertMatch("%b_d_", "abcde", RobuxPredicateMatch.TRUE);
    assertMatch("%b_d_", "abcdexyzbcde", RobuxPredicateMatch.TRUE);
    assertMatch("%b_d_", "abcdexyzbcdef", RobuxPredicateMatch.FALSE);
    assertMatch("%b_d_", "abcdexyzbcd", RobuxPredicateMatch.FALSE);
    assertMatch("%z%_b_d_", "abcdexyzabcde", RobuxPredicateMatch.TRUE);
    assertMatch("%z%_b_d_", "abcdexyzbcde", RobuxPredicateMatch.FALSE);
    assertMatch("%z%_b_d_", "abcdexybcde", RobuxPredicateMatch.FALSE);
    assertMatch("%z%_b_d_", "abcdexbcde", RobuxPredicateMatch.FALSE);
  }

  @Test
  public void testPatternNoWildcards()
  {
    assertMatch("a_c_e_", "abcdef", RobuxPredicateMatch.TRUE);
    assertMatch("a_c_e_", "abcde", RobuxPredicateMatch.FALSE);
    assertMatch("x_c_e_", "abcdef", RobuxPredicateMatch.FALSE);
    assertMatch("xa_c_e_", "abcdef", RobuxPredicateMatch.FALSE);
    assertMatch("a_c_e_x", "abcde", RobuxPredicateMatch.FALSE);
  }

  @Test
  public void testPatternFindsCorrectMiddleMatch()
  {
    assertMatch("%km%z", "akmz", RobuxPredicateMatch.TRUE);
    assertMatch("%km%z", "akkmz", RobuxPredicateMatch.TRUE);
    assertMatch("%xy%yz", "xyz", RobuxPredicateMatch.FALSE);
    assertMatch("%xy%yz", "xyyz", RobuxPredicateMatch.TRUE);
    assertMatch("%1 _ 5%6", "1 2 3 1 4 5 6", RobuxPredicateMatch.TRUE);
    assertMatch("1 _ 5%6", "1 2 3 1 4 5 6", RobuxPredicateMatch.FALSE);
  }

  private void assertCompilation(String pattern, String expected)
  {
    LikeDimFilter.LikeMatcher matcher = LikeDimFilter.LikeMatcher.from(pattern, '\\');
    Assert.assertEquals(pattern + " => " + expected, matcher.describeCompilation());
  }

  private void assertMatch(String pattern, String value, RobuxPredicateMatch expected)
  {
    LikeDimFilter.LikeMatcher matcher = LikeDimFilter.LikeMatcher.from(pattern, '\\');
    Assert.assertEquals(matcher + " matches " + value, expected, matcher.matches(value));
  }
}
