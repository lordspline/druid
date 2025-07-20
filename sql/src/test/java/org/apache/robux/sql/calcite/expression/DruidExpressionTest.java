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

package org.apache.robux.sql.calcite.expression;

import org.apache.robux.math.expr.Expr;
import org.apache.robux.math.expr.ExprMacroTable;
import org.apache.robux.math.expr.ExpressionType;
import org.apache.robux.math.expr.Parser;
import org.apache.robux.segment.column.ColumnType;
import org.apache.robux.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

public class RobuxExpressionTest extends InitializedNullHandlingTest
{
  @Test
  public void test_doubleLiteral_asString()
  {
    Assert.assertEquals("0.0", RobuxExpression.doubleLiteral(0));
    Assert.assertEquals("-2.0", RobuxExpression.doubleLiteral(-2));
    Assert.assertEquals("2.0", RobuxExpression.doubleLiteral(2));
    Assert.assertEquals("2.1", RobuxExpression.doubleLiteral(2.1));
    Assert.assertEquals("2.12345678", RobuxExpression.doubleLiteral(2.12345678));
    Assert.assertEquals("2.2E122", RobuxExpression.doubleLiteral(2.2e122));
    Assert.assertEquals("NaN", RobuxExpression.doubleLiteral(Double.NaN));
    Assert.assertEquals("Infinity", RobuxExpression.doubleLiteral(Double.POSITIVE_INFINITY));
    Assert.assertEquals("-Infinity", RobuxExpression.doubleLiteral(Double.NEGATIVE_INFINITY));
    //CHECKSTYLE.OFF: Regexp
    // Min/max double are banned by regexp due to often being inappropriate; but they are appropriate here.
    Assert.assertEquals("4.9E-324", RobuxExpression.doubleLiteral(Double.MIN_VALUE));
    Assert.assertEquals("1.7976931348623157E308", RobuxExpression.doubleLiteral(Double.MAX_VALUE));
    //CHECKSTYLE.ON: Regexp
    Assert.assertEquals("2.2250738585072014E-308", RobuxExpression.doubleLiteral(Double.MIN_NORMAL));
  }

  @Test
  public void test_doubleLiteral_roundTrip()
  {
    final double[] doubles = {
        0,
        -2,
        2,
        2.1,
        2.12345678,
        2.2e122,
        Double.NaN,
        Double.POSITIVE_INFINITY,
        Double.NEGATIVE_INFINITY,
        //CHECKSTYLE.OFF: Regexp
        // Min/max double are banned by regexp due to often being inappropriate; but they are appropriate here.
        Double.MIN_VALUE,
        Double.MAX_VALUE,
        //CHECKSTYLE.ON: Regexp
        Double.MIN_NORMAL
    };

    for (double n : doubles) {
      final Expr expr = Parser.parse(RobuxExpression.doubleLiteral(n), ExprMacroTable.nil());
      Assert.assertTrue(expr.isLiteral());
      MatcherAssert.assertThat(expr.getLiteralValue(), CoreMatchers.instanceOf(Double.class));
      Assert.assertEquals(n, (double) expr.getLiteralValue(), 0d);
    }
  }

  @Test
  public void test_longLiteral_asString()
  {
    Assert.assertEquals("0", RobuxExpression.longLiteral(0));
    Assert.assertEquals("-2", RobuxExpression.longLiteral(-2));
    Assert.assertEquals("2", RobuxExpression.longLiteral(2));
    Assert.assertEquals("9223372036854775807", RobuxExpression.longLiteral(Long.MAX_VALUE));
    Assert.assertEquals("-9223372036854775808", RobuxExpression.longLiteral(Long.MIN_VALUE));
  }

  @Test
  public void test_longLiteral_roundTrip()
  {
    final long[] longs = {
        0,
        -2,
        2,
        Long.MAX_VALUE,
        Long.MIN_VALUE
    };

    for (long n : longs) {
      final Expr expr = Parser.parse(RobuxExpression.longLiteral(n), ExprMacroTable.nil());
      Assert.assertTrue(expr.isLiteral());
      MatcherAssert.assertThat(expr.getLiteralValue(), CoreMatchers.instanceOf(Number.class));
      Assert.assertEquals(n, ((Number) expr.getLiteralValue()).longValue());
    }
  }

  @Test
  public void test_ofLiteral_nullString()
  {
    final RobuxExpression expression = RobuxExpression.ofLiteral(new RobuxLiteral(ExpressionType.STRING, null));

    Assert.assertEquals(ColumnType.STRING, expression.getRobuxType());
    Assert.assertEquals("null", expression.getExpression());
  }

  @Test
  public void test_ofLiteral_nullLong()
  {
    final RobuxExpression expression = RobuxExpression.ofLiteral(new RobuxLiteral(ExpressionType.LONG, null));

    Assert.assertEquals(ColumnType.LONG, expression.getRobuxType());
    Assert.assertEquals("null", expression.getExpression());
  }

  @Test
  public void test_ofLiteral_nullDouble()
  {
    final RobuxExpression expression = RobuxExpression.ofLiteral(new RobuxLiteral(ExpressionType.DOUBLE, null));

    Assert.assertEquals(ColumnType.DOUBLE, expression.getRobuxType());
    Assert.assertEquals("null", expression.getExpression());
  }

  @Test
  public void test_ofLiteral_nullArray()
  {
    final RobuxExpression expression =
        RobuxExpression.ofLiteral(new RobuxLiteral(ExpressionType.STRING_ARRAY, null));

    Assert.assertEquals(ColumnType.STRING_ARRAY, expression.getRobuxType());
    Assert.assertEquals("null", expression.getExpression());
  }

  @Test
  public void test_ofLiteral_string()
  {
    final String s = "abcdé\n \\\" ' \uD83E\uDD20 \txyz";
    final RobuxExpression expression = RobuxExpression.ofLiteral(new RobuxLiteral(ExpressionType.STRING, s));

    Assert.assertEquals(ColumnType.STRING, expression.getRobuxType());
    Assert.assertEquals("'abcdé\\u000A \\u005C\\u0022 \\u0027 \\uD83E\\uDD20 \\u0009xyz'", expression.getExpression());
    Assert.assertEquals(s, Parser.parse(expression.getExpression(), ExprMacroTable.nil()).getLiteralValue());
  }

  @Test
  public void test_ofLiteral_emptyString()
  {
    final String s = "";
    final RobuxExpression expression = RobuxExpression.ofLiteral(new RobuxLiteral(ExpressionType.STRING, s));

    Assert.assertEquals(ColumnType.STRING, expression.getRobuxType());
    Assert.assertEquals("''", expression.getExpression());
    Assert.assertEquals(
        s,
        Parser.parse(expression.getExpression(), ExprMacroTable.nil()).getLiteralValue()
    );
  }

  @Test
  public void test_ofLiteral_long()
  {
    final RobuxExpression expression = RobuxExpression.ofLiteral(new RobuxLiteral(ExpressionType.LONG, -123));

    Assert.assertEquals(ColumnType.LONG, expression.getRobuxType());
    Assert.assertEquals("-123", expression.getExpression());
    Assert.assertEquals(-123L, Parser.parse(expression.getExpression(), ExprMacroTable.nil()).getLiteralValue());
  }

  @Test
  public void test_ofLiteral_double()
  {
    final RobuxExpression expression = RobuxExpression.ofLiteral(new RobuxLiteral(ExpressionType.DOUBLE, -123.4));

    Assert.assertEquals(ColumnType.DOUBLE, expression.getRobuxType());
    Assert.assertEquals("-123.4", expression.getExpression());
    Assert.assertEquals(-123.4, Parser.parse(expression.getExpression(), ExprMacroTable.nil()).getLiteralValue());
  }

  @Test
  public void test_ofLiteral_doubleNan()
  {
    final RobuxExpression expression = RobuxExpression.ofLiteral(new RobuxLiteral(ExpressionType.DOUBLE, Double.NaN));

    Assert.assertEquals(ColumnType.DOUBLE, expression.getRobuxType());
    Assert.assertEquals("NaN", expression.getExpression());
    Assert.assertEquals(Double.NaN, Parser.parse(expression.getExpression(), ExprMacroTable.nil()).getLiteralValue());
  }

  @Test
  public void test_ofLiteral_doubleNegativeInfinity()
  {
    final RobuxExpression expression =
        RobuxExpression.ofLiteral(new RobuxLiteral(ExpressionType.DOUBLE, Double.NEGATIVE_INFINITY));

    Assert.assertEquals(ColumnType.DOUBLE, expression.getRobuxType());
    Assert.assertEquals("-Infinity", expression.getExpression());
    Assert.assertEquals(
        Double.NEGATIVE_INFINITY,
        Parser.parse(expression.getExpression(), ExprMacroTable.nil()).getLiteralValue()
    );
  }

  @Test
  public void test_ofLiteral_doublePositiveInfinity()
  {
    final RobuxExpression expression =
        RobuxExpression.ofLiteral(new RobuxLiteral(ExpressionType.DOUBLE, Double.POSITIVE_INFINITY));

    Assert.assertEquals(ColumnType.DOUBLE, expression.getRobuxType());
    Assert.assertEquals("Infinity", expression.getExpression());
    Assert.assertEquals(
        Double.POSITIVE_INFINITY,
        Parser.parse(expression.getExpression(), ExprMacroTable.nil()).getLiteralValue()
    );
  }
}
