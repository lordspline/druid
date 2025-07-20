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

package org.apache.robux.frame.segment;

import org.apache.robux.error.RobuxException;
import org.apache.robux.frame.Frame;
import org.apache.robux.frame.write.FrameWriter;
import org.apache.robux.frame.write.FrameWriterFactory;
import org.apache.robux.frame.write.UnsupportedColumnTypeException;
import org.apache.robux.java.util.common.Intervals;
import org.apache.robux.java.util.common.guava.Sequence;
import org.apache.robux.java.util.common.guava.Sequences;
import org.apache.robux.query.QueryContexts;
import org.apache.robux.query.filter.BoundDimFilter;
import org.apache.robux.query.filter.Filter;
import org.apache.robux.query.ordering.StringComparators;
import org.apache.robux.segment.Cursor;
import org.apache.robux.segment.column.ColumnHolder;
import org.apache.robux.segment.column.RowSignature;
import org.apache.robux.segment.filter.BoundFilter;
import org.apache.robux.segment.filter.Filters;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FrameCursorUtils
{

  /**
   * Exception to be thrown when the subquery's rows are too wide to fit in a single frame. In such case, byte based
   * limiting should be disabled or the user should modify the query.
   * <p>
   * NOTE: This error message is not appropriate when a similar exception is hit in MSQ, since this workaround
   * is not applicable in that scenario
   */
  public static final RobuxException SUBQUERY_ROW_TOO_LARGE_EXCEPTION =
      RobuxException
          .forPersona(RobuxException.Persona.OPERATOR)
          .ofCategory(RobuxException.Category.CAPACITY_EXCEEDED)
          .build(
              "Subquery's row size exceeds the frame size and therefore cannot write the subquery's "
              + "row to the frame. Either modify the subqueries to materialize smaller rows by removing wide columns, "
              + "or disable byte based limiting by setting '%s' to 'disabled'",
              QueryContexts.MAX_SUBQUERY_BYTES_KEY
          );

  private FrameCursorUtils()
  {
    // No instantiation.
  }

  /**
   * Builds a {@link Filter} from a {@link Filter} plus an {@link Interval}. Useful when we want to do a time filter
   * on a frame, but can't push the time filter into the frame itself (perhaps because it isn't time-sorted).
   */
  @Nullable
  public static Filter buildFilter(@Nullable Filter filter, Interval interval)
  {
    if (Intervals.ETERNITY.equals(interval)) {
      return filter;
    } else {
      return Filters.and(
          Arrays.asList(
              new BoundFilter(
                  new BoundDimFilter(
                      ColumnHolder.TIME_COLUMN_NAME,
                      String.valueOf(interval.getStartMillis()),
                      String.valueOf(interval.getEndMillis()),
                      false,
                      true,
                      null,
                      null,
                      StringComparators.NUMERIC
                  )
              ),
              filter
          )
      );
    }
  }

  /**
   * Writes a {@link Cursor} to a sequence of {@link Frame}. This method iterates over the rows of the cursor,
   * and writes the columns to the frames. The iterable is lazy, and it traverses the required portion of the cursor
   * as required.
   * <p>
   * If the type is missing from the signature, the method throws an exception without advancing/modifying/closing the
   * cursor
   */
  public static Iterable<Frame> cursorToFramesIterable(
      final Cursor cursor,
      final FrameWriterFactory frameWriterFactory
  )
  {
    throwIfColumnsHaveUnknownType(frameWriterFactory.signature());

    return () -> new Iterator<>()
    {
      @Override
      public boolean hasNext()
      {
        return !cursor.isDone();
      }

      @Override
      public Frame next()
      {
        // Makes sure that cursor contains some elements prior. This ensures if no row is written, then the row size
        // is larger than the MemoryAllocators returned by the provided factory
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        boolean firstRowWritten = false;
        Frame frame;
        try (final FrameWriter frameWriter = frameWriterFactory.newFrameWriter(cursor.getColumnSelectorFactory())) {
          while (!cursor.isDone()) {
            if (!frameWriter.addSelection()) {
              break;
            }
            firstRowWritten = true;
            cursor.advance();
          }

          if (!firstRowWritten) {
            throw SUBQUERY_ROW_TOO_LARGE_EXCEPTION;
          }

          frame = Frame.wrap(frameWriter.toByteArray());
        }
        return frame;
      }
    };
  }

  /**
   * Writes a {@link Cursor} to a sequence of {@link Frame}. This method iterates over the rows of the cursor,
   * and writes the columns to the frames
   *
   * @param cursor             Cursor to write to the frame
   * @param frameWriterFactory Frame writer factory to write to the frame.
   *                           It also determines the signature of the rows that are written to the frames
   */
  public static Sequence<Frame> cursorToFramesSequence(
      final Cursor cursor,
      final FrameWriterFactory frameWriterFactory
  )
  {
    return Sequences.simple(cursorToFramesIterable(cursor, frameWriterFactory));
  }

  /**
   * Throws {@link UnsupportedColumnTypeException} if the row signature has columns with unknown types. This is used to
   * pre-determine if the frames can be materialized as rows, without touching the resource generating the frames.
   */
  public static void throwIfColumnsHaveUnknownType(final RowSignature rowSignature)
  {
    for (int i = 0; i < rowSignature.size(); ++i) {
      if (!rowSignature.getColumnType(i).isPresent()) {
        throw new UnsupportedColumnTypeException(rowSignature.getColumnName(i), null);
      }
    }
  }
}
