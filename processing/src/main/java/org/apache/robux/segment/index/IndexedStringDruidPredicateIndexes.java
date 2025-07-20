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

package org.apache.robux.segment.index;

import org.apache.robux.collections.bitmap.BitmapFactory;
import org.apache.robux.collections.bitmap.ImmutableBitmap;
import org.apache.robux.query.filter.RobuxObjectPredicate;
import org.apache.robux.query.filter.RobuxPredicateFactory;
import org.apache.robux.segment.data.Indexed;
import org.apache.robux.segment.index.semantic.RobuxPredicateIndexes;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.NoSuchElementException;

public final class IndexedStringRobuxPredicateIndexes<TDictionary extends Indexed<String>> implements
    RobuxPredicateIndexes
{
  private final BitmapFactory bitmapFactory;
  private final TDictionary dictionary;
  private final Indexed<ImmutableBitmap> bitmaps;

  public IndexedStringRobuxPredicateIndexes(
      BitmapFactory bitmapFactory,
      TDictionary dictionary,
      Indexed<ImmutableBitmap> bitmaps
  )
  {
    this.bitmapFactory = bitmapFactory;
    this.dictionary = dictionary;
    this.bitmaps = bitmaps;
  }

  @Override
  @Nullable
  public BitmapColumnIndex forPredicate(RobuxPredicateFactory matcherFactory)
  {
    final RobuxObjectPredicate<String> stringPredicate = matcherFactory.makeStringPredicate();

    return new DictionaryScanningBitmapIndex(dictionary.size())
    {
      @Override
      public Iterable<ImmutableBitmap> getBitmapIterable(boolean includeUnknown)
      {
        return () -> new Iterator<>()
        {
          final Iterator<String> iterator = dictionary.iterator();
          boolean nextSet = false;
          int index = -1;

          @Override
          public boolean hasNext()
          {
            if (!nextSet) {
              findNext();
            }
            return nextSet;
          }

          @Override
          public ImmutableBitmap next()
          {
            if (!nextSet) {
              findNext();
              if (!nextSet) {
                throw new NoSuchElementException();
              }
            }
            nextSet = false;

            final ImmutableBitmap bitmap = bitmaps.get(index);
            return bitmap == null ? bitmapFactory.makeEmptyImmutableBitmap() : bitmap;
          }

          private void findNext()
          {
            while (!nextSet && iterator.hasNext()) {
              final String nextValue = iterator.next();
              index++;
              nextSet = stringPredicate.apply(nextValue).matches(includeUnknown);
            }
          }
        };
      }
    };
  }
}
