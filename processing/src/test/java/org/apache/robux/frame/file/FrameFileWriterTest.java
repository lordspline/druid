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

package org.apache.robux.frame.file;

import org.apache.robux.frame.Frame;
import org.apache.robux.frame.FrameType;
import org.apache.robux.frame.allocation.ArenaMemoryAllocator;
import org.apache.robux.frame.channel.ByteTracker;
import org.apache.robux.frame.testutil.FrameSequenceBuilder;
import org.apache.robux.java.util.common.guava.Sequence;
import org.apache.robux.segment.TestIndex;
import org.apache.robux.segment.incremental.IncrementalIndexCursorFactory;
import org.apache.robux.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class FrameFileWriterTest extends InitializedNullHandlingTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void test_abort_afterAllFrames() throws IOException
  {
    final Sequence<Frame> frames = FrameSequenceBuilder.fromCursorFactory(new IncrementalIndexCursorFactory(TestIndex.getIncrementalTestIndex()))
                                                       .allocator(ArenaMemoryAllocator.createOnHeap(1000000))
                                                       .frameType(FrameType.latestRowBased())
                                                       .frames();

    final File file = temporaryFolder.newFile();
    final FrameFileWriter fileWriter = FrameFileWriter.open(Files.newByteChannel(
        file.toPath(),
        StandardOpenOption.WRITE
    ), null, ByteTracker.unboundedTracker());

    frames.forEach(frame -> {
      try {
        fileWriter.writeFrame(frame, FrameFileWriter.NO_PARTITION);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    fileWriter.abort();

    final IllegalStateException e = Assert.assertThrows(IllegalStateException.class, () -> FrameFile.open(file, null));

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(
            CoreMatchers.containsString("Corrupt or truncated file[")
        )
    );
  }
}
