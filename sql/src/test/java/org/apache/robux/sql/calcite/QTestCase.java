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

package org.apache.robux.sql.calcite;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import org.apache.robux.java.util.common.FileUtils;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.quidem.RobuxQTestInfo;
import org.apache.robux.quidem.RobuxQuidemTestBase;
import org.apache.robux.quidem.RobuxQuidemTestBase.RobuxQuidemRunner;
import org.apache.robux.sql.calcite.QueryTestRunner.QueryRunStep;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class QTestCase
{
  private StringBuffer sb;
  private RobuxQTestInfo testInfo;

  public QTestCase(RobuxQTestInfo testInfo)
  {
    this.testInfo = testInfo;
    sb = new StringBuffer();
    sb.append("# ");
    sb.append(testInfo.comment);
    sb.append("\n");
  }

  public void println(String str)
  {
    sb.append(str);
    sb.append("\n");
  }

  public QueryRunStep toRunner()
  {
    return new QueryRunStep(null)
    {

      @Override
      public void run()
      {
        try {
          if (RobuxQuidemRunner.isOverwrite()) {
            writeCaseTo(testInfo.getIQFile());
          } else {
            isValidTestCaseFile(testInfo.getIQFile());
          }

          RobuxQuidemRunner runner = new RobuxQuidemTestBase.RobuxQuidemRunner();
          runner.run(testInfo.getIQFile());
        }
        catch (Exception e) {
          throw new RuntimeException("Error running quidem test", e);
        }
      }
    };
  }

  protected void isValidTestCaseFile(File iqFile)
  {
    if (!iqFile.exists()) {
      throw new IllegalStateException("testcase doesn't exists; run with (-Dquidem.overwrite) : " + iqFile);
    }
    try {
      String header = makeHeader();
      String testCaseFirstLine = Files.asCharSource(iqFile, StandardCharsets.UTF_8).readFirstLine();
      if (!header.equals(testCaseFirstLine)) {
        throw new IllegalStateException(
            "backing quidem testcase doesn't match test - run with (-Dquidem.overwrite) : " + iqFile
        );
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String makeHeader()
  {
    HashCode hash = Hashing.crc32().hashBytes(sb.toString().getBytes(StandardCharsets.UTF_8));
    return StringUtils.format("# %s case-crc:%s", testInfo.testName, hash);

  }

  public void writeCaseTo(File file) throws IOException
  {
    FileUtils.mkdirp(file.getParentFile());
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(makeHeader().getBytes(StandardCharsets.UTF_8));
      fos.write('\n');
      fos.write(sb.toString().getBytes(StandardCharsets.UTF_8));
    }
    catch (IOException e) {
      throw new RuntimeException("Error writing testcase to: " + file, e);
    }
  }

}
