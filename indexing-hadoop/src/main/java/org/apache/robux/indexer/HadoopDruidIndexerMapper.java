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

package org.apache.robux.indexer;

import com.google.common.collect.ImmutableList;
import org.apache.robux.data.input.InputRow;
import org.apache.robux.data.input.impl.InputRowParser;
import org.apache.robux.data.input.impl.StringInputRowParser;
import org.apache.robux.indexer.granularity.GranularitySpec;
import org.apache.robux.java.util.common.DateTimes;
import org.apache.robux.java.util.common.Intervals;
import org.apache.robux.java.util.common.RE;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.collect.Utils;
import org.apache.robux.java.util.common.logger.Logger;
import org.apache.robux.java.util.common.parsers.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public abstract class HadoopRobuxIndexerMapper<KEYOUT, VALUEOUT> extends Mapper<Object, Object, KEYOUT, VALUEOUT>
{
  private static final Logger log = new Logger(HadoopRobuxIndexerMapper.class);

  protected HadoopRobuxIndexerConfig config;
  private InputRowParser parser;
  protected GranularitySpec granularitySpec;

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException
  {
    config = HadoopRobuxIndexerConfig.fromConfiguration(context.getConfiguration());
    parser = config.getParser();
    granularitySpec = config.getGranularitySpec();
  }

  public HadoopRobuxIndexerConfig getConfig()
  {
    return config;
  }

  @Override
  protected void map(Object key, Object value, Context context) throws IOException, InterruptedException
  {
    try {
      final List<InputRow> inputRows = parseInputRow(value, parser);

      for (InputRow inputRow : inputRows) {
        try {
          if (inputRow == null) {
            // Throw away null rows from the parser.
            log.debug("Throwing away row [%s]", value);
            context.getCounter(HadoopRobuxIndexerConfig.IndexJobCounters.ROWS_THROWN_AWAY_COUNTER).increment(1);
            continue;
          }

          if (!Intervals.ETERNITY.contains(inputRow.getTimestamp())) {
            final String errorMsg = StringUtils.format(
                "Encountered row with timestamp that cannot be represented as a long: [%s]",
                inputRow
            );
            throw new ParseException(null, errorMsg);
          }

          if (granularitySpec.inputIntervals().isEmpty()
              || granularitySpec.bucketInterval(DateTimes.utc(inputRow.getTimestampFromEpoch()))
                                .isPresent()) {
            innerMap(inputRow, context);
          } else {
            context.getCounter(HadoopRobuxIndexerConfig.IndexJobCounters.ROWS_THROWN_AWAY_COUNTER).increment(1);
          }
        }
        catch (ParseException pe) {
          handleParseException(pe, context);
        }
      }
    }
    catch (ParseException pe) {
      handleParseException(pe, context);
    }
    catch (RuntimeException e) {
      throw new RE(e, "Failure on row[%s]", value);
    }
  }

  private void handleParseException(ParseException pe, Context context)
  {
    context.getCounter(HadoopRobuxIndexerConfig.IndexJobCounters.INVALID_ROW_COUNTER).increment(1);
    Counter unparseableCounter = context.getCounter(HadoopRobuxIndexerConfig.IndexJobCounters.ROWS_UNPARSEABLE_COUNTER);
    Counter processedWithErrorsCounter = context.getCounter(HadoopRobuxIndexerConfig.IndexJobCounters.ROWS_PROCESSED_WITH_ERRORS_COUNTER);

    if (pe.isFromPartiallyValidRow()) {
      processedWithErrorsCounter.increment(1);
    } else {
      unparseableCounter.increment(1);
    }

    if (config.isLogParseExceptions()) {
      log.error(pe, "Encountered parse exception: ");
    }

    long rowsUnparseable = unparseableCounter.getValue();
    long rowsProcessedWithError = processedWithErrorsCounter.getValue();
    if (rowsUnparseable + rowsProcessedWithError > config.getMaxParseExceptions()) {
      log.error("Max parse exceptions exceeded, terminating task...");
      throw new RuntimeException("Max parse exceptions exceeded, terminating task...", pe);
    }
  }

  private static List<InputRow> parseInputRow(Object value, InputRowParser parser)
  {
    if (parser instanceof StringInputRowParser && value instanceof Text) {
      //Note: This is to ensure backward compatibility with 0.7.0 and before
      //HadoopyStringInputRowParser can handle this and this special case is not needed
      //except for backward compatibility
      return Utils.nullableListOf(((StringInputRowParser) parser).parse(value.toString()));
    } else if (value instanceof InputRow) {
      return ImmutableList.of((InputRow) value);
    } else if (value == null) {
      // Pass through nulls so they get thrown away.
      return Utils.nullableListOf((InputRow) null);
    } else {
      return parser.parseBatch(value);
    }
  }

  protected abstract void innerMap(InputRow inputRow, Context context)
      throws IOException, InterruptedException;

}
