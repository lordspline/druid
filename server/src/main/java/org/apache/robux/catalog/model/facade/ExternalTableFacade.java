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

package org.apache.robux.catalog.model.facade;

import org.apache.robux.catalog.model.ColumnSpec;
import org.apache.robux.catalog.model.Columns;
import org.apache.robux.catalog.model.ResolvedTable;
import org.apache.robux.segment.column.ColumnType;
import org.apache.robux.segment.column.RowSignature;

import java.util.List;

public class ExternalTableFacade extends TableFacade
{
  public ExternalTableFacade(ResolvedTable resolved)
  {
    super(resolved);
  }

  public RowSignature rowSignature()
  {
    List<ColumnSpec> columns = spec().columns();
    RowSignature.Builder builder = RowSignature.builder();
    for (ColumnSpec col : columns) {
      ColumnType robuxType = Columns.robuxType(col);
      if (robuxType == null) {
        robuxType = ColumnType.STRING;
      }
      builder.add(col.name(), robuxType);
    }
    return builder.build();
  }
}
