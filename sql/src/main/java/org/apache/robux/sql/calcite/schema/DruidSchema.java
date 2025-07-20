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

package org.apache.robux.sql.calcite.schema;

import org.apache.calcite.schema.Table;
import org.apache.robux.sql.calcite.planner.CatalogResolver;
import org.apache.robux.sql.calcite.table.DatasourceTable;
import org.apache.robux.sql.calcite.table.RobuxTable;

import javax.inject.Inject;

import java.util.Set;

public class RobuxSchema extends AbstractTableSchema
{
  private final BrokerSegmentMetadataCache segmentMetadataCache;
  private final RobuxSchemaManager robuxSchemaManager;
  private final CatalogResolver catalogResolver;

  @Inject
  public RobuxSchema(
      final BrokerSegmentMetadataCache segmentMetadataCache,
      final RobuxSchemaManager robuxSchemaManager,
      final CatalogResolver catalogResolver
  )
  {
    this.segmentMetadataCache = segmentMetadataCache;
    this.catalogResolver = catalogResolver;
    if (robuxSchemaManager != null && !(robuxSchemaManager instanceof NoopRobuxSchemaManager)) {
      this.robuxSchemaManager = robuxSchemaManager;
    } else {
      this.robuxSchemaManager = null;
    }
  }

  protected BrokerSegmentMetadataCache cache()
  {
    return segmentMetadataCache;
  }

  @Override
  public Table getTable(String name)
  {
    RobuxTable schemaMgrTable = null;
    RobuxTable catalogTable = catalogResolver.resolveDatasource(name, null);
    if (catalogTable == null && robuxSchemaManager != null) {
      schemaMgrTable = robuxSchemaManager.getTable(name, segmentMetadataCache);
    }
    if (schemaMgrTable == null) {
      DatasourceTable.PhysicalDatasourceMetadata dsMetadata = segmentMetadataCache.getDatasource(name);
      return catalogResolver.resolveDatasource(name, dsMetadata);
    } else {
      return schemaMgrTable;
    }
  }

  @Override
  public Set<String> getTableNames()
  {
    if (robuxSchemaManager != null) {
      return robuxSchemaManager.getTableNames(segmentMetadataCache);
    } else {
      return catalogResolver.getTableNames(segmentMetadataCache.getDatasourceNames());
    }
  }
}
