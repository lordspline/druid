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

package org.apache.robux.testsEx.catalog;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.robux.catalog.http.TableEditRequest.DropColumns;
import org.apache.robux.catalog.http.TableEditRequest.HideColumns;
import org.apache.robux.catalog.http.TableEditRequest.MoveColumn;
import org.apache.robux.catalog.http.TableEditRequest.UnhideColumns;
import org.apache.robux.catalog.model.CatalogUtils;
import org.apache.robux.catalog.model.TableId;
import org.apache.robux.catalog.model.TableMetadata;
import org.apache.robux.catalog.model.TableSpec;
import org.apache.robux.catalog.model.table.ClusterKeySpec;
import org.apache.robux.catalog.model.table.DatasourceDefn;
import org.apache.robux.catalog.model.table.TableBuilder;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.testsEx.categories.Catalog;
import org.apache.robux.testsEx.cluster.CatalogClient;
import org.apache.robux.testsEx.cluster.RobuxClusterClient;
import org.apache.robux.testsEx.config.RobuxTestRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Light sanity check of the Catalog REST API. Functional testing is
 * done via a unit test. Here we simply ensure that the Jersey plumbing
 * works as intended.
 */
@RunWith(RobuxTestRunner.class)
@Category(Catalog.class)
public class ITCatalogRestTest
{
  @Inject
  private RobuxClusterClient clusterClient;

  /**
   * Sample a few error cases to ensure the plumbing works.
   * Complete error testing appears in unit tests.
   */
  @Test
  public void testErrors()
  {
    CatalogClient client = new CatalogClient(clusterClient);

    // Bogus schema
    {
      final TableMetadata table = new TableBuilder(
            TableId.of("bogus", "foo"),
            DatasourceDefn.TABLE_TYPE
           )
          .build();

      assertThrows(
          Exception.class,
          () -> client.createTable(table, false)
      );
    }

    // Read-only schema
    {
      final TableMetadata table = new TableBuilder(
              TableId.of(TableId.SYSTEM_SCHEMA, "foo"),
              DatasourceDefn.TABLE_TYPE
           )
          .property(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY, "P1D")
          .build();
      assertThrows(
          Exception.class,
          () -> client.createTable(table, false)
      );
    }

    // Malformed table name
    {
      final TableMetadata table = TableBuilder.datasource(" foo ", "P1D")
          .build();
      assertThrows(
          Exception.class,
          () -> client.createTable(table, false)
      );
    }

    // DESC cluster keys not supported
    {
      final TableMetadata table = TableBuilder.datasource("foo", "P1D")
          .property(DatasourceDefn.CLUSTER_KEYS_PROPERTY, ImmutableList.of(new ClusterKeySpec("clusterKeyA", true)))
          .build();
      assertThrows(
          Exception.class,
          () -> client.createTable(table, false)
      );
    }
  }

  /**
   * Run though a table lifecycle to sanity check each API. Thorough
   * testing of each API appears in unit tests.
   */
  @Test
  public void testLifecycle()
  {
    CatalogClient client = new CatalogClient(clusterClient);

    // Create a datasource
    TableMetadata table = TableBuilder.datasource("example", "P1D")
        .column("a", "VARCHAR")
        .column("b", "BIGINT")
        .column("c", "FLOAT")
        .build();

    // Use force action so test is reentrant if it fails part way through
    // when debugging.
    long version = client.createTable(table, true);

    // Update the datasource
    TableSpec dsSpec2 = TableBuilder.copyOf(table)
        .property(DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY, 3_000_000)
        .column("d", "DOUBLE")
        .buildSpec();

    // First, optimistic locking, wrong version
    assertThrows(ISE.class, () -> client.updateTable(table.id(), dsSpec2, 1));

    // Optimistic locking, correct version
    long newVersion = client.updateTable(table.id(), dsSpec2, version);
    assertTrue(newVersion > version);

    // Verify the update
    TableMetadata read = client.readTable(table.id());
    assertEquals(dsSpec2, read.spec());

    // Move a column
    MoveColumn moveCmd = new MoveColumn("d", MoveColumn.Position.BEFORE, "a");
    client.editTable(table.id(), moveCmd);

    // Drop a column
    DropColumns dropCmd = new DropColumns(Collections.singletonList("b"));
    client.editTable(table.id(), dropCmd);
    read = client.readTable(table.id());
    assertEquals(Arrays.asList("d", "a", "c"), CatalogUtils.columnNames(read.spec().columns()));

    // Hide columns
    HideColumns hideCmd = new HideColumns(
        Arrays.asList("e", "f")
    );
    client.editTable(table.id(), hideCmd);
    read = client.readTable(table.id());
    assertEquals(
          Arrays.asList("e", "f"),
          read.spec().properties().get(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );

    // Unhide
    UnhideColumns unhideCmd = new UnhideColumns(
        Collections.singletonList("e")
    );
    client.editTable(table.id(), unhideCmd);
    read = client.readTable(table.id());
    assertEquals(
          Collections.singletonList("f"),
          read.spec().properties().get(DatasourceDefn.HIDDEN_COLUMNS_PROPERTY)
    );

    // List schemas
    List<String> schemaNames = client.listSchemas();
    assertTrue(schemaNames.contains(TableId.ROBUX_SCHEMA));
    assertTrue(schemaNames.contains(TableId.EXTERNAL_SCHEMA));
    assertTrue(schemaNames.contains(TableId.SYSTEM_SCHEMA));
    assertTrue(schemaNames.contains(TableId.CATALOG_SCHEMA));

    // List table names in schema
    List<String> tableNames = client.listTableNamesInSchema(TableId.ROBUX_SCHEMA);
    assertTrue(tableNames.contains(table.id().name()));

    // List tables
    List<TableId> tables = client.listTables();
    assertTrue(tables.contains(table.id()));

    // Drop the table
    client.dropTable(table.id());
    tableNames = client.listTableNamesInSchema(TableId.ROBUX_SCHEMA);
    assertFalse(tableNames.contains(table.id().name()));
  }
}
