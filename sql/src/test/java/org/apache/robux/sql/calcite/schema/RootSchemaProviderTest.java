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

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.schema.Schema;
import org.apache.robux.java.util.common.ISE;
import org.apache.robux.sql.calcite.util.CalciteTestBase;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(EasyMockExtension.class)
public class RootSchemaProviderTest extends CalciteTestBase
{
  private static final String SCHEMA_1 = "SCHEMA_1";
  private static final String SCHEMA_2 = "SCHEMA_2";
  @Mock
  private NamedSchema robuxSchema1;
  @Mock
  private NamedSchema robuxSchema2;
  @Mock
  private NamedSchema duplicateSchema1;
  @Mock
  private Schema schema1;
  @Mock
  private Schema schema2;
  @Mock
  private Schema schema3;
  private Set<NamedSchema> robuxSchemas;

  private RootSchemaProvider target;

  @BeforeEach
  public void setUp()
  {
    EasyMock.expect(robuxSchema1.getSchema()).andStubReturn(schema1);
    EasyMock.expect(robuxSchema2.getSchema()).andStubReturn(schema2);
    EasyMock.expect(duplicateSchema1.getSchema()).andStubReturn(schema3);
    EasyMock.expect(robuxSchema1.getSchemaName()).andStubReturn(SCHEMA_1);
    EasyMock.expect(robuxSchema2.getSchemaName()).andStubReturn(SCHEMA_2);
    EasyMock.expect(duplicateSchema1.getSchemaName()).andStubReturn(SCHEMA_1);
    EasyMock.replay(robuxSchema1, robuxSchema2, duplicateSchema1);

    robuxSchemas = ImmutableSet.of(robuxSchema1, robuxSchema2);
    target = new RootSchemaProvider(robuxSchemas);
  }
  @Test
  public void testGetShouldReturnRootSchemaWithProvidedSchemasRegistered()
  {
    RobuxSchemaCatalog rootSchema = target.get();
    Assert.assertEquals("", rootSchema.getRootSchema().getName());
    Assert.assertFalse(rootSchema.getRootSchema().isCacheEnabled());
    // metadata schema should not be added
    Assert.assertEquals(robuxSchemas.size(), rootSchema.getSubSchemaNames().size());

    Assert.assertEquals(schema1, rootSchema.getSubSchema(SCHEMA_1).unwrap(schema1.getClass()));
    Assert.assertEquals(schema2, rootSchema.getSubSchema(SCHEMA_2).unwrap(schema2.getClass()));
  }

  @Test
  public void testGetWithDuplicateSchemasShouldThrowISE()
  {
    assertThrows(ISE.class, () -> {
      target = new RootSchemaProvider(ImmutableSet.of(robuxSchema1, robuxSchema2, duplicateSchema1));
      target.get();
    });
  }
}
