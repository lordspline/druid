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

package org.apache.robux.security.authorization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.robux.audit.AuditManager;
import org.apache.robux.java.util.common.StringUtils;
import org.apache.robux.java.util.common.concurrent.Execs;
import org.apache.robux.metadata.MetadataStorageTablesConfig;
import org.apache.robux.metadata.TestDerbyConnector;
import org.apache.robux.security.basic.BasicAuthCommonCacheConfig;
import org.apache.robux.security.basic.BasicAuthUtils;
import org.apache.robux.security.basic.authorization.BasicRoleBasedAuthorizer;
import org.apache.robux.security.basic.authorization.db.updater.CoordinatorBasicAuthorizerMetadataStorageUpdater;
import org.apache.robux.security.basic.authorization.endpoint.BasicAuthorizerResource;
import org.apache.robux.security.basic.authorization.endpoint.CoordinatorBasicAuthorizerResourceHandler;
import org.apache.robux.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.robux.security.basic.authorization.entity.BasicAuthorizerGroupMappingFull;
import org.apache.robux.security.basic.authorization.entity.BasicAuthorizerPermission;
import org.apache.robux.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.robux.security.basic.authorization.entity.BasicAuthorizerRoleFull;
import org.apache.robux.security.basic.authorization.entity.BasicAuthorizerRoleSimplifiedPermissions;
import org.apache.robux.security.basic.authorization.entity.BasicAuthorizerUser;
import org.apache.robux.security.basic.authorization.entity.BasicAuthorizerUserFull;
import org.apache.robux.security.basic.authorization.entity.BasicAuthorizerUserFullSimplifiedPermissions;
import org.apache.robux.server.security.Action;
import org.apache.robux.server.security.AuthValidator;
import org.apache.robux.server.security.AuthorizerMapper;
import org.apache.robux.server.security.Resource;
import org.apache.robux.server.security.ResourceAction;
import org.apache.robux.server.security.ResourceType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

@RunWith(MockitoJUnitRunner.class)
public class CoordinatorBasicAuthorizerResourceTest
{
  private static final String AUTHORIZER_NAME = "test";
  private static final String AUTHORIZER_NAME2 = "test2";
  private static final String AUTHORIZER_NAME3 = "test3";

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();
  @Mock
  private AuthValidator authValidator;
  @Mock
  private HttpServletRequest req;
  @Mock
  private AuditManager auditManager;

  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig;
  private BasicAuthorizerResource resource;
  private CoordinatorBasicAuthorizerMetadataStorageUpdater storageUpdater;

  @Before
  public void setUp()
  {
    connector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createConfigTable();

    AuthorizerMapper authorizerMapper = new AuthorizerMapper(
        ImmutableMap.of(
            AUTHORIZER_NAME,
            new BasicRoleBasedAuthorizer(
                null,
                AUTHORIZER_NAME,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            AUTHORIZER_NAME2,
            new BasicRoleBasedAuthorizer(
                null,
                AUTHORIZER_NAME2,
                null,
                null,
                null,
                null,
                null,
                null
            ),
            AUTHORIZER_NAME3,
            new BasicRoleBasedAuthorizer(
                null,
                AUTHORIZER_NAME3,
                null,
                null,
                "adminGroupMapping",
                null,
                null,
                null
            )
        )
    );

    storageUpdater = new CoordinatorBasicAuthorizerMetadataStorageUpdater(
        authorizerMapper,
        connector,
        tablesConfig,
        new BasicAuthCommonCacheConfig(null, null, null, null),
        new ObjectMapper(new SmileFactory()),
        new NoopBasicAuthorizerCacheNotifier(),
        null
    );

    resource = new BasicAuthorizerResource(
        new CoordinatorBasicAuthorizerResourceHandler(
            storageUpdater,
            authorizerMapper,
            new ObjectMapper(new SmileFactory())
        ),
        authValidator,
        auditManager
    );

    storageUpdater.start();
  }

  @After
  public void tearDown()
  {
    storageUpdater.stop();
  }

  @Test
  public void testSeparateDatabaseTables()
  {
    Response response = resource.getAllUsers(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        ImmutableSet.of(BasicAuthUtils.ADMIN_NAME, BasicAuthUtils.INTERNAL_USER_NAME),
        response.getEntity()
    );

    response = resource.getAllUsers(req, AUTHORIZER_NAME2);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        ImmutableSet.of(BasicAuthUtils.ADMIN_NAME, BasicAuthUtils.INTERNAL_USER_NAME),
        response.getEntity()
    );

    response = resource.getAllGroupMappings(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        ImmutableSet.of(),
        response.getEntity()
    );

    response = resource.getAllGroupMappings(req, AUTHORIZER_NAME2);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        ImmutableSet.of(),
        response.getEntity()
    );

    response = resource.getAllGroupMappings(req, AUTHORIZER_NAME3);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        ImmutableSet.of("adminGroupMapping"),
        response.getEntity()
    );

    resource.createUser(req, AUTHORIZER_NAME, "robux");
    resource.createUser(req, AUTHORIZER_NAME, "robux2");
    resource.createUser(req, AUTHORIZER_NAME, "robux3");
    resource.createGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", new BasicAuthorizerGroupMapping("robuxGroupMapping", "", new HashSet<>()));
    resource.createGroupMapping(req, AUTHORIZER_NAME, "robux2GroupMapping", new BasicAuthorizerGroupMapping("robux2GroupMapping", "", new HashSet<>()));
    resource.createGroupMapping(req, AUTHORIZER_NAME, "robux3GroupMapping", new BasicAuthorizerGroupMapping("robux3GroupMapping", "", new HashSet<>()));

    resource.createUser(req, AUTHORIZER_NAME2, "robux4");
    resource.createUser(req, AUTHORIZER_NAME2, "robux5");
    resource.createUser(req, AUTHORIZER_NAME2, "robux6");
    resource.createGroupMapping(req, AUTHORIZER_NAME2, "robux4GroupMapping", new BasicAuthorizerGroupMapping("robux4GroupMapping", "", new HashSet<>()));
    resource.createGroupMapping(req, AUTHORIZER_NAME2, "robux5GroupMapping", new BasicAuthorizerGroupMapping("robux5GroupMapping", "", new HashSet<>()));
    resource.createGroupMapping(req, AUTHORIZER_NAME2, "robux6GroupMapping", new BasicAuthorizerGroupMapping("robux6GroupMapping", "", new HashSet<>()));

    Set<String> expectedUsers = ImmutableSet.of(
        BasicAuthUtils.ADMIN_NAME,
        BasicAuthUtils.INTERNAL_USER_NAME,
        "robux",
        "robux2",
        "robux3"
    );

    Set<String> expectedUsers2 = ImmutableSet.of(
        BasicAuthUtils.ADMIN_NAME,
        BasicAuthUtils.INTERNAL_USER_NAME,
        "robux4",
        "robux5",
        "robux6"
    );

    response = resource.getAllUsers(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers, response.getEntity());

    response = resource.getAllUsers(req, AUTHORIZER_NAME2);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers2, response.getEntity());

    Set<String> expectedGroupMappings = ImmutableSet.of(
        "robuxGroupMapping",
        "robux2GroupMapping",
        "robux3GroupMapping"
    );

    Set<String> expectedGroupMappings2 = ImmutableSet.of(
        "robux4GroupMapping",
        "robux5GroupMapping",
        "robux6GroupMapping"
    );

    response = resource.getAllGroupMappings(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedGroupMappings, response.getEntity());

    response = resource.getAllGroupMappings(req, AUTHORIZER_NAME2);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedGroupMappings2, response.getEntity());
  }

  @Test
  public void testInvalidAuthorizer()
  {
    Response response = resource.getAllUsers(req, "invalidName");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(
        errorMapWithMsg("Basic authorizer with name [invalidName] does not exist."),
        response.getEntity()
    );
  }

  @Test
  public void testGetAllUsers()
  {
    Response response = resource.getAllUsers(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        ImmutableSet.of(BasicAuthUtils.ADMIN_NAME, BasicAuthUtils.INTERNAL_USER_NAME),
        response.getEntity()
    );

    resource.createUser(req, AUTHORIZER_NAME, "robux");
    resource.createUser(req, AUTHORIZER_NAME, "robux2");
    resource.createUser(req, AUTHORIZER_NAME, "robux3");

    Set<String> expectedUsers = ImmutableSet.of(
        BasicAuthUtils.ADMIN_NAME,
        BasicAuthUtils.INTERNAL_USER_NAME,
        "robux",
        "robux2",
        "robux3"
    );

    response = resource.getAllUsers(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers, response.getEntity());
  }

  @Test
  public void testGetAllGroupMappings()
  {
    Response response = resource.getAllGroupMappings(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        ImmutableSet.of(),
        response.getEntity()
    );

    resource.createGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", new BasicAuthorizerGroupMapping("robuxGroupMapping", "", new HashSet<>()));
    resource.createGroupMapping(req, AUTHORIZER_NAME, "robux2GroupMapping", new BasicAuthorizerGroupMapping("robux2GroupMapping", "", new HashSet<>()));
    resource.createGroupMapping(req, AUTHORIZER_NAME, "robux3GroupMapping", new BasicAuthorizerGroupMapping("robux3GroupMapping", "", new HashSet<>()));

    Set<String> expectedGroupMappings = ImmutableSet.of(
        "robuxGroupMapping",
        "robux2GroupMapping",
        "robux3GroupMapping"
    );

    response = resource.getAllGroupMappings(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedGroupMappings, response.getEntity());
  }

  @Test
  public void testGetAllRoles()
  {
    Response response = resource.getAllRoles(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(
        ImmutableSet.of(BasicAuthUtils.ADMIN_NAME, BasicAuthUtils.INTERNAL_USER_NAME),
        response.getEntity()
    );

    resource.createRole(req, AUTHORIZER_NAME, "robux");
    resource.createRole(req, AUTHORIZER_NAME, "robux2");
    resource.createRole(req, AUTHORIZER_NAME, "robux3");

    Set<String> expectedRoles = ImmutableSet.of(
        BasicAuthUtils.ADMIN_NAME,
        BasicAuthUtils.INTERNAL_USER_NAME,
        "robux",
        "robux2",
        "robux3"
    );

    response = resource.getAllRoles(req, AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoles, response.getEntity());
  }

  @Test
  public void testCreateDeleteUser()
  {
    Response response = resource.createUser(req, AUTHORIZER_NAME, "robux");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, AUTHORIZER_NAME, "robux", null, null);
    Assert.assertEquals(200, response.getStatus());

    BasicAuthorizerUser expectedUser = new BasicAuthorizerUser(
        "robux",
        ImmutableSet.of()
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = resource.deleteUser(req, AUTHORIZER_NAME, "robux");
    Assert.assertEquals(200, response.getStatus());

    response = resource.deleteUser(req, AUTHORIZER_NAME, "robux");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [robux] does not exist."), response.getEntity());

    response = resource.getUser(req, AUTHORIZER_NAME, "robux", null, null);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("User [robux] does not exist."), response.getEntity());
  }

  @Test
  public void testCreateDeleteGroupMapping()
  {
    Response response = resource.createGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", new BasicAuthorizerGroupMapping("robuxGroupMapping", "", new HashSet<>()));
    Assert.assertEquals(200, response.getStatus());

    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", null);
    Assert.assertEquals(200, response.getStatus());

    BasicAuthorizerGroupMapping expectedGroupMapping = new BasicAuthorizerGroupMapping(
        "robuxGroupMapping",
        "", ImmutableSet.of()
    );
    Assert.assertEquals(expectedGroupMapping, response.getEntity());

    response = resource.deleteGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping");
    Assert.assertEquals(200, response.getStatus());

    response = resource.deleteGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Group mapping [robuxGroupMapping] does not exist."), response.getEntity());

    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", null);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Group mapping [robuxGroupMapping] does not exist."), response.getEntity());
  }

  @Test
  public void testCreateDeleteRole()
  {
    Response response = resource.createRole(req, AUTHORIZER_NAME, "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole", null, null);
    Assert.assertEquals(200, response.getStatus());

    BasicAuthorizerRole expectedRole = new BasicAuthorizerRole("robuxRole", ImmutableList.of());
    Assert.assertEquals(expectedRole, response.getEntity());

    response = resource.deleteRole(req, AUTHORIZER_NAME, "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.deleteRole(req, AUTHORIZER_NAME, "robuxRole");
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Role [robuxRole] does not exist."), response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole", null, null);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Role [robuxRole] does not exist."), response.getEntity());
  }

  @Test
  public void testUserRoleAssignment()
  {
    Response response = resource.createRole(req, AUTHORIZER_NAME, "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createUser(req, AUTHORIZER_NAME, "robux");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, AUTHORIZER_NAME, "robux", "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, AUTHORIZER_NAME, "robux", null, null);
    Assert.assertEquals(200, response.getStatus());

    BasicAuthorizerUser expectedUser = new BasicAuthorizerUser(
        "robux",
        ImmutableSet.of("robuxRole")
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole", null, null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerRole expectedRole = new BasicAuthorizerRole("robuxRole", ImmutableList.of());
    Assert.assertEquals(expectedRole, response.getEntity());

    response = resource.unassignRoleFromUser(req, AUTHORIZER_NAME, "robux", "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, AUTHORIZER_NAME, "robux", null, null);
    Assert.assertEquals(200, response.getStatus());
    expectedUser = new BasicAuthorizerUser(
        "robux",
        ImmutableSet.of()
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole", null, null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRole, response.getEntity());
  }

  @Test
  public void testGroupMappingRoleAssignment()
  {
    Response response = resource.createRole(req, AUTHORIZER_NAME, "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", new BasicAuthorizerGroupMapping("robuxGroupMapping", "", new HashSet<>()));
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", null);
    Assert.assertEquals(200, response.getStatus());

    BasicAuthorizerGroupMapping expectedGroupMapping = new BasicAuthorizerGroupMapping(
        "robuxGroupMapping",
        "", ImmutableSet.of("robuxRole")
    );
    Assert.assertEquals(expectedGroupMapping, response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole", null, null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerRole expectedRole = new BasicAuthorizerRole("robuxRole", ImmutableList.of());
    Assert.assertEquals(expectedRole, response.getEntity());

    response = resource.unassignRoleFromGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", null);
    Assert.assertEquals(200, response.getStatus());
    expectedGroupMapping = new BasicAuthorizerGroupMapping(
        "robuxGroupMapping",
        "", ImmutableSet.of()
    );
    Assert.assertEquals(expectedGroupMapping, response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole", null, null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRole, response.getEntity());
  }

  @Test
  public void testDeleteAssignedRole()
  {
    Response response = resource.createRole(req, AUTHORIZER_NAME, "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createUser(req, AUTHORIZER_NAME, "robux");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createUser(req, AUTHORIZER_NAME, "robux2");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, AUTHORIZER_NAME, "robux", "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, AUTHORIZER_NAME, "robux2", "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", new BasicAuthorizerGroupMapping("robuxGroupMapping", "", new HashSet<>()));
    Assert.assertEquals(200, response.getStatus());

    response = resource.createGroupMapping(req, AUTHORIZER_NAME, "robux2GroupMapping", new BasicAuthorizerGroupMapping("robux2GroupMapping", "", new HashSet<>()));
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToGroupMapping(req, AUTHORIZER_NAME, "robux2GroupMapping", "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, AUTHORIZER_NAME, "robux", null, null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerUser expectedUser = new BasicAuthorizerUser(
        "robux",
        ImmutableSet.of("robuxRole")
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = resource.getUser(req, AUTHORIZER_NAME, "robux2", null, null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerUser expectedUser2 = new BasicAuthorizerUser(
        "robux2",
        ImmutableSet.of("robuxRole")
    );
    Assert.assertEquals(expectedUser2, response.getEntity());

    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerGroupMapping expectedGroupMapping = new BasicAuthorizerGroupMapping(
        "robuxGroupMapping",
        "", ImmutableSet.of("robuxRole")
    );
    Assert.assertEquals(expectedGroupMapping, response.getEntity());

    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "robux2GroupMapping", null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerGroupMapping expectedGroupMapping2 = new BasicAuthorizerGroupMapping(
        "robux2GroupMapping",
        "", ImmutableSet.of("robuxRole")
    );
    Assert.assertEquals(expectedGroupMapping2, response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole", null, null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerRole expectedRole = new BasicAuthorizerRole("robuxRole", ImmutableList.of());
    Assert.assertEquals(expectedRole, response.getEntity());

    response = resource.deleteRole(req, AUTHORIZER_NAME, "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.getUser(req, AUTHORIZER_NAME, "robux", null, null);
    Assert.assertEquals(200, response.getStatus());
    expectedUser = new BasicAuthorizerUser(
        "robux",
        ImmutableSet.of()
    );
    Assert.assertEquals(expectedUser, response.getEntity());

    response = resource.getUser(req, AUTHORIZER_NAME, "robux2", null, null);
    Assert.assertEquals(200, response.getStatus());
    expectedUser2 = new BasicAuthorizerUser(
        "robux2",
        ImmutableSet.of()
    );
    Assert.assertEquals(expectedUser2, response.getEntity());

    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", null);
    Assert.assertEquals(200, response.getStatus());
    expectedGroupMapping = new BasicAuthorizerGroupMapping(
        "robuxGroupMapping",
        "", ImmutableSet.of()
    );
    Assert.assertEquals(expectedGroupMapping, response.getEntity());

    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "robux2GroupMapping", null);
    Assert.assertEquals(200, response.getStatus());
    expectedGroupMapping2 = new BasicAuthorizerGroupMapping(
        "robux2GroupMapping",
        "", ImmutableSet.of()
    );
    Assert.assertEquals(expectedGroupMapping2, response.getEntity());
  }

  @Test
  public void testRolesAndPerms()
  {
    Response response = resource.createRole(req, AUTHORIZER_NAME, "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    List<ResourceAction> perms = ImmutableList.of(
        new ResourceAction(new Resource("A", ResourceType.DATASOURCE), Action.READ),
        new ResourceAction(new Resource("B", ResourceType.DATASOURCE), Action.WRITE),
        new ResourceAction(new Resource("C", ResourceType.CONFIG), Action.WRITE)
    );

    response = resource.setRolePermissions(req, AUTHORIZER_NAME, "robuxRole", perms);
    Assert.assertEquals(200, response.getStatus());

    response = resource.setRolePermissions(req, AUTHORIZER_NAME, "wrongRole", perms);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals(errorMapWithMsg("Role [wrongRole] does not exist."), response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole", null, null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerRole expectedRole = new BasicAuthorizerRole("robuxRole", BasicAuthorizerPermission.makePermissionList(perms));
    Assert.assertEquals(expectedRole, response.getEntity());

    List<ResourceAction> newPerms = ImmutableList.of(
        new ResourceAction(new Resource("D", ResourceType.DATASOURCE), Action.READ),
        new ResourceAction(new Resource("B", ResourceType.DATASOURCE), Action.WRITE),
        new ResourceAction(new Resource("F", ResourceType.CONFIG), Action.WRITE)
    );

    response = resource.setRolePermissions(req, AUTHORIZER_NAME, "robuxRole", newPerms);
    Assert.assertEquals(200, response.getStatus());

    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole", null, null);
    Assert.assertEquals(200, response.getStatus());
    expectedRole = new BasicAuthorizerRole("robuxRole", BasicAuthorizerPermission.makePermissionList(newPerms));
    Assert.assertEquals(expectedRole, response.getEntity());

    response = resource.setRolePermissions(req, AUTHORIZER_NAME, "robuxRole", null);
    Assert.assertEquals(200, response.getStatus());

    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole", null, null);
    Assert.assertEquals(200, response.getStatus());
    expectedRole = new BasicAuthorizerRole("robuxRole", null);
    Assert.assertEquals(expectedRole, response.getEntity());
  }

  @Test
  public void testUsersGroupMappingsRolesAndPerms()
  {
    Response response = resource.createUser(req, AUTHORIZER_NAME, "robux");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createUser(req, AUTHORIZER_NAME, "robux2");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", new BasicAuthorizerGroupMapping("robuxGroupMapping", "", new HashSet<>()));
    Assert.assertEquals(200, response.getStatus());

    response = resource.createGroupMapping(req, AUTHORIZER_NAME, "robux2GroupMapping", new BasicAuthorizerGroupMapping("robux2GroupMapping", "", new HashSet<>()));
    Assert.assertEquals(200, response.getStatus());

    response = resource.createRole(req, AUTHORIZER_NAME, "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.createRole(req, AUTHORIZER_NAME, "robuxRole2");
    Assert.assertEquals(200, response.getStatus());

    List<ResourceAction> perms = ImmutableList.of(
        new ResourceAction(new Resource("A", ResourceType.DATASOURCE), Action.READ),
        new ResourceAction(new Resource("B", ResourceType.DATASOURCE), Action.WRITE),
        new ResourceAction(new Resource("C", ResourceType.CONFIG), Action.WRITE)
    );

    List<ResourceAction> perms2 = ImmutableList.of(
        new ResourceAction(new Resource("D", ResourceType.STATE), Action.READ),
        new ResourceAction(new Resource("E", ResourceType.DATASOURCE), Action.WRITE),
        new ResourceAction(new Resource("F", ResourceType.CONFIG), Action.WRITE)
    );

    response = resource.setRolePermissions(req, AUTHORIZER_NAME, "robuxRole", perms);
    Assert.assertEquals(200, response.getStatus());

    response = resource.setRolePermissions(req, AUTHORIZER_NAME, "robuxRole2", perms2);
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, AUTHORIZER_NAME, "robux", "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, AUTHORIZER_NAME, "robux", "robuxRole2");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, AUTHORIZER_NAME, "robux2", "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToUser(req, AUTHORIZER_NAME, "robux2", "robuxRole2");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", "robuxRole2");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToGroupMapping(req, AUTHORIZER_NAME, "robux2GroupMapping", "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.assignRoleToGroupMapping(req, AUTHORIZER_NAME, "robux2GroupMapping", "robuxRole2");
    Assert.assertEquals(200, response.getStatus());

    BasicAuthorizerRole expectedRole = new BasicAuthorizerRole("robuxRole", BasicAuthorizerPermission.makePermissionList(perms));
    BasicAuthorizerRole expectedRole2 = new BasicAuthorizerRole("robuxRole2", BasicAuthorizerPermission.makePermissionList(perms2));
    Set<BasicAuthorizerRole> expectedRoles = Sets.newHashSet(expectedRole, expectedRole2);

    BasicAuthorizerUserFull expectedUserFull = new BasicAuthorizerUserFull("robux", expectedRoles);
    response = resource.getUser(req, AUTHORIZER_NAME, "robux", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFull, response.getEntity());
    BasicAuthorizerUserFullSimplifiedPermissions expectedUserFullSimplifiedPermissions =
        new BasicAuthorizerUserFullSimplifiedPermissions(
            "robux",
            BasicAuthorizerRoleSimplifiedPermissions.convertRoles(expectedRoles)
        );
    response = resource.getUser(req, AUTHORIZER_NAME, "robux", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFullSimplifiedPermissions, response.getEntity());

    BasicAuthorizerUserFull expectedUserFull2 = new BasicAuthorizerUserFull("robux2", expectedRoles);
    response = resource.getUser(req, AUTHORIZER_NAME, "robux2", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFull2, response.getEntity());
    BasicAuthorizerUserFullSimplifiedPermissions expectedUserFullSimplifiedPermissions2 =
        new BasicAuthorizerUserFullSimplifiedPermissions(
            "robux2",
            BasicAuthorizerRoleSimplifiedPermissions.convertRoles(expectedRoles)
        );
    response = resource.getUser(req, AUTHORIZER_NAME, "robux2", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFullSimplifiedPermissions2, response.getEntity());

    BasicAuthorizerGroupMappingFull expectedGroupMappingFull = new BasicAuthorizerGroupMappingFull("robuxGroupMapping", "", expectedRoles);
    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedGroupMappingFull, response.getEntity());

    BasicAuthorizerGroupMappingFull expectedGroupMappingFull2 = new BasicAuthorizerGroupMappingFull("robux2GroupMapping", "", expectedRoles);
    response = resource.getGroupMapping(req, AUTHORIZER_NAME, "robux2GroupMapping", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedGroupMappingFull2, response.getEntity());

    Set<String> expectedUserSet = Sets.newHashSet("robux", "robux2");
    Set<String> expectedGroupMappingSet = Sets.newHashSet("robuxGroupMapping", "robux2GroupMapping");
    BasicAuthorizerRoleFull expectedRoleFull = new BasicAuthorizerRoleFull(
        "robuxRole",
        expectedUserSet,
        expectedGroupMappingSet,
        BasicAuthorizerPermission.makePermissionList(perms)
    );
    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleFull, response.getEntity());
    BasicAuthorizerRoleSimplifiedPermissions expectedRoleSimplifiedPerms = new BasicAuthorizerRoleSimplifiedPermissions(
        "robuxRole",
        expectedUserSet,
        perms
    );
    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleSimplifiedPerms, response.getEntity());
    expectedRoleSimplifiedPerms = new BasicAuthorizerRoleSimplifiedPermissions(
        "robuxRole",
        null,
        perms
    );
    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole", null, "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleSimplifiedPerms, response.getEntity());

    BasicAuthorizerRoleFull expectedRoleFull2 = new BasicAuthorizerRoleFull(
        "robuxRole2",
        expectedUserSet,
        expectedGroupMappingSet,
        BasicAuthorizerPermission.makePermissionList(perms2)
    );
    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole2", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleFull2, response.getEntity());
    BasicAuthorizerRoleSimplifiedPermissions expectedRoleSimplifiedPerms2 = new BasicAuthorizerRoleSimplifiedPermissions(
        "robuxRole2",
        expectedUserSet,
        perms2
    );
    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole2", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleSimplifiedPerms2, response.getEntity());
    expectedRoleSimplifiedPerms2 = new BasicAuthorizerRoleSimplifiedPermissions(
        "robuxRole2",
        null,
        perms2
    );
    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole2", null, "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleSimplifiedPerms2, response.getEntity());

    perms = ImmutableList.of(
        new ResourceAction(new Resource("A", ResourceType.DATASOURCE), Action.READ),
        new ResourceAction(new Resource("C", ResourceType.CONFIG), Action.WRITE)
    );

    perms2 = ImmutableList.of(
        new ResourceAction(new Resource("E", ResourceType.DATASOURCE), Action.WRITE)
    );

    response = resource.setRolePermissions(req, AUTHORIZER_NAME, "robuxRole", perms);
    Assert.assertEquals(200, response.getStatus());

    response = resource.setRolePermissions(req, AUTHORIZER_NAME, "robuxRole2", perms2);
    Assert.assertEquals(200, response.getStatus());

    expectedRole = new BasicAuthorizerRole("robuxRole", BasicAuthorizerPermission.makePermissionList(perms));
    expectedRole2 = new BasicAuthorizerRole("robuxRole2", BasicAuthorizerPermission.makePermissionList(perms2));
    expectedRoles = Sets.newHashSet(expectedRole, expectedRole2);
    expectedUserFull = new BasicAuthorizerUserFull("robux", expectedRoles);
    expectedUserFull2 = new BasicAuthorizerUserFull("robux2", expectedRoles);
    expectedUserFullSimplifiedPermissions = new BasicAuthorizerUserFullSimplifiedPermissions(
        "robux",
        BasicAuthorizerRoleSimplifiedPermissions.convertRoles(expectedRoles)
    );
    expectedUserFullSimplifiedPermissions2 = new BasicAuthorizerUserFullSimplifiedPermissions(
        "robux2",
        BasicAuthorizerRoleSimplifiedPermissions.convertRoles(expectedRoles)
    );

    response = resource.getUser(req, AUTHORIZER_NAME, "robux", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFull, response.getEntity());
    response = resource.getUser(req, AUTHORIZER_NAME, "robux", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFullSimplifiedPermissions, response.getEntity());

    response = resource.getUser(req, AUTHORIZER_NAME, "robux2", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFull2, response.getEntity());
    response = resource.getUser(req, AUTHORIZER_NAME, "robux2", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFullSimplifiedPermissions2, response.getEntity());

    response = resource.unassignRoleFromUser(req, AUTHORIZER_NAME, "robux", "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.unassignRoleFromUser(req, AUTHORIZER_NAME, "robux2", "robuxRole2");
    Assert.assertEquals(200, response.getStatus());

    response = resource.unassignRoleFromGroupMapping(req, AUTHORIZER_NAME, "robuxGroupMapping", "robuxRole");
    Assert.assertEquals(200, response.getStatus());

    response = resource.unassignRoleFromGroupMapping(req, AUTHORIZER_NAME, "robux2GroupMapping", "robuxRole2");
    Assert.assertEquals(200, response.getStatus());

    expectedUserFull = new BasicAuthorizerUserFull("robux", Sets.newHashSet(expectedRole2));
    expectedUserFull2 = new BasicAuthorizerUserFull("robux2", Sets.newHashSet(expectedRole));
    expectedRoleFull = new BasicAuthorizerRoleFull(
        "robuxRole",
        Sets.newHashSet("robux2"),
        Sets.newHashSet("robux2GroupMapping"),
        BasicAuthorizerPermission.makePermissionList(perms)
    );
    expectedRoleFull2 = new BasicAuthorizerRoleFull(
        "robuxRole2",
        Sets.newHashSet("robux"),
        Sets.newHashSet("robuxGroupMapping"),
        BasicAuthorizerPermission.makePermissionList(perms2)
    );
    expectedUserFullSimplifiedPermissions = new BasicAuthorizerUserFullSimplifiedPermissions(
        "robux",
        BasicAuthorizerRoleSimplifiedPermissions.convertRoles(expectedUserFull.getRoles())
    );
    expectedUserFullSimplifiedPermissions2 = new BasicAuthorizerUserFullSimplifiedPermissions(
        "robux2",
        BasicAuthorizerRoleSimplifiedPermissions.convertRoles(expectedUserFull2.getRoles())
    );
    expectedRoleSimplifiedPerms = new BasicAuthorizerRoleSimplifiedPermissions(expectedRoleFull);
    expectedRoleSimplifiedPerms2 = new BasicAuthorizerRoleSimplifiedPermissions(expectedRoleFull2);

    response = resource.getUser(req, AUTHORIZER_NAME, "robux", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFull, response.getEntity());
    response = resource.getUser(req, AUTHORIZER_NAME, "robux", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFullSimplifiedPermissions, response.getEntity());

    response = resource.getUser(req, AUTHORIZER_NAME, "robux2", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFull2, response.getEntity());
    response = resource.getUser(req, AUTHORIZER_NAME, "robux2", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUserFullSimplifiedPermissions2, response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleFull, response.getEntity());
    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleSimplifiedPerms, response.getEntity());

    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole2", "", null);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleFull2, response.getEntity());
    response = resource.getRole(req, AUTHORIZER_NAME, "robuxRole2", "", "");
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedRoleSimplifiedPerms2, response.getEntity());
  }

  @Test
  public void testConcurrentUpdate()
  {
    final int testMultiple = 100;

    // setup a user and the roles
    Response response = resource.createUser(req, AUTHORIZER_NAME, "robux");
    Assert.assertEquals(200, response.getStatus());

    List<ResourceAction> perms = ImmutableList.of(
        new ResourceAction(new Resource("A", ResourceType.DATASOURCE), Action.READ),
        new ResourceAction(new Resource("B", ResourceType.DATASOURCE), Action.WRITE),
        new ResourceAction(new Resource("C", ResourceType.CONFIG), Action.WRITE)
    );

    for (int i = 0; i < testMultiple; i++) {
      String roleName = "robuxRole-" + i;
      response = resource.createRole(req, AUTHORIZER_NAME, roleName);
      Assert.assertEquals(200, response.getStatus());

      response = resource.setRolePermissions(req, AUTHORIZER_NAME, roleName, perms);
      Assert.assertEquals(200, response.getStatus());
    }

    ExecutorService exec = Execs.multiThreaded(testMultiple, "thread---");
    int[] responseCodesAssign = new int[testMultiple];

    // assign 'testMultiple' roles to the user concurrently
    List<Callable<Void>> addRoleCallables = new ArrayList<>();
    for (int i = 0; i < testMultiple; i++) {
      final int innerI = i;
      String roleName = "robuxRole-" + i;
      addRoleCallables.add(
          () -> {
            Response response12 = resource.assignRoleToUser(req, AUTHORIZER_NAME, "robux", roleName);
            responseCodesAssign[innerI] = response12.getStatus();
            return null;
          }
      );
    }
    try {
      List<Future<Void>> futures = exec.invokeAll(addRoleCallables);
      for (Future future : futures) {
        future.get();
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    // the API can return !200 if the update attempt fails by exhausting retries because of
    // too much contention from other conflicting requests, make sure that we don't get any successful requests
    // that didn't actually take effect
    Set<String> roleNames = getRoleNamesAssignedToUser("robux");
    for (int i = 0; i < testMultiple; i++) {
      String roleName = "robuxRole-" + i;
      if (responseCodesAssign[i] == 200 && !roleNames.contains(roleName)) {
        Assert.fail(
            StringUtils.format("Got response status 200 for assigning role [%s] but user did not have role.", roleName)
        );
      }
    }

    // Now unassign the roles concurrently
    List<Callable<Void>> removeRoleCallables = new ArrayList<>();
    int[] responseCodesRemove = new int[testMultiple];

    for (int i = 0; i < testMultiple; i++) {
      final int innerI = i;
      String roleName = "robuxRole-" + i;
      removeRoleCallables.add(
          () -> {
            Response response1 = resource.unassignRoleFromUser(req, AUTHORIZER_NAME, "robux", roleName);
            responseCodesRemove[innerI] = response1.getStatus();
            return null;
          }
      );
    }
    try {
      List<Future<Void>> futures = exec.invokeAll(removeRoleCallables);
      for (Future future : futures) {
        future.get();
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    roleNames = getRoleNamesAssignedToUser("robux");
    for (int i = 0; i < testMultiple; i++) {
      String roleName = "robuxRole-" + i;
      if (responseCodesRemove[i] == 200 && roleNames.contains(roleName)) {
        Assert.fail(
            StringUtils.format("Got response status 200 for removing role [%s] but user still has role.", roleName)
        );
      }
    }
  }

  private Set<String> getRoleNamesAssignedToUser(
      String user
  )
  {
    Response response = resource.getUser(req, AUTHORIZER_NAME, user, "", null);
    Assert.assertEquals(200, response.getStatus());
    BasicAuthorizerUserFull userFull = (BasicAuthorizerUserFull) response.getEntity();
    Set<String> roleNames = new HashSet<>();
    for (BasicAuthorizerRole role : userFull.getRoles()) {
      roleNames.add(role.getName());
    }
    return roleNames;
  }

  private static Map<String, String> errorMapWithMsg(String errorMsg)
  {
    return ImmutableMap.of("error", errorMsg);
  }
}
