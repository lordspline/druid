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
import com.google.common.collect.ImmutableMap;
import org.apache.robux.metadata.MetadataStorageTablesConfig;
import org.apache.robux.metadata.TestDerbyConnector;
import org.apache.robux.security.basic.BasicAuthCommonCacheConfig;
import org.apache.robux.security.basic.BasicAuthUtils;
import org.apache.robux.security.basic.authorization.BasicRoleBasedAuthorizer;
import org.apache.robux.security.basic.authorization.LDAPRoleProvider;
import org.apache.robux.security.basic.authorization.MetadataStoreRoleProvider;
import org.apache.robux.security.basic.authorization.db.cache.MetadataStoragePollingBasicAuthorizerCacheManager;
import org.apache.robux.security.basic.authorization.db.updater.CoordinatorBasicAuthorizerMetadataStorageUpdater;
import org.apache.robux.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.robux.server.security.Access;
import org.apache.robux.server.security.Action;
import org.apache.robux.server.security.AuthenticationResult;
import org.apache.robux.server.security.AuthorizerMapper;
import org.apache.robux.server.security.Resource;
import org.apache.robux.server.security.ResourceAction;
import org.apache.robux.server.security.ResourceType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.SearchResult;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BasicRoleBasedAuthorizerTest
{
  private static final String DB_AUTHORIZER_NAME = "metadata";
  private static final String LDAP_AUTHORIZER_NAME = "ldap";

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private BasicRoleBasedAuthorizer authorizer;
  private BasicRoleBasedAuthorizer ldapAuthorizer;

  private CoordinatorBasicAuthorizerMetadataStorageUpdater updater;
  private String[] groupFilters = {
      "*,OU=Robux,OU=Application,OU=Groupings,DC=corp,DC=apache,DC=org",
      "*,OU=Platform,OU=Groupings,DC=corp,DC=apache,DC=org"
  };

  private SearchResult userSearchResult;
  private SearchResult adminSearchResult;

  @Before
  public void setUp()
  {
    TestDerbyConnector connector = derbyConnectorRule.getConnector();
    MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createConfigTable();

    BasicAttributes userAttrs = new BasicAttributes(true);
    userAttrs.put(new BasicAttribute("sAMAccountName", "robuxuser"));
    userAttrs.put(new BasicAttribute("memberOf", "CN=user,OU=Robux,OU=Application,OU=Groupings,DC=corp,DC=apache,DC=org"));

    BasicAttributes adminAttrs = new BasicAttributes(true);
    adminAttrs.put(new BasicAttribute("sAMAccountName", "robuxadmin"));
    adminAttrs.put(new BasicAttribute("memberOf", "CN=admin,OU=Platform,OU=Groupings,DC=corp,DC=apache,DC=org"));

    userSearchResult = new SearchResult("CN=1234,OU=Employees,OU=People", null, userAttrs);
    adminSearchResult = new SearchResult("CN=9876,OU=Employees,OU=People", null, adminAttrs);

    updater = new CoordinatorBasicAuthorizerMetadataStorageUpdater(
        new AuthorizerMapper(
            ImmutableMap.of(
                DB_AUTHORIZER_NAME,
                new BasicRoleBasedAuthorizer(
                    null,
                    DB_AUTHORIZER_NAME,
                    null,
                    null,
                    null, null,
                    null,
                    new MetadataStoreRoleProvider(null)
                ),
                LDAP_AUTHORIZER_NAME,
                new BasicRoleBasedAuthorizer(
                    null,
                    LDAP_AUTHORIZER_NAME,
                    null,
                    null,
                    null, null,
                    null,
                    new LDAPRoleProvider(null, groupFilters)
                )
            )
        ),
        connector,
        tablesConfig,
        new BasicAuthCommonCacheConfig(null, null, null, null),
        new ObjectMapper(new SmileFactory()),
        new NoopBasicAuthorizerCacheNotifier(),
        null
    );

    updater.start();

    authorizer = new BasicRoleBasedAuthorizer(
        null,
        DB_AUTHORIZER_NAME,
        null,
        null,
        null, null,
        null,
        new MetadataStoreRoleProvider(new MetadataStoragePollingBasicAuthorizerCacheManager(updater))
    );

    ldapAuthorizer = new BasicRoleBasedAuthorizer(
        null,
        LDAP_AUTHORIZER_NAME,
        null,
        null,
        null, null,
        null,
        new LDAPRoleProvider(new MetadataStoragePollingBasicAuthorizerCacheManager(updater), groupFilters)
    );
  }

  @After
  public void tearDown()
  {
  }

  @Test
  public void testAuth()
  {
    updater.createUser(DB_AUTHORIZER_NAME, "robux");
    updater.createRole(DB_AUTHORIZER_NAME, "robuxRole");
    updater.assignUserRole(DB_AUTHORIZER_NAME, "robux", "robuxRole");

    List<ResourceAction> permissions = Collections.singletonList(
        new ResourceAction(new Resource("testResource", ResourceType.DATASOURCE), Action.WRITE)
    );

    updater.setPermissions(DB_AUTHORIZER_NAME, "robuxRole", permissions);

    AuthenticationResult authenticationResult = new AuthenticationResult("robux", "robux", null, null);

    Access access = authorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertTrue(access.isAllowed());

    access = authorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());
  }

  @Test
  public void testAuthGroupMapping()
  {
    BasicAuthorizerGroupMapping groupMapping = new BasicAuthorizerGroupMapping("robuxGroupMapping", "CN=admin,OU=Platform,OU=Groupings,DC=corp,DC=apache,DC=org", null);
    updater.createGroupMapping(LDAP_AUTHORIZER_NAME, groupMapping);
    updater.createRole(LDAP_AUTHORIZER_NAME, "robuxRole");
    updater.assignGroupMappingRole(LDAP_AUTHORIZER_NAME, "robuxGroupMapping", "robuxRole");

    List<ResourceAction> permissions = Collections.singletonList(
        new ResourceAction(new Resource("testResource", ResourceType.DATASOURCE), Action.WRITE)
    );

    updater.setPermissions(LDAP_AUTHORIZER_NAME, "robuxRole", permissions);

    Map<String, Object> contexMap = new HashMap<>();
    contexMap.put(BasicAuthUtils.SEARCH_RESULT_CONTEXT_KEY, adminSearchResult);

    AuthenticationResult authenticationResult = new AuthenticationResult("robuxadmin", "robux", null, contexMap);

    Access access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertTrue(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());
  }
  @Test
  public void testAuthGroupMappingPatternRightMask()
  {
    //Admin
    BasicAuthorizerGroupMapping adminGrroupMapping = new BasicAuthorizerGroupMapping("adminGrroupMapping", "CN=admin,*", null);
    updater.createGroupMapping(LDAP_AUTHORIZER_NAME, adminGrroupMapping);
    updater.createRole(LDAP_AUTHORIZER_NAME, "adminRobuxRole");
    updater.assignGroupMappingRole(LDAP_AUTHORIZER_NAME, "adminGrroupMapping", "adminRobuxRole");
    List<ResourceAction> adminPermissions = Arrays.asList(
        new ResourceAction(new Resource("testResource", ResourceType.DATASOURCE), Action.WRITE),
        new ResourceAction(new Resource("testResource", ResourceType.DATASOURCE), Action.READ)
    );
    updater.setPermissions(LDAP_AUTHORIZER_NAME, "adminRobuxRole", adminPermissions);

    //User
    BasicAuthorizerGroupMapping userGrroupMapping = new BasicAuthorizerGroupMapping("userGrroupMapping", "CN=user,*", null);
    updater.createGroupMapping(LDAP_AUTHORIZER_NAME, userGrroupMapping);
    updater.createRole(LDAP_AUTHORIZER_NAME, "userRobuxRole");
    updater.assignGroupMappingRole(LDAP_AUTHORIZER_NAME, "userGrroupMapping", "userRobuxRole");

    List<ResourceAction> userPermissions = Collections.singletonList(
        new ResourceAction(new Resource("testResource", ResourceType.DATASOURCE), Action.READ)
    );

    updater.setPermissions(LDAP_AUTHORIZER_NAME, "userRobuxRole", userPermissions);

    Map<String, Object> contexMap = new HashMap<>();

    contexMap.put(BasicAuthUtils.SEARCH_RESULT_CONTEXT_KEY, adminSearchResult);
    AuthenticationResult authenticationResult = new AuthenticationResult("robuxadmin", "robux", null, contexMap);

    Access access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.READ
    );
    Assert.assertTrue(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertTrue(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());

    contexMap = new HashMap<>();
    contexMap.put(BasicAuthUtils.SEARCH_RESULT_CONTEXT_KEY, userSearchResult);
    authenticationResult = new AuthenticationResult("robuxuser", "robux", null, contexMap);

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.READ
    );
    Assert.assertTrue(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.READ
    );
    Assert.assertFalse(access.isAllowed());
  }

  @Test
  public void testAuthGroupMappingPatternLeftMask()
  {
    //Admin
    BasicAuthorizerGroupMapping adminGrroupMapping = new BasicAuthorizerGroupMapping("adminGrroupMapping", "*,CN=admin,OU=Platform,OU=Groupings,DC=corp,DC=apache,DC=org", null);
    updater.createGroupMapping(LDAP_AUTHORIZER_NAME, adminGrroupMapping);
    updater.createRole(LDAP_AUTHORIZER_NAME, "adminRobuxRole");
    updater.assignGroupMappingRole(LDAP_AUTHORIZER_NAME, "adminGrroupMapping", "adminRobuxRole");
    List<ResourceAction> adminPermissions = Arrays.asList(
        new ResourceAction(new Resource("testResource", ResourceType.DATASOURCE), Action.WRITE),
        new ResourceAction(new Resource("testResource", ResourceType.DATASOURCE), Action.READ)
    );
    updater.setPermissions(LDAP_AUTHORIZER_NAME, "adminRobuxRole", adminPermissions);

    //User
    BasicAuthorizerGroupMapping userGrroupMapping = new BasicAuthorizerGroupMapping("userGrroupMapping", "*,CN=user,OU=Robux,OU=Application,OU=Groupings,DC=corp,DC=apache,DC=org", null);
    updater.createGroupMapping(LDAP_AUTHORIZER_NAME, userGrroupMapping);
    updater.createRole(LDAP_AUTHORIZER_NAME, "userRobuxRole");
    updater.assignGroupMappingRole(LDAP_AUTHORIZER_NAME, "userGrroupMapping", "userRobuxRole");

    List<ResourceAction> userPermissions = Collections.singletonList(
        new ResourceAction(new Resource("testResource", ResourceType.DATASOURCE), Action.READ)
    );

    updater.setPermissions(LDAP_AUTHORIZER_NAME, "userRobuxRole", userPermissions);

    Map<String, Object> contexMap = new HashMap<>();

    contexMap.put(BasicAuthUtils.SEARCH_RESULT_CONTEXT_KEY, adminSearchResult);
    AuthenticationResult authenticationResult = new AuthenticationResult("robuxadmin", "robux", null, contexMap);

    Access access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.READ
    );
    Assert.assertTrue(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertTrue(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());

    contexMap = new HashMap<>();
    contexMap.put(BasicAuthUtils.SEARCH_RESULT_CONTEXT_KEY, userSearchResult);
    authenticationResult = new AuthenticationResult("robuxuser", "robux", null, contexMap);

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.READ
    );
    Assert.assertTrue(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.READ
    );
    Assert.assertFalse(access.isAllowed());
  }

  @Test
  public void testAuthMissingGroupMapping()
  {
    BasicAuthorizerGroupMapping groupMapping = new BasicAuthorizerGroupMapping("robuxGroupMapping", "CN=unknown,*", null);
    updater.createGroupMapping(LDAP_AUTHORIZER_NAME, groupMapping);
    updater.createRole(LDAP_AUTHORIZER_NAME, "robuxRole");
    updater.assignGroupMappingRole(LDAP_AUTHORIZER_NAME, "robuxGroupMapping", "robuxRole");

    List<ResourceAction> permissions = Arrays.asList(
        new ResourceAction(new Resource("testResource", ResourceType.DATASOURCE), Action.WRITE),
        new ResourceAction(new Resource("testResource", ResourceType.DATASOURCE), Action.READ)
    );

    updater.setPermissions(LDAP_AUTHORIZER_NAME, "robuxRole", permissions);

    Map<String, Object> contexMap = new HashMap<>();
    contexMap.put(BasicAuthUtils.SEARCH_RESULT_CONTEXT_KEY, userSearchResult);

    AuthenticationResult authenticationResult = new AuthenticationResult("robuxuser", "robux", null, contexMap);

    Access access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.READ
    );
    Assert.assertFalse(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    Assert.assertFalse(access.isAllowed());

    access = ldapAuthorizer.authorize(
        authenticationResult,
        new Resource("wrongResource", ResourceType.DATASOURCE),
        Action.READ
    );
    Assert.assertFalse(access.isAllowed());
  }
}
