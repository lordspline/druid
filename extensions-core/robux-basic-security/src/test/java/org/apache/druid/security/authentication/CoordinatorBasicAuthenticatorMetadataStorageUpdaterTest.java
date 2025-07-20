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

package org.apache.robux.security.authentication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableMap;
import org.apache.robux.metadata.MetadataStorageTablesConfig;
import org.apache.robux.metadata.TestDerbyConnector;
import org.apache.robux.security.basic.BasicAuthCommonCacheConfig;
import org.apache.robux.security.basic.BasicAuthUtils;
import org.apache.robux.security.basic.BasicSecurityDBResourceException;
import org.apache.robux.security.basic.authentication.BasicHTTPAuthenticator;
import org.apache.robux.security.basic.authentication.db.updater.CoordinatorBasicAuthenticatorMetadataStorageUpdater;
import org.apache.robux.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;
import org.apache.robux.security.basic.authentication.entity.BasicAuthenticatorCredentials;
import org.apache.robux.security.basic.authentication.entity.BasicAuthenticatorUser;
import org.apache.robux.security.basic.authentication.validator.PasswordHashGenerator;
import org.apache.robux.server.security.AuthenticatorMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

public class CoordinatorBasicAuthenticatorMetadataStorageUpdaterTest
{
  private static final String AUTHENTICATOR_NAME = "test";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private CoordinatorBasicAuthenticatorMetadataStorageUpdater updater;
  private ObjectMapper objectMapper;

  @Before
  public void setUp()
  {
    objectMapper = new ObjectMapper(new SmileFactory());
    TestDerbyConnector connector = derbyConnectorRule.getConnector();
    MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createConfigTable();
    updater = new CoordinatorBasicAuthenticatorMetadataStorageUpdater(
        new AuthenticatorMapper(
            ImmutableMap.of(
                "test",
                new BasicHTTPAuthenticator(
                    null,
                    "test",
                    "test",
                    null,
                    null,
                    null,
                    null,
                    null,
                    false,
                    null
                )
            )
        ),
        connector,
        tablesConfig,
        new BasicAuthCommonCacheConfig(null, null, null, null),
        objectMapper,
        new NoopBasicAuthenticatorCacheNotifier(),
        null
    );

    updater.start();
  }

  @After
  public void tearDown()
  {
    updater.stop();
  }

  @Test
  public void createUser()
  {
    Map<String, BasicAuthenticatorUser> expectedUserMap = ImmutableMap.of(
        "robux", new BasicAuthenticatorUser("robux", null)
    );
    byte[] expectedSerializeUserMap = BasicAuthUtils.serializeAuthenticatorUserMap(objectMapper, expectedUserMap);

    updater.createUser(AUTHENTICATOR_NAME, "robux");
    Assert.assertArrayEquals(expectedSerializeUserMap, updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME));

    Map<String, BasicAuthenticatorUser> actualUserMap = BasicAuthUtils.deserializeAuthenticatorUserMap(
        objectMapper,
        updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME)
    );
    Assert.assertEquals(expectedUserMap, actualUserMap);

    // Validate cache user map methods
    Assert.assertEquals(expectedUserMap, updater.getCachedUserMap(AUTHENTICATOR_NAME));
    Assert.assertArrayEquals(expectedSerializeUserMap, updater.getCachedSerializedUserMap(AUTHENTICATOR_NAME));

    // create duplicate should fail
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [robux] already exists.");
    updater.createUser(AUTHENTICATOR_NAME, "robux");
  }

  @Test
  public void deleteUser()
  {
    Map<String, BasicAuthenticatorUser> expectedUserMap = ImmutableMap.of();
    byte[] expectedSerializeUserMap = BasicAuthUtils.serializeAuthenticatorUserMap(objectMapper, expectedUserMap);

    updater.createUser(AUTHENTICATOR_NAME, "robux");
    updater.deleteUser(AUTHENTICATOR_NAME, "robux");

    Assert.assertArrayEquals(expectedSerializeUserMap, updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME));

    Map<String, BasicAuthenticatorUser> actualUserMap = BasicAuthUtils.deserializeAuthenticatorUserMap(
        objectMapper,
        updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME)
    );
    Assert.assertEquals(expectedUserMap, actualUserMap);

    // Validate cache user map methods
    Assert.assertEquals(expectedUserMap, updater.getCachedUserMap(AUTHENTICATOR_NAME));
    Assert.assertArrayEquals(expectedSerializeUserMap, updater.getCachedSerializedUserMap(AUTHENTICATOR_NAME));

    // delete non-existent user should fail
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [robux] does not exist.");
    updater.deleteUser(AUTHENTICATOR_NAME, "robux");
  }

  @Test
  public void setCredentials()
  {
    updater.createUser(AUTHENTICATOR_NAME, "robux");
    updater.setUserCredentials(AUTHENTICATOR_NAME, "robux", new BasicAuthenticatorCredentialUpdate("helloworld", null));

    Map<String, BasicAuthenticatorUser> userMap = BasicAuthUtils.deserializeAuthenticatorUserMap(
        objectMapper,
        updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME)
    );
    BasicAuthenticatorCredentials credentials = userMap.get("robux").getCredentials();

    byte[] recalculatedHash = PasswordHashGenerator.computePasswordHash(
        "helloworld".toCharArray(),
        credentials.getSalt(),
        credentials.getIterations()
    );

    Assert.assertArrayEquals(credentials.getHash(), recalculatedHash);

    // Validate cache user map methods
    Map<String, BasicAuthenticatorUser> expectedUserMap = ImmutableMap.of(
        "robux", new BasicAuthenticatorUser("robux", credentials)
    );
    byte[] expectedSerializeUserMap = BasicAuthUtils.serializeAuthenticatorUserMap(objectMapper, expectedUserMap);
    Assert.assertArrayEquals(expectedSerializeUserMap, updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME));
    Assert.assertEquals(expectedUserMap, updater.getCachedUserMap(AUTHENTICATOR_NAME));
    Assert.assertArrayEquals(expectedSerializeUserMap, updater.getCachedSerializedUserMap(AUTHENTICATOR_NAME));
  }
}
