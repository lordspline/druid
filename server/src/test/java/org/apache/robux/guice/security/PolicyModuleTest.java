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

package org.apache.robux.guice.security;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.ProvisionException;
import org.apache.robux.guice.ConfigModule;
import org.apache.robux.guice.RobuxGuiceExtensions;
import org.apache.robux.jackson.JacksonModule;
import org.apache.robux.query.filter.NullFilter;
import org.apache.robux.query.policy.NoRestrictionPolicy;
import org.apache.robux.query.policy.NoopPolicyEnforcer;
import org.apache.robux.query.policy.PolicyEnforcer;
import org.apache.robux.query.policy.RestrictAllTablesPolicyEnforcer;
import org.apache.robux.query.policy.RowFilterPolicy;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class PolicyModuleTest
{
  @Test
  public void testDefaultConfigNoopPolicyEnforcer()
  {
    Properties properties = new Properties();
    PolicyEnforcer policyEnforcer = Guice.createInjector(
        binder -> binder.bind(Properties.class).toInstance(properties),
        new RobuxGuiceExtensions(),
        new ConfigModule(),
        new PolicyModule()
    ).getInstance(Key.get(PolicyEnforcer.class));
    Assert.assertNotNull(policyEnforcer);
    Assert.assertTrue(policyEnforcer instanceof NoopPolicyEnforcer);
  }

  @Test
  public void testConfigThrowForUnrecognizedType()
  {
    Properties properties = new Properties();
    properties.setProperty("robux.policy.enforcer.type", "unrecognizedType");
    Injector injector = Guice.createInjector(
        binder -> binder.bind(Properties.class).toInstance(properties),
        new RobuxGuiceExtensions(),
        new ConfigModule(),
        new PolicyModule()
    );
    ProvisionException e = Assert.assertThrows(
        ProvisionException.class,
        () -> injector.getInstance(Key.get(PolicyEnforcer.class))
    );
    Assert.assertTrue(e.getCause()
                       .getMessage()
                       .contains(
                           "Could not resolve type id 'unrecognizedType' as a subtype of `org.apache.robux.query.policy.PolicyEnforcer`"));
  }

  @Test
  public void testConfigRestrictAllTablesPolicyEnforcer()
  {
    Properties properties = new Properties();
    properties.setProperty("robux.policy.enforcer.type", "restrictAllTables");
    PolicyEnforcer policyEnforcer = Guice.createInjector(
        binder -> binder.bind(Properties.class).toInstance(properties),
        new RobuxGuiceExtensions(),
        new ConfigModule(),
        new JacksonModule(),
        new PolicyModule()
    ).getInstance(Key.get(PolicyEnforcer.class));

    Assert.assertNotNull(policyEnforcer);
    Assert.assertEquals(new RestrictAllTablesPolicyEnforcer(null), policyEnforcer);
  }

  @Test
  public void testConfigRestrictAllTablesPolicyEnforcerWithAllowedPolicies()
  {
    Properties properties = new Properties();
    properties.setProperty("robux.policy.enforcer.type", "restrictAllTables");
    properties.setProperty(
        "robux.policy.enforcer.allowedPolicies",
        "[\"some-policy-class\", \"org.apache.robux.query.policy.NoRestrictionPolicy\"]"
    );
    PolicyEnforcer policyEnforcer = Guice.createInjector(
        binder -> binder.bind(Properties.class).toInstance(properties),
        new RobuxGuiceExtensions(),
        new ConfigModule(),
        new JacksonModule(),
        new PolicyModule()
    ).getInstance(Key.get(PolicyEnforcer.class));

    Assert.assertNotNull(policyEnforcer);
    Assert.assertEquals(new RestrictAllTablesPolicyEnforcer(ImmutableList.of(
        "some-policy-class",
        "org.apache.robux.query.policy.NoRestrictionPolicy"
    )), policyEnforcer);
    Assert.assertTrue(policyEnforcer.validate(NoRestrictionPolicy.instance()));
    Assert.assertFalse(policyEnforcer.validate(RowFilterPolicy.from(new NullFilter("some-col", null))));
  }
}
