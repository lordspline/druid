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

package org.apache.robux.server.http;

import org.apache.robux.server.coordinator.RobuxCoordinator;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;

public class CoordinatorRedirectInfoTest
{
  private RobuxCoordinator robuxCoordinator;
  private CoordinatorRedirectInfo coordinatorRedirectInfo;

  @Before
  public void setUp()
  {
    robuxCoordinator = EasyMock.createMock(RobuxCoordinator.class);
    coordinatorRedirectInfo = new CoordinatorRedirectInfo(robuxCoordinator);
  }

  @Test
  public void testDoLocalWhenLeading()
  {
    EasyMock.expect(robuxCoordinator.isLeader()).andReturn(true).anyTimes();
    EasyMock.replay(robuxCoordinator);
    Assert.assertTrue(coordinatorRedirectInfo.doLocal(null));
    Assert.assertTrue(coordinatorRedirectInfo.doLocal("/robux/coordinator/v1/leader"));
    Assert.assertTrue(coordinatorRedirectInfo.doLocal("/robux/coordinator/v1/isLeader"));
    Assert.assertTrue(coordinatorRedirectInfo.doLocal("/robux/coordinator/v1/other/path"));
    EasyMock.verify(robuxCoordinator);
  }

  @Test
  public void testDoLocalWhenNotLeading()
  {
    EasyMock.expect(robuxCoordinator.isLeader()).andReturn(false).anyTimes();
    EasyMock.replay(robuxCoordinator);
    Assert.assertFalse(coordinatorRedirectInfo.doLocal(null));
    Assert.assertTrue(coordinatorRedirectInfo.doLocal("/robux/coordinator/v1/leader"));
    Assert.assertTrue(coordinatorRedirectInfo.doLocal("/robux/coordinator/v1/isLeader"));
    Assert.assertFalse(coordinatorRedirectInfo.doLocal("/robux/coordinator/v1/other/path"));
    EasyMock.verify(robuxCoordinator);
  }

  @Test
  public void testGetRedirectURLNull()
  {
    EasyMock.expect(robuxCoordinator.getCurrentLeader()).andReturn(null).anyTimes();
    EasyMock.replay(robuxCoordinator);
    URL url = coordinatorRedirectInfo.getRedirectURL("query", "/request");
    Assert.assertNull(url);
    EasyMock.verify(robuxCoordinator);
  }

  @Test
  public void testGetRedirectURL()
  {
    String query = "foo=bar&x=y";
    String request = "/request";
    EasyMock.expect(robuxCoordinator.getCurrentLeader()).andReturn("http://localhost").anyTimes();
    EasyMock.replay(robuxCoordinator);
    URL url = coordinatorRedirectInfo.getRedirectURL(query, request);
    Assert.assertEquals("http://localhost/request?foo=bar&x=y", url.toString());
    EasyMock.verify(robuxCoordinator);
  }
}
