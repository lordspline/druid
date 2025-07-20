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

package org.apache.robux.cli;

import org.apache.robux.indexing.overlord.RobuxOverlord;
import org.apache.robux.server.coordinator.RobuxCoordinator;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CoordinatorOverlordRedirectInfoTest
{
  private RobuxOverlord overlord;
  private RobuxCoordinator coordinator;
  private CoordinatorOverlordRedirectInfo redirectInfo;

  @Before
  public void setUp()
  {
    overlord = EasyMock.createMock(RobuxOverlord.class);
    coordinator = EasyMock.createMock(RobuxCoordinator.class);
    redirectInfo = new CoordinatorOverlordRedirectInfo(overlord, coordinator);
  }

  @Test
  public void testDoLocalIndexerWhenLeading()
  {
    EasyMock.expect(overlord.isLeader()).andReturn(true).anyTimes();
    EasyMock.replay(overlord);
    Assert.assertTrue(redirectInfo.doLocal("/robux/indexer/v1/leader"));
    Assert.assertTrue(redirectInfo.doLocal("/robux/indexer/v1/isLeader"));
    Assert.assertTrue(redirectInfo.doLocal("/robux/indexer/v1/other/path"));
    EasyMock.verify(overlord);
  }
}
