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

package org.apache.druid.server.http;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.server.compaction.CompactionStatusResponse;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.easymock.EasyMock;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CoordinatorCompactionResourceTest
{
  private DruidCoordinator mock;
  private final String dataSourceName = "datasource_1";
  private final AutoCompactionSnapshot expectedSnapshot = new AutoCompactionSnapshot(
      dataSourceName,
      AutoCompactionSnapshot.ScheduleStatus.RUNNING,
      null,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1,
      1
  );

  @Before
  public void setUp()
  {
    mock = EasyMock.createStrictMock(DruidCoordinator.class);
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(mock);
  }

  @Test
  public void testGetCompactionSnapshotForDataSourceWithEmptyQueryParameter()
  {
    Map<String, AutoCompactionSnapshot> expected = Map.of(
        dataSourceName,
        expectedSnapshot
    );

    EasyMock.expect(mock.getAutoCompactionSnapshot()).andReturn(expected).once();
    EasyMock.replay(mock);

    final Response response = new CoordinatorCompactionResource(mock)
        .getCompactionSnapshotForDataSource("");
    Assert.assertEquals(new CompactionStatusResponse(List.of(expectedSnapshot)), response.getEntity());
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testGetCompactionSnapshotForDataSourceWithNullQueryParameter()
  {
    String dataSourceName = "datasource_1";
    Map<String, AutoCompactionSnapshot> expected = ImmutableMap.of(
        dataSourceName,
        expectedSnapshot
    );

    EasyMock.expect(mock.getAutoCompactionSnapshot()).andReturn(expected).once();
    EasyMock.replay(mock);

    final Response response = new CoordinatorCompactionResource(mock)
        .getCompactionSnapshotForDataSource(null);
    Assert.assertEquals(new CompactionStatusResponse(List.of(expectedSnapshot)), response.getEntity());
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testGetCompactionSnapshotForDataSourceWithValidQueryParameter()
  {
    String dataSourceName = "datasource_1";

    EasyMock.expect(mock.getAutoCompactionSnapshotForDataSource(dataSourceName))
            .andReturn(expectedSnapshot).once();
    EasyMock.replay(mock);

    final Response response = new CoordinatorCompactionResource(mock)
        .getCompactionSnapshotForDataSource(dataSourceName);
    Assert.assertEquals(
        new CompactionStatusResponse(Collections.singletonList(expectedSnapshot)),
        response.getEntity()
    );
    Assert.assertEquals(200, response.getStatus());
  }

  @Test
  public void testGetCompactionSnapshotForDataSourceWithInvalidQueryParameter()
  {
    String dataSourceName = "invalid_datasource";

    EasyMock.expect(mock.getAutoCompactionSnapshotForDataSource(dataSourceName))
            .andReturn(null).once();
    EasyMock.replay(mock);

    final Response response = new CoordinatorCompactionResource(mock)
        .getCompactionSnapshotForDataSource(dataSourceName);
    Assert.assertEquals(404, response.getStatus());
  }

  @Test
  public void testGetProgressForNullDatasourceReturnsBadRequest()
  {
    EasyMock.replay(mock);

    final Response response = new CoordinatorCompactionResource(mock)
        .getCompactionProgress(null);
    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());

    final Object responseEntity = response.getEntity();
    Assert.assertTrue(responseEntity instanceof ErrorResponse);

    MatcherAssert.assertThat(
        ((ErrorResponse) responseEntity).getUnderlyingException(),
        DruidExceptionMatcher.invalidInput().expectMessageIs("No DataSource specified")
    );
  }
}
