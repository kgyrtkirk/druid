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

package org.apache.druid.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexer.TaskIdentifier;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.metadata.TaskLookup.ActiveTaskLookup;
import org.apache.druid.metadata.TaskLookup.CompleteTaskLookup;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SQLMetadataStorageActionHandlerTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private static final Random RANDOM = new Random(1);

  private SQLMetadataStorageActionHandler<Map<String, Object>, Map<String, Object>, Map<String, String>, Map<String, Object>> handler;

  private final String entryTable = "entries";

  @BeforeEach
  public void setUp()
  {
    TestDerbyConnector connector = derbyConnectorRule.getConnector();

    final String entryType = "entry";
    final String logTable = "logs";
    final String lockTable = "locks";

    connector.prepareTaskEntryTable(entryTable);
    connector.createLockTable(lockTable, entryType);
    connector.createLogTable(logTable, entryType);

    handler = new DerbyMetadataStorageActionHandler<>(
        connector,
        JSON_MAPPER,
        new MetadataStorageActionHandlerTypes<Map<String, Object>, Map<String, Object>, Map<String, String>, Map<String, Object>>()
        {
          @Override
          public TypeReference<Map<String, Object>> getEntryType()
          {
            return new TypeReference<Map<String, Object>>()
            {
            };
          }

          @Override
          public TypeReference<Map<String, Object>> getStatusType()
          {
            return new TypeReference<Map<String, Object>>()
            {
            };
          }

          @Override
          public TypeReference<Map<String, String>> getLogType()
          {
            return JacksonUtils.TYPE_REFERENCE_MAP_STRING_STRING;
          }

          @Override
          public TypeReference<Map<String, Object>> getLockType()
          {
            return new TypeReference<Map<String, Object>>()
            {
            };
          }
        },
        entryType,
        entryTable,
        logTable,
        lockTable
    );
  }

  @Test
  public void testEntryAndStatus()
  {
    Map<String, Object> entry = ImmutableMap.of("numericId", 1234);
    Map<String, Object> status1 = ImmutableMap.of("count", 42);
    Map<String, Object> status2 = ImmutableMap.of("count", 42, "temp", 1);

    final String entryId = "1234";

    handler.insert(entryId, DateTimes.of("2014-01-02T00:00:00.123"), "testDataSource", entry, true, null, "type", "group");

    Assertions.assertEquals(
        Optional.of(entry),
        handler.getEntry(entryId)
    );

    Assertions.assertEquals(Optional.absent(), handler.getEntry("non_exist_entry"));

    Assertions.assertEquals(Optional.absent(), handler.getStatus(entryId));

    Assertions.assertEquals(Optional.absent(), handler.getStatus("non_exist_entry"));

    Assertions.assertTrue(handler.setStatus(entryId, true, status1));

    Assertions.assertEquals(
        ImmutableList.of(Pair.of(entry, status1)),
        handler.getTaskInfos(ActiveTaskLookup.getInstance(), null).stream()
               .map(taskInfo -> Pair.of(taskInfo.getTask(), taskInfo.getStatus()))
               .collect(Collectors.toList())
    );

    Assertions.assertTrue(handler.setStatus(entryId, true, status2));

    Assertions.assertEquals(
        ImmutableList.of(Pair.of(entry, status2)),
        handler.getTaskInfos(ActiveTaskLookup.getInstance(), null).stream()
               .map(taskInfo -> Pair.of(taskInfo.getTask(), taskInfo.getStatus()))
               .collect(Collectors.toList())
    );

    Assertions.assertEquals(
        ImmutableList.of(),
        handler.getTaskInfos(CompleteTaskLookup.withTasksCreatedPriorTo(null, DateTimes.of("2014-01-01")), null)
    );

    Assertions.assertTrue(handler.setStatus(entryId, false, status1));

    Assertions.assertEquals(
        Optional.of(status1),
        handler.getStatus(entryId)
    );

    // inactive statuses cannot be updated, this should fail
    Assertions.assertFalse(handler.setStatus(entryId, false, status2));

    Assertions.assertEquals(
        Optional.of(status1),
        handler.getStatus(entryId)
    );

    Assertions.assertEquals(
        Optional.of(entry),
        handler.getEntry(entryId)
    );

    Assertions.assertEquals(
        ImmutableList.of(),
        handler.getTaskInfos(CompleteTaskLookup.withTasksCreatedPriorTo(null, DateTimes.of("2014-01-03")), null)
    );

    Assertions.assertEquals(
        ImmutableList.of(status1),
        handler.getTaskInfos(CompleteTaskLookup.withTasksCreatedPriorTo(null, DateTimes.of("2014-01-01")), null)
               .stream()
               .map(TaskInfo::getStatus)
               .collect(Collectors.toList())
    );
  }

  @Test
  public void testGetRecentStatuses() throws EntryExistsException
  {
    for (int i = 1; i < 11; i++) {
      final String entryId = "abcd_" + i;
      final Map<String, Object> entry = ImmutableMap.of("a", i);
      final Map<String, Object> status = ImmutableMap.of("count", i * 10);

      handler.insert(entryId, DateTimes.of(StringUtils.format("2014-01-%02d", i)), "test", entry, false, status, "type", "group");
    }

    final List<TaskInfo<Map<String, Object>, Map<String, Object>>> statuses = handler.getTaskInfos(
        CompleteTaskLookup.withTasksCreatedPriorTo(
            7,
            DateTimes.of("2014-01-01")
        ),
        null
    );
    Assertions.assertEquals(7, statuses.size());
    int i = 10;
    for (TaskInfo<Map<String, Object>, Map<String, Object>> status : statuses) {
      Assertions.assertEquals(ImmutableMap.of("count", i-- * 10), status.getStatus());
    }
  }

  @Test
  public void testGetRecentStatuses2() throws EntryExistsException
  {
    for (int i = 1; i < 6; i++) {
      final String entryId = "abcd_" + i;
      final Map<String, Object> entry = ImmutableMap.of("a", i);
      final Map<String, Object> status = ImmutableMap.of("count", i * 10);

      handler.insert(entryId, DateTimes.of(StringUtils.format("2014-01-%02d", i)), "test", entry, false, status, "type", "group");
    }

    final List<TaskInfo<Map<String, Object>, Map<String, Object>>> statuses = handler.getTaskInfos(
        CompleteTaskLookup.withTasksCreatedPriorTo(
            10,
            DateTimes.of("2014-01-01")
        ),
        null
    );
    Assertions.assertEquals(5, statuses.size());
    int i = 5;
    for (TaskInfo<Map<String, Object>, Map<String, Object>> status : statuses) {
      Assertions.assertEquals(ImmutableMap.of("count", i-- * 10), status.getStatus());
    }
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testDuplicateInsertThrowsEntryExistsException()
  {
    final String entryId = "abcd";
    Map<String, Object> entry = ImmutableMap.of("a", 1);
    Map<String, Object> status = ImmutableMap.of("count", 42);

    handler.insert(entryId, DateTimes.of("2014-01-01"), "test", entry, true, status, "type", "group");
    Assertions.assertThrows(
        EntryExistsException.class,
        () -> handler.insert(entryId, DateTimes.of("2014-01-01"), "test", entry, true, status, "type", "group")
    );
  }

  @Test
  public void testLogs()
  {
    final String entryId = "abcd";
    Map<String, Object> entry = ImmutableMap.of("a", 1);
    Map<String, Object> status = ImmutableMap.of("count", 42);

    handler.insert(entryId, DateTimes.of("2014-01-01"), "test", entry, true, status, "type", "group");

    Assertions.assertEquals(
        ImmutableList.of(),
        handler.getLogs("non_exist_entry")
    );

    Assertions.assertEquals(
        ImmutableMap.of(),
        handler.getLocks(entryId)
    );

    final ImmutableMap<String, String> log1 = ImmutableMap.of("logentry", "created");
    final ImmutableMap<String, String> log2 = ImmutableMap.of("logentry", "updated");

    Assertions.assertTrue(handler.addLog(entryId, log1));
    Assertions.assertTrue(handler.addLog(entryId, log2));

    Assertions.assertEquals(
        ImmutableList.of(log1, log2),
        handler.getLogs(entryId)
    );
  }


  @Test
  public void testLocks()
  {
    final String entryId = "ABC123";
    Map<String, Object> entry = ImmutableMap.of("a", 1);
    Map<String, Object> status = ImmutableMap.of("count", 42);

    handler.insert(entryId, DateTimes.of("2014-01-01"), "test", entry, true, status, "type", "group");

    Assertions.assertEquals(
        ImmutableMap.<Long, Map<String, Object>>of(),
        handler.getLocks("non_exist_entry")
    );

    Assertions.assertEquals(
        ImmutableMap.<Long, Map<String, Object>>of(),
        handler.getLocks(entryId)
    );

    final ImmutableMap<String, Object> lock1 = ImmutableMap.of("lock", 1);
    final ImmutableMap<String, Object> lock2 = ImmutableMap.of("lock", 2);

    Assertions.assertTrue(handler.addLock(entryId, lock1));
    Assertions.assertTrue(handler.addLock(entryId, lock2));

    final Map<Long, Map<String, Object>> locks = handler.getLocks(entryId);
    Assertions.assertEquals(2, locks.size());

    Assertions.assertEquals(
        ImmutableSet.<Map<String, Object>>of(lock1, lock2),
        new HashSet<>(locks.values())
    );

    long lockId = locks.keySet().iterator().next();
    handler.removeLock(lockId);
    locks.remove(lockId);

    final Map<Long, Map<String, Object>> updated = handler.getLocks(entryId);
    Assertions.assertEquals(
        new HashSet<>(locks.values()),
        new HashSet<>(updated.values())
    );
    Assertions.assertEquals(updated.keySet(), locks.keySet());
  }

  @Test
  public void testReplaceLock() throws EntryExistsException
  {
    final String entryId = "ABC123";
    Map<String, Object> entry = ImmutableMap.of("a", 1);
    Map<String, Object> status = ImmutableMap.of("count", 42);

    handler.insert(entryId, DateTimes.of("2014-01-01"), "test", entry, true, status, "type", "group");

    Assertions.assertEquals(
        ImmutableMap.<Long, Map<String, Object>>of(),
        handler.getLocks("non_exist_entry")
    );

    Assertions.assertEquals(
        ImmutableMap.<Long, Map<String, Object>>of(),
        handler.getLocks(entryId)
    );

    final ImmutableMap<String, Object> lock1 = ImmutableMap.of("lock", 1);
    final ImmutableMap<String, Object> lock2 = ImmutableMap.of("lock", 2);

    Assertions.assertTrue(handler.addLock(entryId, lock1));

    final Long lockId1 = handler.getLockId(entryId, lock1);
    Assertions.assertNotNull(lockId1);

    Assertions.assertTrue(handler.replaceLock(entryId, lockId1, lock2));
  }

  @Test
  public void testGetLockId() throws EntryExistsException
  {
    final String entryId = "ABC123";
    Map<String, Object> entry = ImmutableMap.of("a", 1);
    Map<String, Object> status = ImmutableMap.of("count", 42);

    handler.insert(entryId, DateTimes.of("2014-01-01"), "test", entry, true, status, "type", "group");

    Assertions.assertEquals(
        ImmutableMap.<Long, Map<String, Object>>of(),
        handler.getLocks("non_exist_entry")
    );

    Assertions.assertEquals(
        ImmutableMap.<Long, Map<String, Object>>of(),
        handler.getLocks(entryId)
    );

    final ImmutableMap<String, Object> lock1 = ImmutableMap.of("lock", 1);
    final ImmutableMap<String, Object> lock2 = ImmutableMap.of("lock", 2);

    Assertions.assertTrue(handler.addLock(entryId, lock1));

    Assertions.assertNotNull(handler.getLockId(entryId, lock1));
    Assertions.assertNull(handler.getLockId(entryId, lock2));
  }

  @Test
  public void testRemoveTasksOlderThan()
  {
    final String entryId1 = "1234";
    Map<String, Object> entry1 = ImmutableMap.of("numericId", 1234);
    Map<String, Object> status1 = ImmutableMap.of("count", 42, "temp", 1);
    handler.insert(entryId1, DateTimes.of("2014-01-01T00:00:00.123"), "testDataSource", entry1, false, status1, "type", "group");
    Assertions.assertTrue(handler.addLog(entryId1, ImmutableMap.of("logentry", "created")));

    final String entryId2 = "ABC123";
    Map<String, Object> entry2 = ImmutableMap.of("a", 1);
    Map<String, Object> status2 = ImmutableMap.of("count", 42);
    handler.insert(entryId2, DateTimes.of("2014-01-01T00:00:00.123"), "test", entry2, true, status2, "type", "group");
    Assertions.assertTrue(handler.addLog(entryId2, ImmutableMap.of("logentry", "created")));

    final String entryId3 = "DEF5678";
    Map<String, Object> entry3 = ImmutableMap.of("numericId", 5678);
    Map<String, Object> status3 = ImmutableMap.of("count", 21, "temp", 2);
    handler.insert(entryId3, DateTimes.of("2014-01-02T12:00:00.123"), "testDataSource", entry3, false, status3, "type", "group");
    Assertions.assertTrue(handler.addLog(entryId3, ImmutableMap.of("logentry", "created")));

    Assertions.assertEquals(Optional.of(entry1), handler.getEntry(entryId1));
    Assertions.assertEquals(Optional.of(entry2), handler.getEntry(entryId2));
    Assertions.assertEquals(Optional.of(entry3), handler.getEntry(entryId3));

    Assertions.assertEquals(
        ImmutableList.of(entryId2),
        handler.getTaskInfos(ActiveTaskLookup.getInstance(), null).stream()
               .map(taskInfo -> taskInfo.getId())
               .collect(Collectors.toList())
    );

    Assertions.assertEquals(
        ImmutableList.of(entryId3, entryId1),
        handler.getTaskInfos(CompleteTaskLookup.withTasksCreatedPriorTo(null, DateTimes.of("2014-01-01")), null)
               .stream()
               .map(taskInfo -> taskInfo.getId())
               .collect(Collectors.toList())

    );

    handler.removeTasksOlderThan(DateTimes.of("2014-01-02").getMillis());
    // active task not removed.
    Assertions.assertEquals(
        ImmutableList.of(entryId2),
        handler.getTaskInfos(ActiveTaskLookup.getInstance(), null).stream()
               .map(taskInfo -> taskInfo.getId())
               .collect(Collectors.toList())
    );
    Assertions.assertEquals(
        ImmutableList.of(entryId3),
        handler.getTaskInfos(CompleteTaskLookup.withTasksCreatedPriorTo(null, DateTimes.of("2014-01-01")), null)
               .stream()
               .map(taskInfo -> taskInfo.getId())
               .collect(Collectors.toList())

    );
    // tasklogs
    Assertions.assertEquals(0, handler.getLogs(entryId1).size());
    Assertions.assertEquals(1, handler.getLogs(entryId2).size());
    Assertions.assertEquals(1, handler.getLogs(entryId3).size());
  }

  @Test
  public void testMigration()
  {
    int numActiveTasks = 123;
    for (int i = 0; i < numActiveTasks; i++) {
      insertTaskInfo(createRandomTaskInfo(TaskState.RUNNING), false);
    }

    int numCompletedTasks = 101;
    for (int i = 0; i < numCompletedTasks; i++) {
      insertTaskInfo(createRandomTaskInfo(TaskState.SUCCESS), false);
    }

    Assertions.assertEquals(numActiveTasks + numCompletedTasks, getUnmigratedTaskCount().intValue());

    handler.populateTaskTypeAndGroupId();

    Assertions.assertEquals(0, getUnmigratedTaskCount().intValue());
  }

  @Test
  public void testGetTaskStatusPlusListInternal()
  {
    // SETUP
    TaskInfo<Map<String, Object>, Map<String, Object>> activeUnaltered = createRandomTaskInfo(TaskState.RUNNING);
    insertTaskInfo(activeUnaltered, false);

    TaskInfo<Map<String, Object>, Map<String, Object>> completedUnaltered = createRandomTaskInfo(TaskState.SUCCESS);
    insertTaskInfo(completedUnaltered, false);

    TaskInfo<Map<String, Object>, Map<String, Object>> activeAltered = createRandomTaskInfo(TaskState.RUNNING);
    insertTaskInfo(activeAltered, true);

    TaskInfo<Map<String, Object>, Map<String, Object>> completedAltered = createRandomTaskInfo(TaskState.SUCCESS);
    insertTaskInfo(completedAltered, true);

    Map<TaskLookup.TaskLookupType, TaskLookup> taskLookups = new HashMap<>();
    taskLookups.put(TaskLookup.TaskLookupType.ACTIVE, ActiveTaskLookup.getInstance());
    taskLookups.put(TaskLookup.TaskLookupType.COMPLETE, CompleteTaskLookup.of(null, Duration.millis(86400000)));

    List<TaskInfo<TaskIdentifier, Map<String, Object>>> taskMetadataInfos;

    // BEFORE MIGRATION

    // Payload based fetch. task type and groupid will be populated
    taskMetadataInfos = handler.getTaskStatusList(taskLookups, null, true);
    Assertions.assertEquals(4, taskMetadataInfos.size());
    verifyTaskInfoToMetadataInfo(completedUnaltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(completedAltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(activeUnaltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(activeAltered, taskMetadataInfos, false);

    // New columns based fetch before migration is complete. type and payload are null when altered = false
    taskMetadataInfos = handler.getTaskStatusList(taskLookups, null, false);
    Assertions.assertEquals(4, taskMetadataInfos.size());
    verifyTaskInfoToMetadataInfo(completedUnaltered, taskMetadataInfos, true);
    verifyTaskInfoToMetadataInfo(completedAltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(activeUnaltered, taskMetadataInfos, true);
    verifyTaskInfoToMetadataInfo(activeAltered, taskMetadataInfos, false);

    // MIGRATION
    handler.populateTaskTypeAndGroupId();

    // Payload based fetch. task type and groupid will still be populated in tasks tab
    taskMetadataInfos = handler.getTaskStatusList(taskLookups, null, true);
    Assertions.assertEquals(4, taskMetadataInfos.size());
    verifyTaskInfoToMetadataInfo(completedUnaltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(completedAltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(activeUnaltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(activeAltered, taskMetadataInfos, false);

    // New columns based fetch after migration is complete. All data must be populated in the tasks table
    taskMetadataInfos = handler.getTaskStatusList(taskLookups, null, false);
    Assertions.assertEquals(4, taskMetadataInfos.size());
    verifyTaskInfoToMetadataInfo(completedUnaltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(completedAltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(activeUnaltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(activeAltered, taskMetadataInfos, false);
  }

  private Integer getUnmigratedTaskCount()
  {
    return handler.getConnector().retryWithHandle(
        handle -> {
          String sql = StringUtils.format(
              "SELECT COUNT(*) FROM %s WHERE type is NULL or group_id is NULL",
              entryTable
          );
          ResultSet resultSet = handle.getConnection().createStatement().executeQuery(sql);
          resultSet.next();
          return resultSet.getInt(1);
        }
    );
  }

  private TaskInfo<Map<String, Object>, Map<String, Object>> createRandomTaskInfo(TaskState taskState)
  {
    String id = UUID.randomUUID().toString();
    DateTime createdTime = DateTime.now(DateTimeZone.UTC);
    String datasource = UUID.randomUUID().toString();
    String type = UUID.randomUUID().toString();
    String groupId = UUID.randomUUID().toString();

    Map<String, Object> payload = new HashMap<>();
    payload.put("id", id);
    payload.put("type", type);
    payload.put("groupId", groupId);

    Map<String, Object> status = new HashMap<>();
    status.put("id", id);
    status.put("status", taskState);
    status.put("duration", RANDOM.nextLong());
    status.put("location", TaskLocation.create(UUID.randomUUID().toString(), 8080, 995));
    status.put("errorMsg", UUID.randomUUID().toString());

    return new TaskInfo<>(
        id,
        createdTime,
        status,
        datasource,
        payload
    );
  }

  private void insertTaskInfo(TaskInfo<Map<String, Object>, Map<String, Object>> taskInfo, boolean altered)
  {
    try {
      handler.insert(
          taskInfo.getId(),
          taskInfo.getCreatedTime(),
          taskInfo.getDataSource(),
          taskInfo.getTask(),
          TaskState.RUNNING.equals(taskInfo.getStatus().get("status")),
          taskInfo.getStatus(),
          altered ? taskInfo.getTask().get("type").toString() : null,
          altered ? taskInfo.getTask().get("groupId").toString() : null
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void verifyTaskInfoToMetadataInfo(TaskInfo<Map<String, Object>, Map<String, Object>> taskInfo,
                                            List<TaskInfo<TaskIdentifier, Map<String, Object>>> taskMetadataInfos,
                                            boolean nullNewColumns)
  {
    for (TaskInfo<TaskIdentifier, Map<String, Object>> taskMetadataInfo : taskMetadataInfos) {
      if (taskMetadataInfo.getId().equals(taskInfo.getId())) {
        verifyTaskInfoToMetadataInfo(taskInfo, taskMetadataInfo, nullNewColumns);
      }
      return;
    }
    Assertions.fail();
  }

  private void verifyTaskInfoToMetadataInfo(TaskInfo<Map<String, Object>, Map<String, Object>> taskInfo,
                                            TaskInfo<TaskIdentifier, Map<String, Object>> taskMetadataInfo,
                                            boolean nullNewColumns)
  {
    Assertions.assertEquals(taskInfo.getId(), taskMetadataInfo.getId());
    Assertions.assertEquals(taskInfo.getCreatedTime(), taskMetadataInfo.getCreatedTime());
    Assertions.assertEquals(taskInfo.getDataSource(), taskMetadataInfo.getDataSource());

    verifyTaskStatus(taskInfo.getStatus(), taskMetadataInfo.getStatus());

    Map<String, Object> task = taskInfo.getTask();
    TaskIdentifier taskIdentifier = taskMetadataInfo.getTask();
    Assertions.assertEquals(task.get("id"), taskIdentifier.getId());
    if (nullNewColumns) {
      Assertions.assertNull(taskIdentifier.getGroupId());
      Assertions.assertNull(taskIdentifier.getType());
    } else {
      Assertions.assertEquals(task.get("groupId"), taskIdentifier.getGroupId());
      Assertions.assertEquals(task.get("type"), taskIdentifier.getType());
    }
  }

  private void verifyTaskStatus(Map<String, Object> expected, Map<String, Object> actual)
  {
    Assertions.assertEquals(expected.get("id"), actual.get("id"));
    Assertions.assertEquals(expected.get("duration"), actual.get("duration"));
    Assertions.assertEquals(expected.get("errorMsg"), actual.get("errorMsg"));
    Assertions.assertEquals(expected.get("status").toString(), actual.get("status"));
    Assertions.assertEquals(expected.get("location"), JSON_MAPPER.convertValue(actual.get("location"), TaskLocation.class));
  }
}
