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

package org.apache.druid.sql.avatica;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter.SerializeExceptFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.guice.MultiStageQuery;
import org.apache.druid.msq.indexing.report.MSQResultsReport.ColumnAndType;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.test.MSQTestBase;
import org.apache.druid.msq.test.MSQTestOverlordServiceClient;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.sql.hook.DruidHook;
import org.apache.druid.sql.hook.DruidHookDispatcher;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class MSQDruidMeta extends DruidMeta
{
  protected final MSQTestOverlordServiceClient overlordClient;
  protected final ObjectMapper objectMapper;
  protected final DruidHookDispatcher hookDispatcher;

  @Inject
  public MSQDruidMeta(
      final @MultiStageQuery SqlStatementFactory sqlStatementFactory,
      final AvaticaServerConfig config,
      final ErrorHandler errorHandler,
      final AuthenticatorMapper authMapper,
      final MSQTestOverlordServiceClient overlordClient,
      final ObjectMapper objectMapper,
      final DruidHookDispatcher hookDispatcher
      )
  {
    super(sqlStatementFactory, config, errorHandler, authMapper);
    this.overlordClient = overlordClient;
    this.objectMapper = objectMapper;
    this.hookDispatcher = hookDispatcher;
  }

  @Override
  protected ExecuteResult doFetch(AbstractDruidJdbcStatement druidStatement, int maxRows)
  {
    String taskId = extractTaskId(druidStatement);

    MSQTaskReportPayload payload = (MSQTaskReportPayload) overlordClient.getReportForTask(taskId)
        .get(MSQTaskReport.REPORT_KEY)
        .getPayload();
    if (payload.getStatus().getStatus().isFailure()) {
      throw new ISE(
          "Query task [%s] failed due to %s",
          taskId,
          payload.getStatus().getErrorReport().toString()
      );
    }

    if (!payload.getStatus().getStatus().isComplete()) {
      throw new ISE("Query task [%s] should have finished", taskId);
    }
    final List<?> resultRows = MSQTestBase.getRows(payload.getResults());
    if (resultRows == null) {
      throw new ISE("Results report not present in the task's report payload");
    }
    try {
//      objectMapper.setexc
//      objectMapper.wit

      Set<String> ignorableFieldNames = ImmutableSet.of("sqlQueryId", "startTime", "duration");
      SimpleBeanPropertyFilter simpleFilterProvider = SerializeExceptFilter.serializeAllExcept(ignorableFieldNames);
//      simpleFilterProvider.serializeAllExcept(ignorableFieldNames);
//      FilterProvider filters = simpleFilterProvider.addFilter("customerFilter", SimpleBeanPropertyFilter.serializeAllExcept(ignorableFieldNames));

      SerializationFeature f1;

      SimpleFilterProvider ff = new SimpleFilterProvider();
      ff.addFilter("removeSome",simpleFilterProvider);
      ;
      ObjectWriter writerWithDefaultPrettyPrinter = objectMapper.copy().setFilterProvider(ff)
          .writerWithDefaultPrettyPrinter();

      String str = writerWithDefaultPrettyPrinter
          .writeValueAsString(payload.getStages());
      str = str.replaceAll(taskId, "<taskId>");

      hookDispatcher.dispatch(DruidHook.MSQ_PLAN, str);
    }
    catch (JsonProcessingException e) {
      hookDispatcher.dispatch(DruidHook.MSQ_PLAN, "error happened during json serialization");
    }

    Signature signature = makeSignature(druidStatement, payload.getResults().getSignature());
    @SuppressWarnings("unchecked")
    Frame firstFrame = Frame.create(0, true, (List<Object>) resultRows);
    return new ExecuteResult(
        ImmutableList.of(
            MetaResultSet.create(
                druidStatement.connectionId,
                druidStatement.statementId,
                false,
                signature,
                firstFrame
            )
        )
    );
  }

  private Signature makeSignature(AbstractDruidJdbcStatement druidStatement, List<ColumnAndType> cat)
  {
    RowSignature sig = ColumnAndType.toRowSignature(cat);
    RelDataType rowType = RowSignatures.toRelDataType(sig, DruidTypeSystem.TYPE_FACTORY);
    return Meta.Signature.create(
        AbstractDruidJdbcStatement.createColumnMetaData(rowType),
        druidStatement.getSqlQuery().sql(),
        Collections.emptyList(),
        Meta.CursorFactory.ARRAY,
        Meta.StatementType.SELECT
    );

  }

  private String extractTaskId(AbstractDruidJdbcStatement druidStatement)
  {
    ExecuteResult r = super.doFetch(druidStatement, 2);
    Object[] row = (Object[]) r.resultSets.get(0).firstFrame.rows.iterator().next();
    return (String) row[0];

  }

}
