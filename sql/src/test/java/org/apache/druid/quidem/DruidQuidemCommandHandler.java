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

package org.apache.druid.quidem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import net.hydromatic.quidem.AbstractCommand;
import net.hydromatic.quidem.Command;
import net.hydromatic.quidem.CommandHandler;
import net.hydromatic.quidem.Quidem.SqlCommand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.Query;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.util.QueryLogHook;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class DruidQuidemCommandHandler implements CommandHandler
{

  @Override
  public Command parseCommand(List<String> lines, List<String> content, String line)
  {
    if (line.startsWith("nativePlan")) {
      return new NativePlanCommand(lines, content);
    }
    if (line.startsWith("logicalPlan")) {
      return new LogicalPlanCommand(lines, content);
    }
    if (line.startsWith("convertedPlan")) {
      return new LogicalPlanCommand(lines, content);
    }
    return null;
  }

  /** Command that prints the plan for the current query. */
  static class NativePlanCommand extends AbstractCommand
  {
    private final List<String> content;
    private final List<String> lines;

    NativePlanCommand(List<String> lines, List<String> content)
    {
      this.lines = ImmutableList.copyOf(lines);
      this.content = content;
    }

    public String describe(Context x)
    {
      return commandName() + " [sql: " + x.previousSqlCommand().sql + "]";
    }

    public void execute(Context x, boolean execute) throws Exception
    {
      if (execute) {
        try {
          final SqlCommand sqlCommand = x.previousSqlCommand();

          QueryLogHook qlh = new QueryLogHook(new DefaultObjectMapper());
          qlh.logQueriesForGlobal(
              () -> {
                try (
                    final Statement statement = x.connection().createStatement();
                    final ResultSet resultSet = statement.executeQuery(sqlCommand.sql);) {
                  // throw away all results
                  while (resultSet.next()) {
                  }
                }
                catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
          );
          List<Query<?>> queries = qlh.getRecordedQueries();

          ObjectMapper objectMapper = TestHelper.JSON_MAPPER;
          queries = queries
              .stream()
              .map(q -> BaseCalciteQueryTest.recursivelyClearContext(q, objectMapper))
              .collect(Collectors.toList());

          for (Query<?> query : queries) {
            String str = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(query);
            x.echo(ImmutableList.of(str));

          }
        }
        catch (Exception e) {
          throw new Error(e);
        }
      } else {
        x.echo(content);
      }
      x.echo(lines);
    }
  }

  /** Command that prints the plan for the current query. */
  static class LogicalPlanCommand extends AbstractCommand
  {
    private final List<String> content;
    private final List<String> lines;

    LogicalPlanCommand(List<String> lines, List<String> content)
    {
      this.lines = ImmutableList.copyOf(lines);
      this.content = content;
    }

    public String describe(Context x)
    {
      return commandName() + " [sql: " + x.previousSqlCommand().sql + "]";
    }

    public void execute(Context x, boolean execute) throws Exception
    {
      if (execute) {
        final SqlCommand sqlCommand = x.previousSqlCommand();

        List<RelNode> logged = new ArrayList<RelNode>();
        try (final Hook.Closeable unhook = Hook.TRIMMED.add(
            (Consumer<RelNode>) a -> logged.add(a)
        )) {
          try (
              final Statement statement = x.connection().createStatement();
              final ResultSet resultSet = statement.executeQuery(sqlCommand.sql);) {
            // throw away all results
            while (resultSet.next()) {
            }
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        for (RelNode node : logged) {
          String str = RelOptUtil.dumpPlan("", node, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES);
          x.echo(ImmutableList.of(str));
        }
      } else {
        x.echo(content);
      }
      x.echo(lines);
    }
  }

  /** Command that prints the plan for the current query. */
  static class ConvertedPlanCommand extends AbstractCommand
  {
    private final List<String> content;
    private final List<String> lines;

    ConvertedPlanCommand(List<String> lines, List<String> content)
    {
      this.lines = ImmutableList.copyOf(lines);
      this.content = content;
    }

    public String describe(Context x)
    {
      return commandName() + " [sql: " + x.previousSqlCommand().sql + "]";
    }

    public void execute(Context x, boolean execute) throws Exception
    {
      if (execute) {
        final SqlCommand sqlCommand = x.previousSqlCommand();

        List<RelNode> logged = new ArrayList<RelNode>();
        try (final Hook.Closeable unhook = Hook.CONVERTED.add(
            (Consumer<RelNode>) a -> logged.add(a)
        )) {
          try (
              final Statement statement = x.connection().createStatement();
              final ResultSet resultSet = statement.executeQuery(sqlCommand.sql);) {
            // throw away all results
            while (resultSet.next()) {
            }
          }
          catch (Exception e) {
            throw new RuntimeException(e);
          }
        }

        for (RelNode node : logged) {
          String str = RelOptUtil.dumpPlan("", node, SqlExplainFormat.TEXT, SqlExplainLevel.ALL_ATTRIBUTES);
          x.echo(ImmutableList.of(str));
        }
      } else {
        x.echo(content);
      }
      x.echo(lines);
    }
  }

}
