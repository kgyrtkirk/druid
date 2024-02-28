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

package org.apache.druid.sql.calcite;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.input.InputSourceModule;
import org.apache.druid.sql.calcite.CalciteIngestionDmlTest.TestFileInputSource;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.ExternalOperatorConversion;
import org.apache.druid.sql.calcite.external.HttpOperatorConversion;
import org.apache.druid.sql.calcite.external.InlineOperatorConversion;
import org.apache.druid.sql.calcite.external.LocalOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.guice.SqlBindings;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CalciteTableExternTest extends BaseCalciteQueryTest
{


  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);

    builder.addModule(new DruidModule() {

      // Clone of MSQExternalDataSourceModule since it is not
      // visible here.
      @Override
      public List<? extends Module> getJacksonModules()
      {
        return Collections.singletonList(
            new SimpleModule(getClass().getSimpleName())
                .registerSubtypes(ExternalDataSource.class)
        );
      }

      @Override
      public void configure(Binder binder)
      {
        // Nothing to do.
      }
    });

    builder.addModule(new DruidModule() {

      // Partial clone of MsqSqlModule, since that module is not
      // visible to this one.

      @Override
      public List<? extends Module> getJacksonModules()
      {
        // We want this module to bring input sources along for the ride.
        List<Module> modules = new ArrayList<>(new InputSourceModule().getJacksonModules());
        modules.add(new SimpleModule("test-module").registerSubtypes(TestFileInputSource.class));
        return modules;
      }

      @Override
      public void configure(Binder binder)
      {
        // We want this module to bring InputSourceModule along for the ride.
        binder.install(new InputSourceModule());

        // Set up the EXTERN macro.
        SqlBindings.addOperatorConversion(binder, ExternalOperatorConversion.class);

        // Enable the extended table functions for testing even though these
        // are not enabled in production in Druid 26.
        SqlBindings.addOperatorConversion(binder, HttpOperatorConversion.class);
        SqlBindings.addOperatorConversion(binder, InlineOperatorConversion.class);
        SqlBindings.addOperatorConversion(binder, LocalOperatorConversion.class);
      }
    });
  }


  @Test
  public void testInsertFromExternalWithSchema()
  {
    String extern;
    ObjectMapper queryJsonMapper = queryFramework().queryJsonMapper();
    try {
      extern = StringUtils.format(
          "TABLE(extern(%s, %s))",
          Calcites.escapeStringLiteral(
              queryJsonMapper.writeValueAsString(
                  new InlineInputSource("a,b,1\nc,d,2\n")
              )
          ),
          Calcites.escapeStringLiteral(
              queryJsonMapper.writeValueAsString(
                  new CsvInputFormat(ImmutableList.of("x", "y", "z"), null, false, false, 0)
              )
          )
      );
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    testBuilder()
        .sql(
            StringUtils.format(
                "SELECT * FROM %s\n" +
                    "  (x VARCHAR, y VARCHAR, z BIGINT)\n",
                extern
            )
        )
        .run();

  }

}