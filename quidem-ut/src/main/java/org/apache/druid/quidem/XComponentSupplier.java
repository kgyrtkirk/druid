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

import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.AttemptId;
import org.apache.druid.indexing.common.task.IndexTask.IndexIngestionSpec;
import org.apache.druid.indexing.input.InputRowSchemas;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.metadata.TestDerbyConnector.DerbyConnectorRule;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.util.DruidModuleCollection;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.apache.druid.sql.calcite.util.datasets.MapBasedTestDataset;
import org.apache.druid.sql.calcite.util.datasets.NumFoo;

import java.io.File;
import java.util.List;
import java.util.Map;

public class XComponentSupplier extends StandardComponentSupplier
{
  public XComponentSupplier(TempDirProducer tempDirProducer)
  {
    super(tempDirProducer);
  }

  @Override
  public DruidModule getCoreModule()
  {
    return DruidModuleCollection.of(
        super.getCoreModule(),
        new IndexingServiceTuningConfigModule(),
        new LocalModule()
    );
  }

  static class LocalModule implements DruidModule
  {

    @Override
    public void configure(Binder binder)
    {
    }

    @Provides
    @AttemptId
    String getAttemptId()
    {
      return "test";
    }

    @Provides
    @LazySingleton
    TestDerbyConnector makeDerbyConnector()
    {
      DerbyConnectorRule dcr = new TestDerbyConnector.DerbyConnectorRule(
          CentralizedDatasourceSchemaConfig.create(true)
      );
      dcr.before();
      // FIXME possible leak
      return dcr.getConnector();
    }

  }

  @Override
  public SpecificSegmentsQuerySegmentWalker addSegmentsToWalker(SpecificSegmentsQuerySegmentWalker walker)
  {


    IndexIngestionSpec s;

    LocalInputSource lis = new LocalInputSource(new File("/tmp/"), "wikipedia.json.gz");
    DataSchema dataSchema = null;
    InputRowSchema irs= InputRowSchemas.fromDataSchema(dataSchema);
    InputFormat ifs=null;
    lis.reader(irs, ifs, tempDirProducer.newTempFolder());
    new MapBasedTestDataset("asd")
    {

      @Override
      public List<Map<String, Object>> getRawRows()
      {
        if (true) {
          throw new RuntimeException("FIXME: Unimplemented!");
        }
        return null;

      }

      @Override
      public List<AggregatorFactory> getMetrics()
      {
        if (true) {
          throw new RuntimeException("FIXME: Unimplemented!");
        }
        return null;

      }

      @Override
      public InputRowSchema getInputRowSchema()
      {
        if (true) {
          throw new RuntimeException("FIXME: Unimplemented!");
        }
        return null;

      }
    };
    walker.add(new NumFoo("wikipediax2"), tempDirProducer.newTempFolder());
    return walker;
  }
}
