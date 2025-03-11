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

package org.apache.druid.msq.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.guice.JoinableFactoryModule;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Merging;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.msq.dart.Dart;
import org.apache.druid.msq.dart.controller.DartControllerContextFactory;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.msq.dart.guice.DartControllerModule;
import org.apache.druid.msq.dart.guice.DartModules;
import org.apache.druid.msq.dart.guice.DartWorkerMemoryManagementModule;
import org.apache.druid.msq.dart.guice.DartWorkerModule;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.guice.MSQExternalDataSourceModule;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.test.CalciteMSQTestsHelper.MSQTestModule;
import org.apache.druid.query.TestBufferPool;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.guice.ServiceClientModule;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.avatica.DartDruidMeta;
import org.apache.druid.sql.avatica.DruidMeta;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.util.DruidModuleCollection;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplierDelegate;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DartComponentSupplier extends QueryComponentSupplierDelegate
{
  public DartComponentSupplier(TempDirProducer tempFolderProducer)
  {
    super(new StandardComponentSupplier(tempFolderProducer));
  }

  @Override
  public void gatherProperties(Properties properties)
  {
    super.gatherProperties(properties);
    properties.put(DartModules.DART_ENABLED_PROPERTY, "true");
  }

  @Override
  public DruidModule getCoreModule()
  {
    return DruidModuleCollection.of(
        super.getCoreModule(),
        new DartControllerModule(),
        new DartWorkerModule(),
        new DartWorkerMemoryManagementModule(),
        new IndexingServiceTuningConfigModule(),
        new JoinableFactoryModule(),
        new MSQExternalDataSourceModule(),
        new MSQIndexingModule(),
        new MSQTestModule(),
        new DartTestCoreModule()
    );
  }

  @Override
  public DruidModule getOverrideModule()
  {
    return DruidModuleCollection.of(
        super.getOverrideModule(),
        new DartTestOverrideModule()
    );
  }

  @Override
  public SqlEngine createEngine(
      QueryLifecycleFactory qlf,
      ObjectMapper queryJsonMapper,
      Injector injector)
  {
    return injector.getInstance(DartSqlEngine.class);
  }

  static class DartTestCoreModule implements DruidModule
  {
    @Provides
    @EscalatedGlobal
    final ServiceClientFactory getServiceClientFactory(HttpClient ht)
    {
      return ServiceClientModule.makeServiceClientFactory(ht);
    }

    @Provides
    final DruidNodeDiscoveryProvider getDiscoveryProvider()
    {
      return null;
    }

    @Override
    public void configure(Binder binder)
    {
      binder.install(
          new FactoryModuleBuilder()
              .implement(I1.class, C1.class)
              .build(I1Factory.class)
      );
    }

  }

  static interface I1Factory
  {
    public I1 make(int asd);
  }

  static interface I1
  {
    public void i();
  }

  static class C1 implements I1
  {

    private int asd;
    private DartSqlEngine e;

    @Inject
    C1(
        DartSqlEngine e,
        @Assisted int asd)
    {
      this.e = e;
      this.asd = asd;
    }

    @Override
    public void i()
    {
      System.out.println("x" + asd+" " +e);
    }

  }

  static class DartTestOverrideModule implements DruidModule
  {

    @Provides
    @LazySingleton
    public DruidMeta createMeta(DartDruidMeta druidMeta)
    {
      return druidMeta;
    }

    @Override
    public void configure(Binder binder)
    {
      binder.bind(DartControllerContextFactory.class)
          .to(TestDartControllerContextFactoryImpl.class)
          .in(LazySingleton.class);
    }

    @Provides
    @Merging
    NonBlockingPool<ByteBuffer> makeMergingBuffer(TestBufferPool bufferPool)
    {
      return bufferPool;
    }

    @Provides
    @LazySingleton
    @Dart
    Map<String, Worker> workerMap()
    {
      return new HashMap<String, Worker>();
    }
  }
}
