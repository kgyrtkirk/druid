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

package org.apache.druid.guice;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.ProvisionException;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class StorageNodeModuleTest
{
  private static final boolean INJECT_SERVER_TYPE_CONFIG = true;

  @Mock(answer = Answers.RETURNS_MOCKS)
  private DruidNode self;
  @Mock(answer = Answers.RETURNS_MOCKS)
  private ServerTypeConfig serverTypeConfig;
  @Mock
  private DruidProcessingConfig druidProcessingConfig;
  @Mock
  private SegmentLoaderConfig segmentLoaderConfig;
  @Mock
  private StorageLocationConfig storageLocation;

  private Injector injector;
  private StorageNodeModule target;

  @BeforeEach
  public void setUp()
  {
    Mockito.when(segmentLoaderConfig.getLocations()).thenReturn(Collections.singletonList(storageLocation));

    target = new StorageNodeModule();
    injector = makeInjector(INJECT_SERVER_TYPE_CONFIG);
  }

  @Test
  public void testIsSegmentCacheConfiguredIsInjected()
  {
    Boolean isSegmentCacheConfigured = injector.getInstance(
        Key.get(Boolean.class, Names.named(StorageNodeModule.IS_SEGMENT_CACHE_CONFIGURED))
    );
    Assertions.assertNotNull(isSegmentCacheConfigured);
    Assertions.assertTrue(isSegmentCacheConfigured);
  }

  @Test
  public void testIsSegmentCacheConfiguredWithNoLocationsConfiguredIsInjected()
  {
    mockSegmentCacheNotConfigured();
    Boolean isSegmentCacheConfigured = injector.getInstance(
        Key.get(Boolean.class, Names.named(StorageNodeModule.IS_SEGMENT_CACHE_CONFIGURED))
    );
    Assertions.assertNotNull(isSegmentCacheConfigured);
    Assertions.assertFalse(isSegmentCacheConfigured);
  }

  @Test
  public void getDataNodeServiceWithNoServerTypeConfigShouldThrowProvisionException()
  {
    Throwable exception = assertThrows(ProvisionException.class, () -> {
      injector = makeInjector(!INJECT_SERVER_TYPE_CONFIG);
      injector.getInstance(DataNodeService.class);
    });
    assertTrue(exception.getMessage().contains("Must override the binding for ServerTypeConfig if you want a DataNodeService."));
  }

  @Test
  public void getDataNodeServiceWithNoSegmentCacheConfiguredThrowProvisionException()
  {
    Throwable exception = assertThrows(ProvisionException.class, () -> {
      Mockito.doReturn(ServerType.HISTORICAL).when(serverTypeConfig).getServerType();
      mockSegmentCacheNotConfigured();
      injector.getInstance(DataNodeService.class);
    });
    assertTrue(exception.getMessage().contains("druid.segmentCache.locations must be set on historicals."));
  }

  @Test
  public void getDataNodeServiceIsInjectedAsSingleton()
  {
    DataNodeService dataNodeService = injector.getInstance(DataNodeService.class);
    Assertions.assertNotNull(dataNodeService);
    DataNodeService other = injector.getInstance(DataNodeService.class);
    Assertions.assertSame(dataNodeService, other);
  }

  @Test
  public void getDataNodeServiceIsInjectedAndDiscoverable()
  {
    DataNodeService dataNodeService = injector.getInstance(DataNodeService.class);
    Assertions.assertNotNull(dataNodeService);
    Assertions.assertTrue(dataNodeService.isDiscoverable());
  }

  @Test
  public void getDataNodeServiceWithSegmentCacheNotConfiguredIsInjectedAndDiscoverable()
  {
    mockSegmentCacheNotConfigured();
    DataNodeService dataNodeService = injector.getInstance(DataNodeService.class);
    Assertions.assertNotNull(dataNodeService);
    Assertions.assertFalse(dataNodeService.isDiscoverable());
  }

  @Test
  public void testDruidServerMetadataIsInjectedAsSingleton()
  {
    DruidServerMetadata druidServerMetadata = injector.getInstance(DruidServerMetadata.class);
    Assertions.assertNotNull(druidServerMetadata);
    DruidServerMetadata other = injector.getInstance(DruidServerMetadata.class);
    Assertions.assertSame(druidServerMetadata, other);
  }

  @Test
  public void testDruidServerMetadataWithNoServerTypeConfigShouldThrowProvisionException()
  {
    Throwable exception = assertThrows(ProvisionException.class, () -> {
      injector = makeInjector(!INJECT_SERVER_TYPE_CONFIG);
      injector.getInstance(DruidServerMetadata.class);
    });
    assertTrue(exception.getMessage().contains("Must override the binding for ServerTypeConfig if you want a DruidServerMetadata."));
  }

  private Injector makeInjector(boolean withServerTypeConfig)
  {
    return Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), (ImmutableList.of(Modules.override(
            (binder) -> {
              binder.bind(DruidNode.class).annotatedWith(Self.class).toInstance(self);
              binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
              binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
              binder.bind(DruidProcessingConfig.class).toInstance(druidProcessingConfig);
            },
            target).with(
              (binder) -> {
                binder.bind(SegmentLoaderConfig.class).toInstance(segmentLoaderConfig);
                if (withServerTypeConfig) {
                  binder.bind(ServerTypeConfig.class).toInstance(serverTypeConfig);
                }
              }
                                                                )
        )));
  }

  private void mockSegmentCacheNotConfigured()
  {
    Mockito.doReturn(Collections.emptyList()).when(segmentLoaderConfig).getLocations();
  }
}
