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

package org.apache.druid.curator;

import com.google.inject.Injector;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.testing.junit.LoggerCaptureRule;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;

public final class CuratorModuleTest
{
  private static final String CURATOR_CONNECTION_TIMEOUT_MS_KEY =
      CuratorConfig.CONFIG_PREFIX + "." + CuratorConfig.CONNECTION_TIMEOUT_MS;

  @Rule
  public final ExpectedSystemExit exit = ExpectedSystemExit.none();

  @Rule
  public final LoggerCaptureRule logger = new LoggerCaptureRule(CuratorModule.class);

  @Test
  public void createsCuratorFrameworkAsConfigured()
  {
    CuratorConfig config = CuratorConfig.create("myhost1:2888,myhost2:2888");
    CuratorFramework curatorFramework = CuratorModule.createCurator(config);
    CuratorZookeeperClient client = curatorFramework.getZookeeperClient();

    Assertions.assertEquals(config.getZkHosts(), client.getCurrentConnectionString());
    Assertions.assertEquals(config.getZkConnectionTimeoutMs(), client.getConnectionTimeoutMs());

    assertThat(client.getRetryPolicy(), Matchers.instanceOf(BoundedExponentialBackoffRetry.class));
    BoundedExponentialBackoffRetry retryPolicy = (BoundedExponentialBackoffRetry) client.getRetryPolicy();
    Assertions.assertEquals(CuratorModule.BASE_SLEEP_TIME_MS, retryPolicy.getBaseSleepTimeMs());
    Assertions.assertEquals(CuratorModule.MAX_SLEEP_TIME_MS, retryPolicy.getMaxSleepTimeMs());
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void exitsJvmWhenMaxRetriesExceeded() throws Exception
  {
    Properties props = new Properties();
    props.setProperty(CURATOR_CONNECTION_TIMEOUT_MS_KEY, "0");
    Injector injector = newInjector(props);

    logger.clearLogEvents();
    exit.expectSystemExitWithStatus(1);

    // This will result in a curator unhandled error since the connection timeout is 0 and retries are disabled
    CuratorFramework curatorFramework = createCuratorFramework(injector, 0);
    curatorFramework.start();
    curatorFramework.create().inBackground().forPath("/foo");

    // org.apache.curator.framework.impl.CuratorFrameworkImpl logs "Background retry gave up" unhandled error twice
    logger.awaitLogEvents();
    List<LogEvent> loggingEvents = logger.getLogEvents();

    Assertions.assertTrue(
        loggingEvents.stream()
                     .anyMatch(l ->
                                   l.getLevel().equals(Level.ERROR)
                                   && l.getMessage()
                                       .getFormattedMessage()
                                       .equals("Unhandled error in Curator, stopping server.")
                     ),
        "Logging events: " + loggingEvents
    );
  }

  @Disabled("Verifies changes in https://github.com/apache/druid/pull/8458, but overkill for regular testing")
  @Test
  public void ignoresDeprecatedCuratorConfigProperties()
  {
    Properties props = new Properties();
    String deprecatedPropName = CuratorConfig.CONFIG_PREFIX + ".terminateDruidProcessOnConnectFail";
    props.setProperty(deprecatedPropName, "true");
    Injector injector = newInjector(props);

    try {
      injector.getInstance(CuratorFramework.class);
    }
    catch (Exception e) {
      Assertions.fail("Deprecated curator config was not ignored:\n" + e);
    }
  }

  private Injector newInjector(final Properties props)
  {
    return new StartupInjectorBuilder()
        .add(
            new LifecycleModule(),
            new CuratorModule(false),
            binder -> binder.bind(ServiceEmitter.class).to(NoopServiceEmitter.class),
            binder -> binder.bind(Properties.class).toInstance(props)
        )
        .build();
  }

  private static CuratorFramework createCuratorFramework(Injector injector, int maxRetries)
  {
    CuratorFramework curatorFramework = injector.getInstance(CuratorFramework.class);
    RetryPolicy retryPolicy = curatorFramework.getZookeeperClient().getRetryPolicy();
    assertThat(retryPolicy, CoreMatchers.instanceOf(ExponentialBackoffRetry.class));
    RetryPolicy adjustedRetryPolicy = adjustRetryPolicy((BoundedExponentialBackoffRetry) retryPolicy, maxRetries);
    curatorFramework.getZookeeperClient().setRetryPolicy(adjustedRetryPolicy);
    return curatorFramework;
  }

  private static RetryPolicy adjustRetryPolicy(BoundedExponentialBackoffRetry origRetryPolicy, int maxRetries)
  {
    return new BoundedExponentialBackoffRetry(
        origRetryPolicy.getBaseSleepTimeMs(),
        origRetryPolicy.getMaxSleepTimeMs(),
        maxRetries
    );
  }
}
