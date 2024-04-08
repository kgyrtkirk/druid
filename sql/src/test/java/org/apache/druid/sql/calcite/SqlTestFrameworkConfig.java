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

import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.sql.calcite.util.CacheTestHelperModule.ResultCacheMode;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Annotation to specify desired framework settings.
 *
 * This class provides junit rule facilities to build the framework accordingly
 * to the annotation. These rules also cache the previously created frameworks.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface SqlTestFrameworkConfig
{
  int numMergeBuffers() default 0;

  int minTopNThreshold() default TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD;

  ResultCacheMode resultCache() default ResultCacheMode.DISABLED;

  /**
   * Non-annotation version of {@link SqlTestFrameworkConfig}.
   *
   * Makes it less convoluted to work with configurations created at runtime.
   */
  class SqlTestFrameworkConfigInstance
  {
    public final int numMergeBuffers;
    public final int minTopNThreshold;
    public final ResultCacheMode resultCache;

    public SqlTestFrameworkConfigInstance(SqlTestFrameworkConfig annotation)
    {
      numMergeBuffers = annotation.numMergeBuffers();
      minTopNThreshold = annotation.minTopNThreshold();
      resultCache = annotation.resultCache();
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(minTopNThreshold, numMergeBuffers, resultCache);
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      SqlTestFrameworkConfigInstance other = (SqlTestFrameworkConfigInstance) obj;
      return minTopNThreshold == other.minTopNThreshold
          && numMergeBuffers == other.numMergeBuffers
          && resultCache == other.resultCache;
    }

  }

  class SqlTestFrameworkConfigStore {

    public static final SqlTestFrameworkConfigStore INSTANCE = new SqlTestFrameworkConfigStore();

    Map<SqlTestFrameworkConfigInstance, ConfigurationInstance> configMap = new HashMap<>();

    public ConfigurationInstance getConfigurationInstance(SqlTestFrameworkConfigInstance config, QueryComponentSupplier testHost)
    {
      ConfigurationInstance ret = configMap.get(config);
      if (!configMap.containsKey(config)) {
        ret = new ConfigurationInstance(config, testHost);
        configMap.put(config, ret);
      }
      return ret;
    }

    public void close()
    {
      for (ConfigurationInstance f : configMap.values()) {
        f.close();
      }
      configMap.clear();
    }


  }

  /**
   * @see {@link SqlTestFrameworkConfig}
   */
  class Rule implements AfterAllCallback, BeforeEachCallback
  {
    SqlTestFrameworkConfigStore configStore = new SqlTestFrameworkConfigStore();
    private SqlTestFrameworkConfigInstance config;
    private QueryComponentSupplier testHost;
    private Method method;

    @Override
    public void afterAll(ExtensionContext context)
    {
      configStore.close();
    }

    @Override
    public void beforeEach(ExtensionContext context)
    {
      testHost = (QueryComponentSupplier) context.getTestInstance().get();
      method = context.getTestMethod().get();
      setConfig(method.getAnnotation(SqlTestFrameworkConfig.class));

    }

    @SqlTestFrameworkConfig
    public SqlTestFrameworkConfig defaultConfig()
    {
      try {
        SqlTestFrameworkConfig annotation = getClass()
            .getMethod("defaultConfig")
            .getAnnotation(SqlTestFrameworkConfig.class);
        return annotation;
      }
      catch (NoSuchMethodException | SecurityException e) {
        throw new RuntimeException(e);
      }
    }

    // FIXME protect
    public void setConfig(SqlTestFrameworkConfig annotation)
    {
      if(annotation == null ) {
        annotation= defaultConfig();
      }
      config = new SqlTestFrameworkConfigInstance(annotation);
    }

    public SqlTestFrameworkConfigInstance getConfig()
    {
      return config;
    }

    public SqlTestFramework get()
    {
      return configStore.getConfigurationInstance(config, testHost).framework;
    }

    public <T extends Annotation> T getAnnotation(Class<T> annotationType)
    {
      return method.getAnnotation(annotationType);
    }

    public String testName()
    {
      return method.getName();
    }
  }

  public class ConfigurationInstance
  {
    public SqlTestFramework framework;

    public ConfigurationInstance(SqlTestFrameworkConfigInstance config, QueryComponentSupplier testHost)
    {
      SqlTestFramework.Builder builder = new SqlTestFramework.Builder(testHost)
          .catalogResolver(testHost.createCatalogResolver())
          .minTopNThreshold(config.minTopNThreshold)
          .mergeBufferCount(config.numMergeBuffers)
          .withOverrideModule(config.resultCache.makeModule());
      framework = builder.build();
    }

    public void close()
    {
      framework.close();
    }
  }
}
