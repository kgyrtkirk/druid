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

package org.apache.druid.server;

import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import org.apache.druid.query.Query;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public interface EtagProvider
{
  static Key<EtagProvider> KEY = Key.get(EtagProvider.class, EtagProvider.Annotation.class);

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD})
  @BindingAnnotation
  static @interface Annotation
  {
  }

  String getEtagFor(Query query);

  static class EmptyEtagProvider implements EtagProvider
  {
    @Override
    public String getEtagFor(Query query)
    {
      return null;
    }
  }

  static class AlwaysNewEtagProvider implements EtagProvider
  {
    AtomicInteger etagSerial = new AtomicInteger();

    @Override
    public String getEtagFor(Query query)
    {
      if (query.getDataSource().isCacheable(true)) {
        return "TEST_ETAG-" + etagSerial.incrementAndGet();
      } else {
        return null;
      }
    }
  }

  static class UseProvidedIfAvaliable extends AlwaysNewEtagProvider
  {
    @Override
    public String getEtagFor(Query query)
    {
      if (query.getDataSource().isCacheable(true)) {
        byte[] cacheKey = query.getDataSource().getCacheKey();
        return "ETP-" + Arrays.hashCode(cacheKey);
      } else {
        return null;
      }
    }
  }
}
