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

package org.apache.druid.query.planning;

import org.apache.druid.query.DataSource;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.join.JoinableFactoryWrapper;

public interface DataSourceProcessingClause
{
  static PreJoinableClause of(JoinDataSource joinDataSource)
  {
    return new PreJoinableClause(joinDataSource);
  }

  static DataSourceProcessingClause of(UnnestDataSource unnestDataSource)
  {
    return new SimpleDataSourceProcessingClause(unnestDataSource);
  }

  DataSource getDataSource();

  boolean hasColumn(String column);

  DataSource withUpdatedDataSource(DataSource current, DimFilter joinBaseFilter,
      JoinableFactoryWrapper joinableFactoryWrapper);

  void appendCacheKey(CacheKeyBuilder keyBuilder, JoinableFactoryWrapper joinableFactoryWrapper);
}
