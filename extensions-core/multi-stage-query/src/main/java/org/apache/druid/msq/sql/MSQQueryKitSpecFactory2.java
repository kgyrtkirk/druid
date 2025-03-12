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

package org.apache.druid.msq.sql;

import com.google.inject.Inject;
import org.apache.druid.msq.exec.QueryKitSpecFactory;
import org.apache.druid.msq.indexing.MSQSpec0;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernelConfig;
import org.apache.druid.msq.querykit.QueryKit;
import org.apache.druid.msq.querykit.QueryKitSpec;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Query;

public class MSQQueryKitSpecFactory2 implements QueryKitSpecFactory
{
  public QueryKitSpec makeQueryKitSpec(
      final QueryKit<Query<?>> queryKit,
      final String queryId,
      final MSQSpec0 querySpec,
      final ControllerQueryKernelConfig queryKernelConfig
  )
  {
    final QueryContext queryContext = querySpec.getContext();
    return new QueryKitSpec(
        queryKit,
        queryId,
        queryKernelConfig.getWorkerIds().size(),
        queryContext.getInt(
            CTX_MAX_NON_LEAF_WORKER_COUNT,
            DEFAULT_MAX_NON_LEAF_WORKER_COUNT
        ),
        MultiStageQueryContext.getTargetPartitionsPerWorkerWithDefault(
            queryContext,
            DEFAULT_TARGET_PARTITIONS_PER_WORKER
        )
    );
  }


  private DruidProcessingConfig processingConfig;

  @Inject
  public MSQQueryKitSpecFactory2(DruidProcessingConfig processingConfig)
  {
    this.processingConfig = processingConfig;
  }

}
