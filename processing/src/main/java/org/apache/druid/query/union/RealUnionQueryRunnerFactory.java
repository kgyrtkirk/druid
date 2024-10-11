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

package org.apache.druid.query.union;

import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.segment.Segment;

public class RealUnionQueryRunnerFactory implements QueryRunnerFactory<RealUnionResult, UnionQuery>
{

  private static UnionQueryQueryToolChest tt;

  public RealUnionQueryRunnerFactory(String string)
  {
    int asd=1;
  }

  @Override
  public QueryRunner<RealUnionResult> createRunner(Segment segment)
  {
    if(true)
    {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public QueryRunner<RealUnionResult> mergeRunners(QueryProcessingPool queryProcessingPool,
      Iterable<QueryRunner<RealUnionResult>> queryRunners)
  {
    if(true)
    {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public QueryToolChest<RealUnionResult, UnionQuery> getToolchest()
  {
    if (tt == null) {
      tt = new UnionQueryQueryToolChest();
    }
    return tt;
  }

}
