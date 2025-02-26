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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.planning.ExecutionVertex.ExecutionVertexExplorer;

import java.util.List;
import java.util.Set;

public abstract class ChainedDataSource<TDataSource extends DataSource> implements DataSource
{
  protected final TDataSource base;

  public ChainedDataSource(TDataSource base)
  {
    this.base = base;
  }

  @JsonProperty("base")
  public final TDataSource getBase()
  {
    return base;
  }


  @Override
  public final Set<String> getTableNames()
  {
    return base.getTableNames();
  }


  @Override
  public final List<DataSource> getChildren()
  {
    return ImmutableList.of(base);
  }

  @Override
  public final DataSource withChildren(List<DataSource> children)
  {
    if (children.size() != 1) {
      throw new IAE("Expected [1] child, got [%d]", children.size());
    }
    return withUpdatedDataSource(children.get(0));
  }

  @Override
  public final DataSource accept(ExecutionVertexExplorer executionVertexExplorer)
  {
    DataSource newBase = base.accept(executionVertexExplorer);
    return executionVertexExplorer.visit(newBase);
  }
}
