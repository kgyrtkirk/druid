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

import org.apache.druid.error.DruidException;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;

/**
 * Represents the native engine's execution vertex.
 *
 * I believe due to evolutional purposes there are some concepts which went
 * beyond their inital design:
 *
 * Multiple queries might be executed in one stage: <br/>
 * The GroupBy query could be collapsed at exection time).
 *
 * Dag of datasources: <br/>
 * an execution may process an entire dag of datasource in some cases
 * (joindatasource) ; or collapse some into the execution (filter)
 */
public class ExecutionVertex
{
  protected final Query<?> topQuery;

  private ExecutionVertex(Query<?> topQuery)
  {
    this.topQuery = topQuery;
  }

  public static ExecutionVertex of(Query<?> query)
  {
    ExecutionVertexExplorer executionVertexExplorer = new ExecutionVertexExplorer();
    executionVertexExplorer.visit(query);
    query.getDataSource();
    return new ExecutionVertex(query);
  }

  public DataSource getBaseDataSource()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  // FIXME: correct apidcos?
  public boolean isConcreteBased()
  {
    return getBaseDataSource().isConcrete();
  }

  public boolean isTableBased()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  static class ExecutionVertexExplorer
  {

    public void visit(Query<?> query)
    {
      // if(query instanceof BaseQuery<?>) {
      // BaseQuery<?> baseQuery = (BaseQuery<?>) query;
      // DataSource oldDataSource = baseQuery.getDataSource();
      // DataSource newDataSource = visit(oldDataSource);
      // if(oldDataSource!=newDataSource) {
      // baseQuery = baseQuery.withDataSource(newDataSource);
      // }
      //
      // }
      throw DruidException.defensive("fixme");

    }

    private DataSource visit(DataSource dataSource)
    {
      if (true) {
        throw new RuntimeException("FIXME: Unimplemented!");
      }
      return null;

    }

  }

  public static DataSourceAnalysis of1(BaseQuery<?> baseQuery)
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  public static DruidException ofIllegal(Object dataSource)
  {
    if(true)
    {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }
}
