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

import org.apache.druid.query.Query;

/**
 * Identifies and answers questions regarding the native engine's vertex
 * boundary.
 *
 * I believe due to evolutional purposes there are some concepts which went
 * beyond their inital design:
 *
 * Multiple queries might be executed in one stage: <br/>
 * there is one query type which is collapsed at exection time (GroupBy).
 *
 * Dag of datasources: <br/>
 * an execution may process an entire dag of datasource in some cases
 * (joindatasource) ; or collapse some into the execution (filter)
 */
public class VertexBoundary
{
  protected final Query<?> topQuery;

  private VertexBoundary(Query<?> topQuery)
  {
    this.topQuery = topQuery;
  }

}
