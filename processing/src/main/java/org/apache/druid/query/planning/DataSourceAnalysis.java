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
import org.apache.druid.query.DataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import java.util.Optional;

/**
 * Analysis of a datasource for purposes of deciding how to execute a particular
 * query.
 *
 * The analysis breaks a datasource down in the following way:
 *
 * <pre>
 *
 *                             Q  <-- Possible query datasource(s) [may be none, or multiple stacked]
 *                             |
 *                             Q  <-- Base query datasource, returned by {@link #getBaseQuery()} if it exists
 *                             |
 *                             J  <-- Possible join tree, expected to be left-leaning
 *                            / \
 *                           J  Dj <--  Other leaf datasources
 *   Base datasource        / \         which will be joined
 *  (bottom-leftmost) -->  Db Dj  <---- into the base datasource
 *
 * </pre>
 *
 * The base datasource (Db) is returned by {@link #getBaseDataSource()}. The
 * other leaf datasources are returned by {@link #getPreJoinableClauses()}.
 *
 * The base datasource (Db) will never be a join, but it can be any other type
 * of datasource (table, query, etc). Note that join trees are only flattened if
 * they occur at the top of the overall tree (or underneath an outer query), and
 * that join trees are only flattened to the degree that they are left-leaning.
 * Due to these facts, it is possible for the base or leaf datasources to
 * include additional joins.
 *
 * The base datasource is the one that will be considered by the core Druid
 * query stack for scanning via {@link org.apache.druid.segment.Segment} and
 * {@link org.apache.druid.segment.CursorFactory}. The other leaf datasources
 * must be joinable onto the base data.
 *
 * The idea here is to keep things simple and dumb. So we focus only on
 * identifying left-leaning join trees, which map neatly onto a series of hash
 * table lookups at query time. The user/system generating the queries, e.g. the
 * druid-sql layer (or the end user in the case of native queries), is
 * responsible for containing the smarts to structure the tree in a way that
 * will lead to optimal execution.
 */
public interface DataSourceAnalysis
{
  /**
   * Returns the base (bottom-leftmost) datasource.
   */
  public DataSource getBaseDataSource();

  /**
   * Unwraps the {@link #getBaseDataSource()} if its a {@link TableDataSource}.
   *
   * @throws An
   *           error of type {@link DruidException.Category#DEFENSIVE} if the
   *           {@link BaseDataSource} is not a table.
   *
   *           note that this may not be true even
   *           base datasource is a {@link UnionDataSource} of
   *           {@link TableDataSource}.
   */
  public TableDataSource getBaseTableDataSource();

  /**
   * If the original data source is a join data source and there is a DimFilter
   * on the base table data source, that DimFilter is returned here
   */
  public Optional<DimFilter> getJoinBaseTableFilter();

  /**
   * The applicable {@link QuerySegmentSpec} for this vertex.
   *
   * There might be more queries inside a single vertex; so the outer one is not
   * necessary correct.
   */
  public QuerySegmentSpec getEffectiveQuerySegmentSpec();

  /**
   * Returns true if this datasource can be computed by the core Druid query
   * stack via a scan of a concrete base datasource. All other datasources
   * involved, if any, must be global.
   */
  public boolean isConcreteBased();

  /**
   * Returns whether this datasource is one of:
   *
   * <ul>
   * <li>{@link TableDataSource}</li>
   * <li>{@link UnionDataSource} composed entirely of
   * {@link TableDataSource}</li>
   * <li>{@link UnnestDataSource} composed entirely of
   * {@link TableDataSource}</li>
   * </ul>
   */
  public boolean isTableBased();

  /**
   * Returns true if this datasource is made out of a join operation
   */
  public boolean isJoin();

  /**
   * Returns whether "column" on the analyzed datasource refers to a column from
   * the base datasource.
   */
  public boolean isBaseColumn(final String column);

  /**
   * {@link DataSource#isGlobal()}.
   */
  public boolean isGlobal();

  public DataSourceAnalysis maybeWithQuerySegmentSpec(QuerySegmentSpec newQuerySegmentSpec);
}
