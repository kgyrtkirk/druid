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

package org.apache.druid.sql.avatica;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Pair;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DruidAvaticaHandlerTest2
{
  @RegisterExtension
  public static DruidAvaticaHandlerTest3 e = new DruidAvaticaHandlerTest3();

  @Test
  public void testSelectCount() throws SQLException
  {
    try (Connection c = e.getConnection(); Statement stmt = c.createStatement()) {
      final ResultSet resultSet = stmt.executeQuery("SELECT COUNT(*) AS cnt FROM druid.foo");
      final List<Map<String, Object>> rows = getRows(resultSet);
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableMap.of("cnt", 6L)
          ),
          rows
      );
    }
  }

  private static List<Map<String, Object>> getRows(final ResultSet resultSet) throws SQLException
  {
    return getRows(resultSet, null);
  }

  private static List<Map<String, Object>> getRows(final ResultSet resultSet, final Set<String> returnKeys)
      throws SQLException
  {
    try {
      final ResultSetMetaData metaData = resultSet.getMetaData();
      final List<Map<String, Object>> rows = new ArrayList<>();
      while (resultSet.next()) {
        final Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < metaData.getColumnCount(); i++) {
          if (returnKeys == null || returnKeys.contains(metaData.getColumnLabel(i + 1))) {
            Object result = resultSet.getObject(i + 1);
            if (result instanceof Array) {
              row.put(metaData.getColumnLabel(i + 1), ((Array) result).getArray());
            } else {
              row.put(metaData.getColumnLabel(i + 1), result);
            }
          }
        }
        rows.add(row);
      }
      return rows;
    }
    finally {
      resultSet.close();
    }
  }

  @SafeVarargs
  private static Map<String, Object> row(final Pair<String, ?>... entries)
  {
    final Map<String, Object> m = new HashMap<>();
    for (Pair<String, ?> entry : entries) {
      m.put(entry.lhs, entry.rhs);
    }
    return m;
  }
}
