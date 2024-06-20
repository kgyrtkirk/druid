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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.ColumnMetaData.AvaticaType;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.util.ArrayFactoryImpl;
import org.junit.jupiter.api.Test;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.TimeZone;

public class Asd
{
  @Test
  public void scalarInArrayWithStringToArray() throws SQLException
  {
    String connectionURL = "druidtest:///";
    Connection connection = DriverManager.getConnection(connectionURL);
    PreparedStatement stmt = connection
        .prepareStatement("explain plan for select l1 from numfoo where SCALAR_IN_ARRAY(l1, STRING_TO_ARRAY(CAST(? as varchar),','))");
    List<Integer> li = ImmutableList.of(0, 7);
    String sqlArg = Joiner.on(",").join(li);
    stmt.setString(1, sqlArg);
    stmt.executeQuery();
    ResultSet rs = stmt.getResultSet();
    while (rs.next()) {
      System.out.println(rs.getObject(1));
    }
  }

  @Test
  public void scalarInArrayNativeArray() throws SQLException
  {
    String connectionURL = "druidtest:///";
    Connection connection = DriverManager.getConnection(connectionURL);
    PreparedStatement stmt = connection.prepareStatement("select l1 from numfoo where SCALAR_IN_ARRAY(l1, ?)");
    Iterable<Object> li = ImmutableList.of(0, 7);
    ArrayFactoryImpl a = new ArrayFactoryImpl(TimeZone.getDefault());
    AvaticaType type = ColumnMetaData.scalar(Types.INTEGER, SqlType.INTEGER.name(), Rep.INTEGER);
    Array arr = a.createArray(type, li);
    stmt.setArray(1, arr);
    stmt.executeQuery();
    ResultSet rs = stmt.getResultSet();
    while (rs.next()) {
      System.out.println(rs.getObject(1));
    }
  }
}
