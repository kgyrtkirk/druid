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

package org.apache.druid.sql.binga;

import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;

public class MyResultset implements ResultSet1
{
  private Signature signature;
  private Frame frame;
  private Iterator<Object> rowIterator = null;
  private Object[] currentRow;

  public MyResultset(Signature signature, Frame frame)
  {
    this.signature = signature;
    this.frame = frame;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public boolean next() throws SQLException
  {
    if (rowIterator == null) {
      rowIterator = frame.rows.iterator();
    }
    if(!rowIterator.hasNext()) {
      return false;
    }
    currentRow = (Object[]) rowIterator.next();

    return true;
  }

  @Override
  public void close() throws SQLException
  {
  }

  @Override
  public boolean wasNull() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public String getString(int columnIndex) throws SQLException
  {
    Object val = currentRow[columnIndex - 1];
    if (val == null) {
      return "∅";
    }
    return val.toString();

  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public short getShort(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public int getInt(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public long getLong(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public float getFloat(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public double getDouble(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Date getDate(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Time getTime(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public String getString(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public byte getByte(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public short getShort(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public int getInt(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public long getLong(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public float getFloat(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public double getDouble(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Date getDate(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Time getTime(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public SQLWarning getWarnings() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public void clearWarnings() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public String getCursorName() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException
  {
    return new MyResultSetMetaData(signature);
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Object getObject(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public int findColumn(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public boolean isBeforeFirst() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public boolean isAfterLast() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public boolean isFirst() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public boolean isLast() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public void beforeFirst() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void afterLast() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public boolean first() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public boolean last() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public int getRow() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public boolean absolute(int row) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public boolean relative(int rows) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public boolean previous() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public void setFetchDirection(int direction) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public int getFetchDirection() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public void setFetchSize(int rows) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public int getFetchSize() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public int getType() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public int getConcurrency() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public boolean rowUpdated() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public boolean rowInserted() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public boolean rowDeleted() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public void updateNull(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateNull(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void insertRow() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateRow() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void deleteRow() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void refreshRow() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void cancelRowUpdates() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void moveToInsertRow() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void moveToCurrentRow() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public Statement getStatement() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Array getArray(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Array getArray(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public URL getURL(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public URL getURL(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public int getHoldability() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public boolean isClosed() throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public void updateNString(int columnIndex, String nString) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateNString(String columnLabel, String nString) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateNClob(int columnIndex, NClob nClob) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateNClob(String columnLabel, NClob nClob) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public String getNString(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public String getNString(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

}
