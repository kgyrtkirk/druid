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

package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeFamily;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypePrecedenceList;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.catalog.model.facade.DatasourceFacade;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class MetaType implements RelDataType
{

  DatasourceFacade tableMetadata;

  public MetaType(DatasourceFacade tableMetadata)
  {
    this.tableMetadata = tableMetadata;

  }

  @Override
  public boolean isStruct()
  {
    return true;
  }

  @Override
  public List<RelDataTypeField> getFieldList()
  {
    List<RelDataTypeField> ret = new ArrayList<>();
    for (int i = 0; i < getFieldCount(); i++) {
      RelDataTypeField f=new RelDataTypeFieldImpl("__f"+i, i, this);
      ret.add(f);
    }
    return ret;

  }

  @Override
  public List<String> getFieldNames()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public int getFieldCount()
  {
    return 99;

  }

  @Override
  public StructKind getStructKind()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public @Nullable RelDataTypeField getField(String fieldName, boolean caseSensitive, boolean elideRecord)
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public boolean isNullable()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

  @Override
  public @Nullable RelDataType getComponentType()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public @Nullable RelDataType getKeyType()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public @Nullable RelDataType getValueType()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public @Nullable Charset getCharset()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public @Nullable SqlCollation getCollation()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public @Nullable SqlIntervalQualifier getIntervalQualifier()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public int getPrecision()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public int getScale()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return 0;

  }

  @Override
  public SqlTypeName getSqlTypeName()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public @Nullable SqlIdentifier getSqlIdentifier()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public String getFullTypeString()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public RelDataTypeFamily getFamily()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public RelDataTypePrecedenceList getPrecedenceList()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public RelDataTypeComparability getComparability()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return null;

  }

  @Override
  public boolean isDynamicStruct()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }
    return false;

  }

}
