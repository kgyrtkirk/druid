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

package org.apache.druid.segment.filter;

import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.segment.filter.BaseFilterTest2.SetBase;

import java.util.List;

public class FilterCheckBase implements SetBase,IBaseFilterTest2
{

  private IBaseFilterTest2 baseFilterTest;

  @Override
  public void setBase(BaseFilterTest2 base)
  {
    baseFilterTest=base;
  }

  @Override
  public void assertFilterMatches(
      final DimFilter filter,
      final List<String> expectedRows
  )
  {
    baseFilterTest.assertFilterMatches(filter, expectedRows);
  }

}
