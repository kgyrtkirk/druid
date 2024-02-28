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

package org.apache.druid.compressedbigdecimal;

import org.junit.jupiter.api.Test;

public class CompressedBigDecimalSumSqlAggregatorTest extends CompressedBigDecimalSqlAggregatorTestBase
{
  private static final String FUNCTION_NAME = CompressedBigDecimalSumSqlAggregator.NAME;

  @Override
  @Test
  public void testCompressedBigDecimalAggWithNumberParse()
  {
    testCompressedBigDecimalAggWithNumberParseHelper(
        FUNCTION_NAME,
        new Object[]{"21.000000000", "21.000000000", "13.100000000"},
        CompressedBigDecimalSumAggregatorFactory::new
    );
  }

  @Override
  @Test
  public void testCompressedBigDecimalAggWithStrictNumberParse()
  {
    testCompressedBigDecimalAggWithStrictNumberParseHelper(
        FUNCTION_NAME,
        CompressedBigDecimalSumAggregatorFactory::new
    );
  }

  @Override
  @Test
  public void testCompressedBigDecimalAggDefaultNumberParseAndCustomSizeAndScale()
  {
    testCompressedBigDecimalAggDefaultNumberParseAndCustomSizeAndScaleHelper(
        FUNCTION_NAME,
        new Object[]{"21.000", "21.000", "13.100"},
        CompressedBigDecimalSumAggregatorFactory::new
    );
  }

  @Override
  @Test
  public void testCompressedBigDecimalAggDefaultScale()
  {
    testCompressedBigDecimalAggDefaultScaleHelper(
        FUNCTION_NAME,
        new Object[]{"21.000000000", "21.000000000", "13.100000000"},
        CompressedBigDecimalSumAggregatorFactory::new
    );
  }

  @Override
  @Test
  public void testCompressedBigDecimalAggDefaultSizeAndScale()
  {
    testCompressedBigDecimalAggDefaultSizeAndScaleHelper(
        FUNCTION_NAME,
        new Object[]{"21.000000000", "21.000000000", "13.100000000"},
        CompressedBigDecimalSumAggregatorFactory::new
    );
  }
}
