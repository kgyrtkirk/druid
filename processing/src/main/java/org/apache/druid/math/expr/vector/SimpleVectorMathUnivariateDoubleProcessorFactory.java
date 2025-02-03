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

package org.apache.druid.math.expr.vector;

import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.vector.functional.DoubleUnivariateDoubleFunction;
import org.apache.druid.math.expr.vector.functional.DoubleUnivariateLongFunction;

/**
 * Make a 1 argument math processor with the following type rules
 *    long    -> double
 *    double  -> double
 * using simple scalar functions {@link DoubleUnivariateLongFunction} and {@link DoubleUnivariateDoubleFunction}
 */
public class SimpleVectorMathUnivariateDoubleProcessorFactory extends VectorMathUnivariateDoubleProcessorFactory
{
  private final DoubleUnivariateLongFunction longFunction;
  private final DoubleUnivariateDoubleFunction doubleFunction;

  public SimpleVectorMathUnivariateDoubleProcessorFactory(
      DoubleUnivariateLongFunction longFunction,
      DoubleUnivariateDoubleFunction doubleFunction
  )
  {
    this.longFunction = longFunction;
    this.doubleFunction = doubleFunction;
  }

  @Override
  public final ExprVectorProcessor<double[]> longProcessor(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return new DoubleUnivariateLongFunctionVectorProcessor(
        arg.asVectorProcessor(inspector),
        longFunction
    );
  }

  @Override
  public final ExprVectorProcessor<double[]> doubleProcessor(Expr.VectorInputBindingInspector inspector, Expr arg)
  {
    return new DoubleUnivariateDoubleFunctionVectorProcessor(
        arg.asVectorProcessor(inspector),
        doubleFunction
    );
  }
}
