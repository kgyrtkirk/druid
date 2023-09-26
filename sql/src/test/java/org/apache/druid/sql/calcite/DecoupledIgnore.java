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

package org.apache.druid.sql.calcite;

import com.google.common.base.Throwables;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.UOE;
import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertThrows;

/**
 * Can be used to mark tests which are not-yet supported in decoupled mode.
 *
 * In case a testcase marked with this annotation fails - it may mean that the
 * testcase no longer needs that annotation.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface DecoupledIgnore
{
  Modes value() default Modes.NOT_ENOUGH_RULES;

  enum Modes
  {
    PLAN_MISMATCH(AssertionError.class, "AssertionError: query #"),
    NOT_ENOUGH_RULES(DruidException.class, "not enough rules"),
    CANNOT_CONVERT(DruidException.class, "Cannot convert query parts"),
    ERROR_HANDLING(AssertionError.class, "(is <ADMIN> was <OPERATOR>|is <INVALID_INPUT> was <UNCATEGORIZED>|with message a string containing)"),


    COLUMN_NOT_FOUND(DruidException.class, "CalciteContextException.*Column.*not found in any table"),
    NULLS_FIRST_LAST(DruidException.class, "NULLS (FIRST|LAST)"),
    BIGINT_TO_DATE(DruidException.class, "BIGINT to type (DATE|TIME)"),
    NPE(DruidException.class, "java.lang.NullPointerException"),
    RESULT_PARSE_EXCEPTION(Exception.class, "parseResults"),
    AGGREGATION_NOT_SUPPORT_TYPE(DruidException.class, "Aggregation \\[(MIN|MAX)\\] does not support type"),
    CANNOT_APPLY_VIRTUAL_COL(UOE.class, "apply virtual columns"),
    MISSING_DESC(DruidException.class, "function signature DESC");


    public Class<? extends Throwable> throwableClass;
    public String regex;

    Modes(Class<? extends Throwable> cl, String regex)
    {
      this.throwableClass = cl;
      this.regex = regex;
    }

    Pattern getPattern()
    {
      return Pattern.compile(regex);
    }
  };

  /**
   * Processes {@link DecoupledIgnore} annotations.
   *
   * Ensures that test cases disabled with that annotation can still not pass.
   * If the error is as expected; the testcase is marked as "ignored".
   */
  class DecoupledIgnoreProcessor implements TestRule
  {
    @Override
    public Statement apply(Statement base, Description description)
    {
      DecoupledIgnore annotation = description.getAnnotation(DecoupledIgnore.class);

      if (annotation == null) {
        return base;
      }
      return new Statement()
      {
        @Override
        public void evaluate()
        {
          Modes ignoreMode = annotation.value();
          Throwable e = assertThrows(
              "Expected that this testcase will fail - it might got fixed?",
              ignoreMode.throwableClass,
              base::evaluate
              );

          String trace = Throwables.getStackTraceAsString(e);
          Matcher m = annotation.value().getPattern().matcher(trace);

          if (!m.find()) {
            throw new AssertionError("Exception stactrace doesn't match regex: " + annotation.value().regex, e);
          }
          throw new AssumptionViolatedException("Test is not-yet supported in Decoupled mode");
        }
      };
    }
  }
}
