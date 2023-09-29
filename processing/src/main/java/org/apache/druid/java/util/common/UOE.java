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

package org.apache.druid.java.util.common;

import com.google.common.base.Strings;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import org.apache.druid.common.exception.SanitizableException;

import java.util.function.Function;

/**
 */
public class UOE extends UnsupportedOperationException implements SanitizableException
{
  @FormatMethod
  public UOE(@FormatString final String formatText, Object... arguments)
  {
    super(StringUtils.nonStrictFormat(formatText, arguments));
  }

  @Override
  public Exception sanitize(Function<String, String> errorMessageTransformFunction)
  {
    String transformedErrorMessage = errorMessageTransformFunction.apply(getMessage());
    if (Strings.isNullOrEmpty(transformedErrorMessage)) {
      return new UOE("");
    } else {
      return new UOE("%s", transformedErrorMessage);
    }
  }
}
