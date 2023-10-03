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

package org.apache.druid.segment.loading;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.StringUtils;

/**
 */
@PublicApi
public class SegmentLoadingException extends Exception
{
  @FormatMethod
  public SegmentLoadingException(
      @FormatString final String formatString,
      Object... objs
  )
  {
    super(StringUtils.nonStrictFormat(formatString, objs));
  }

  @FormatMethod
  public SegmentLoadingException(
      Throwable cause,
      @FormatString final String formatString,
      Object... objs
  )
  {
    super(StringUtils.nonStrictFormat(formatString, objs), cause);
  }
}
