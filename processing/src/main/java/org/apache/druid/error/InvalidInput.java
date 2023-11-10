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

package org.apache.druid.error;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

public class InvalidInput extends DruidException.Failure
{
  @FormatMethod
  public static DruidException exception(@FormatString final String msg, Object... args)
  {
    return exception(null, msg, args);
  }

  @FormatMethod
  public static DruidException exception(Throwable t, @FormatString final String msg, Object... args)
  {
    return DruidException.fromFailure(new InvalidInput(t, msg, args));
  }

  private final Throwable t;
  private final String msg;
  private final Object[] args;

  @FormatMethod
  public InvalidInput(
      Throwable t,
      @FormatString final String msg,
      Object... args
  )
  {
    super("invalidInput");
    this.t = t;
    this.msg = msg;
    this.args = args;
  }


  @Override
  public DruidException makeException(DruidException.DruidExceptionBuilder bob)
  {
    bob = bob.forPersona(DruidException.Persona.USER)
             .ofCategory(DruidException.Category.INVALID_INPUT);

    if (t == null) {
// FIXME::[64,22] error: [FormatStringAnnotation] Variables used as format strings that are not local variables must be compile time constants.
      return bob.build(msg, args);
    } else {
// FIXME::[66,22] error: [FormatStringAnnotation] Variables used as format strings that are not local variables must be compile time constants.
      return bob.build(t, msg, args);
    }
  }
}
