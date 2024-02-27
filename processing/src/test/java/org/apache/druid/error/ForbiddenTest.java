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

import org.apache.druid.matchers.DruidMatchers;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;

public class ForbiddenTest
{

  @Test
  public void testAsErrorResponse()
  {
    ErrorResponse errorResponse = new ErrorResponse(Forbidden.exception());
    final Map<String, Object> asMap = errorResponse.getAsMap();

    assertThat(
        asMap,
        DruidMatchers.mapMatcher(
            "error", "druidException",
            "errorCode", "forbidden",
            "persona", "USER",
            "category", "FORBIDDEN",
            "errorMessage", "Unauthorized"
        )
    );

    ErrorResponse recomposed = ErrorResponse.fromMap(asMap);

    assertThat(
        recomposed.getUnderlyingException(),
        new DruidExceptionMatcher(
            DruidException.Persona.USER,
            DruidException.Category.FORBIDDEN,
            "forbidden"
        ).expectMessageContains("Unauthorized")
    );
  }
}
