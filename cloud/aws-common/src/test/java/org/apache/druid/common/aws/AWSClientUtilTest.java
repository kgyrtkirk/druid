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

package org.apache.druid.common.aws;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class AWSClientUtilTest
{
  @Test
  public void testRecoverableException_IOException()
  {
    Assertions.assertTrue(AWSClientUtil.isClientExceptionRecoverable(new AmazonClientException(new IOException())));
  }

  @Test
  public void testRecoverableException_RequestTimeout()
  {
    AmazonServiceException ex = new AmazonServiceException(null);
    ex.setErrorCode("RequestTimeout");
    Assertions.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_500()
  {
    AmazonServiceException ex = new AmazonServiceException(null);
    ex.setStatusCode(500);
    Assertions.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_502()
  {
    AmazonServiceException ex = new AmazonServiceException(null);
    ex.setStatusCode(502);
    Assertions.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_503()
  {
    AmazonServiceException ex = new AmazonServiceException(null);
    ex.setStatusCode(503);
    Assertions.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_ProvisionedThroughputExceededException()
  {
    AmazonServiceException ex = new AmazonServiceException(null);
    ex.setErrorCode("ProvisionedThroughputExceededException");
    Assertions.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_ClockSkewedError()
  {
    AmazonServiceException ex = new AmazonServiceException(null);
    ex.setErrorCode("RequestExpired");
    Assertions.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testRecoverableException_MultiObjectDeleteException()
  {
    MultiObjectDeleteException.DeleteError retryableError = new MultiObjectDeleteException.DeleteError();
    retryableError.setCode("RequestLimitExceeded");
    MultiObjectDeleteException.DeleteError nonRetryableError = new MultiObjectDeleteException.DeleteError();
    nonRetryableError.setCode("nonRetryableError");
    MultiObjectDeleteException ex = new MultiObjectDeleteException(
        ImmutableList.of(retryableError, nonRetryableError),
        ImmutableList.of()
    );
    Assertions.assertTrue(AWSClientUtil.isClientExceptionRecoverable(ex));
  }

  @Test
  public void testNonRecoverableException_RuntimeException()
  {
    AmazonClientException ex = new AmazonClientException(new RuntimeException());
    Assertions.assertFalse(AWSClientUtil.isClientExceptionRecoverable(ex));
  }
}
