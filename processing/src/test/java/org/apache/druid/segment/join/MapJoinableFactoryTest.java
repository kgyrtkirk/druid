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

package org.apache.druid.segment.join;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.InlineDataSource;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;

@RunWith(EasyMockRunner.class)
public class MapJoinableFactoryTest
{
  @Mock
  private InlineDataSource inlineDataSource;
  @Mock(MockType.NICE)
  private JoinableFactory noopJoinableFactory;
  private NoopDataSource noopDataSource;
  @Mock
  private JoinConditionAnalysis condition;
  @Mock
  private Joinable mockJoinable;

  private MapJoinableFactory target;

  @BeforeEach
  public void setUp()
  {
    noopDataSource = new NoopDataSource();

    target = new MapJoinableFactory(
        ImmutableSet.of(noopJoinableFactory),
        ImmutableMap.of(noopJoinableFactory.getClass(), NoopDataSource.class)
    );
  }


  @Test
  public void testBuildDataSourceNotRegisteredShouldReturnAbsent()
  {
    Optional<Joinable> joinable = target.build(inlineDataSource, condition);
    Assertions.assertFalse(joinable.isPresent());
  }

  @Test
  public void testBuildDataSourceIsRegisteredAndFactoryDoesNotBuildJoinableShouldReturnAbsent()
  {
    EasyMock.expect(noopJoinableFactory.build(noopDataSource, condition)).andReturn(Optional.empty());
    EasyMock.replay(noopJoinableFactory);
    Optional<Joinable> joinable = target.build(noopDataSource, condition);
    Assertions.assertFalse(joinable.isPresent());
  }

  @Test
  public void testBuildDataSourceIsRegisteredShouldReturnJoinableFromFactory()
  {
    EasyMock.expect(noopJoinableFactory.build(noopDataSource, condition)).andReturn(Optional.of(mockJoinable));
    EasyMock.replay(noopJoinableFactory);
    Optional<Joinable> joinable = target.build(noopDataSource, condition);
    Assertions.assertEquals(mockJoinable, joinable.get());

  }

  @Test
  public void testComputeJoinCacheKey()
  {
    Optional<byte[]> expected = Optional.of(new byte[]{1, 2, 3});
    EasyMock.expect(noopJoinableFactory.computeJoinCacheKey(noopDataSource, condition)).andReturn(expected);
    EasyMock.replay(noopJoinableFactory);
    Optional<byte[]> actual = target.computeJoinCacheKey(noopDataSource, condition);
    Assertions.assertSame(expected, actual);
  }

  @Test
  public void testBuildExceptionWhenTwoJoinableFactoryForSameDataSource()
  {
    Throwable exception = assertThrows(ISE.class, () -> {
      JoinableFactory anotherNoopJoinableFactory = EasyMock.mock(MapJoinableFactory.class);
      target = new MapJoinableFactory(
          ImmutableSet.of(noopJoinableFactory, anotherNoopJoinableFactory),
          ImmutableMap.of(
              noopJoinableFactory.getClass(),
              NoopDataSource.class,
              anotherNoopJoinableFactory.getClass(),
              NoopDataSource.class
          )
      );
      EasyMock.expect(noopJoinableFactory.build(noopDataSource, condition)).andReturn(Optional.of(mockJoinable));
      EasyMock.expect(anotherNoopJoinableFactory.build(noopDataSource, condition)).andReturn(Optional.of(mockJoinable));
      EasyMock.replay(noopJoinableFactory, anotherNoopJoinableFactory);
      target.build(noopDataSource, condition);
    });
    assertTrue(exception.getMessage().contains(StringUtils.format(
        "Multiple joinable factories are valid for table[%s]",
        noopDataSource
    )));
  }

  @Test
  public void testIsDirectShouldBeFalseForNotRegistered()
  {
    Assertions.assertFalse(target.isDirectlyJoinable(inlineDataSource));
  }

  @Test
  public void testIsDirectlyJoinableShouldBeTrueForRegisteredThatIsJoinable()
  {
    EasyMock.expect(noopJoinableFactory.isDirectlyJoinable(noopDataSource)).andReturn(true).anyTimes();
    EasyMock.replay(noopJoinableFactory);
    Assertions.assertTrue(target.isDirectlyJoinable(noopDataSource));
  }
}
