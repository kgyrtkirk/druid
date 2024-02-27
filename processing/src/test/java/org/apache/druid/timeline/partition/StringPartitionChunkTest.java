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

package org.apache.druid.timeline.partition;

import org.apache.druid.data.input.StringTuple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class StringPartitionChunkTest
{
  @Test
  public void testAbuts()
  {
    // Test with multiple dimensions
    StringPartitionChunk<Integer> lhs = StringPartitionChunk.make(null, StringTuple.create("10", "abc"), 0, 1);

    Assertions.assertTrue(lhs.abuts(StringPartitionChunk.make(StringTuple.create("10", "abc"), null, 1, 2)));
    Assertions.assertFalse(lhs.abuts(StringPartitionChunk.make(StringTuple.create("10", "xyz"), null, 2, 3)));
    Assertions.assertFalse(lhs.abuts(StringPartitionChunk.make(StringTuple.create("11", "abc"), null, 2, 3)));
    Assertions.assertFalse(lhs.abuts(StringPartitionChunk.make(null, null, 3, 4)));

    // Test with single dimension
    lhs = StringPartitionChunk.makeForSingleDimension(null, "10", 0, 1);

    Assertions.assertTrue(lhs.abuts(StringPartitionChunk.makeForSingleDimension("10", null, 1, 2)));
    Assertions.assertFalse(lhs.abuts(StringPartitionChunk.makeForSingleDimension("11", null, 2, 3)));
    Assertions.assertFalse(lhs.abuts(StringPartitionChunk.makeForSingleDimension(null, null, 3, 4)));

    Assertions.assertFalse(StringPartitionChunk.make(null, null, 0, 1).abuts(StringPartitionChunk.make(null, null, 1, 2)));
  }

  @Test
  public void testIsStart()
  {
    // Test with multiple dimensions
    Assertions.assertTrue(StringPartitionChunk.make(null, StringTuple.create("10", "abc"), 0, 1).isStart());
    Assertions.assertFalse(StringPartitionChunk.make(StringTuple.create("10", "abc"), null, 0, 1).isStart());
    Assertions.assertFalse(
        StringPartitionChunk.make(
            StringTuple.create("10", "abc"),
            StringTuple.create("11", "def"),
            0,
            1
        ).isStart()
    );

    // Test with a single dimension
    Assertions.assertTrue(StringPartitionChunk.makeForSingleDimension(null, "10", 0, 1).isStart());
    Assertions.assertFalse(StringPartitionChunk.makeForSingleDimension("10", null, 0, 1).isStart());
    Assertions.assertFalse(StringPartitionChunk.makeForSingleDimension("10", "11", 0, 1).isStart());
    Assertions.assertTrue(StringPartitionChunk.makeForSingleDimension(null, null, 0, 1).isStart());
  }

  @Test
  public void testIsEnd()
  {
    // Test with multiple dimensions
    Assertions.assertFalse(StringPartitionChunk.make(null, StringTuple.create("10", "abc"), 0, 1).isEnd());
    Assertions.assertTrue(StringPartitionChunk.make(StringTuple.create("10", "abc"), null, 0, 1).isEnd());
    Assertions.assertFalse(
        StringPartitionChunk.make(
            StringTuple.create("10", "abc"),
            StringTuple.create("11", "def"),
            0,
            1
        ).isEnd()
    );

    // Test with a single dimension
    Assertions.assertFalse(StringPartitionChunk.makeForSingleDimension(null, "10", 0, 1).isEnd());
    Assertions.assertTrue(StringPartitionChunk.makeForSingleDimension("10", null, 0, 1).isEnd());
    Assertions.assertFalse(StringPartitionChunk.makeForSingleDimension("10", "11", 0, 1).isEnd());
    Assertions.assertTrue(StringPartitionChunk.makeForSingleDimension(null, null, 0, 1).isEnd());
  }

  @Test
  public void testCompareTo()
  {
    // Test with multiple dimensions
    Assertions.assertEquals(
        0,
        StringPartitionChunk.make(null, null, 0, 1).compareTo(
            StringPartitionChunk.make(null, null, 0, 2)
        )
    );
    Assertions.assertEquals(
        0,
        StringPartitionChunk.make(StringTuple.create("10", "abc"), null, 0, 1).compareTo(
            StringPartitionChunk.make(StringTuple.create("10", "abc"), null, 0, 2)
        )
    );
    Assertions.assertEquals(
        0,
        StringPartitionChunk.make(null, StringTuple.create("10", "abc"), 1, 1).compareTo(
            StringPartitionChunk.make(null, StringTuple.create("10", "abc"), 1, 2)
        )
    );
    Assertions.assertEquals(
        0,
        StringPartitionChunk.make(StringTuple.create("10", "abc"), StringTuple.create("11", "aa"), 1, 1).compareTo(
            StringPartitionChunk.make(StringTuple.create("10", "abc"), StringTuple.create("11", "aa"), 1, 2)
        )
    );
    Assertions.assertEquals(
        -1,
        StringPartitionChunk.make(null, StringTuple.create("10", "abc"), 0, 1).compareTo(
            StringPartitionChunk.make(StringTuple.create("10", "abc"), null, 1, 2)
        )
    );
    Assertions.assertEquals(
        -1,
        StringPartitionChunk.make(StringTuple.create("11", "b"), StringTuple.create("20", "a"), 0, 1).compareTo(
            StringPartitionChunk.make(StringTuple.create("20", "a"), StringTuple.create("33", "z"), 1, 1)
        )
    );
    Assertions.assertEquals(
        1,
        StringPartitionChunk.make(StringTuple.create("20", "a"), StringTuple.create("33", "z"), 1, 1).compareTo(
            StringPartitionChunk.make(StringTuple.create("11", "b"), StringTuple.create("20", "a"), 0, 1)
        )
    );

    // Test with a single dimension
    Assertions.assertEquals(
        0,
        StringPartitionChunk.makeForSingleDimension(null, null, 0, 1)
                            .compareTo(StringPartitionChunk.makeForSingleDimension(null, null, 0, 2))
    );
    Assertions.assertEquals(
        0,
        StringPartitionChunk.makeForSingleDimension("10", null, 0, 1)
                            .compareTo(StringPartitionChunk.makeForSingleDimension("10", null, 0, 2))
    );
    Assertions.assertEquals(
        0,
        StringPartitionChunk.makeForSingleDimension(null, "10", 1, 1)
                            .compareTo(StringPartitionChunk.makeForSingleDimension(null, "10", 1, 2))
    );
    Assertions.assertEquals(
        0,
        StringPartitionChunk.makeForSingleDimension("10", "11", 1, 1)
                            .compareTo(StringPartitionChunk.makeForSingleDimension("10", "11", 1, 2))
    );
    Assertions.assertEquals(
        -1,
        StringPartitionChunk.makeForSingleDimension(null, "10", 0, 1)
                            .compareTo(StringPartitionChunk.makeForSingleDimension("10", null, 1, 2))
    );
    Assertions.assertEquals(
        -1,
        StringPartitionChunk.makeForSingleDimension("11", "20", 0, 1)
                            .compareTo(StringPartitionChunk.makeForSingleDimension("20", "33", 1, 1))
    );
    Assertions.assertEquals(
        1,
        StringPartitionChunk.makeForSingleDimension("20", "33", 1, 1)
                            .compareTo(StringPartitionChunk.makeForSingleDimension("11", "20", 0, 1))
    );
    Assertions.assertEquals(
        1,
        StringPartitionChunk.makeForSingleDimension("10", null, 1, 1)
                            .compareTo(StringPartitionChunk.makeForSingleDimension(null, "10", 0, 1))
    );
  }

  @Test
  public void testEquals()
  {
    Assertions.assertEquals(
        StringPartitionChunk.makeForSingleDimension(null, null, 0, 1),
        StringPartitionChunk.makeForSingleDimension(null, null, 0, 1)
    );
    Assertions.assertEquals(
        StringPartitionChunk.makeForSingleDimension(null, "10", 0, 1),
        StringPartitionChunk.makeForSingleDimension(null, "10", 0, 1)
    );
    Assertions.assertEquals(
        StringPartitionChunk.makeForSingleDimension("10", null, 0, 1),
        StringPartitionChunk.makeForSingleDimension("10", null, 0, 1)
    );
    Assertions.assertEquals(
        StringPartitionChunk.makeForSingleDimension("10", "11", 0, 1),
        StringPartitionChunk.makeForSingleDimension("10", "11", 0, 1)
    );
  }

  @Test
  public void testMakeForSingleDimension()
  {
    StringPartitionChunk<Integer> chunk = StringPartitionChunk
        .makeForSingleDimension("a", null, 0, 1);
    Assertions.assertEquals(0, chunk.getChunkNumber());
    Assertions.assertTrue(chunk.isEnd());
    Assertions.assertFalse(chunk.isStart());
  }
}
