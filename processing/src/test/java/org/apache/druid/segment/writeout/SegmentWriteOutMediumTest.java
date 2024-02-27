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

package org.apache.druid.segment.writeout;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.io.Closer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;

public class SegmentWriteOutMediumTest
{

  public static Iterable<?> constructorFeeder()
  {
    return ImmutableList.of(
        TmpFileSegmentWriteOutMediumFactory.instance(),
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        OnHeapMemorySegmentWriteOutMediumFactory.instance()
    );
  }

  @TempDir
  public File temporaryFolder;

  private SegmentWriteOutMediumFactory factory;
  private SegmentWriteOutMedium medium;

  public void initSegmentWriteOutMediumTest(SegmentWriteOutMediumFactory factory)
  {
    this.factory = factory;
  }

  @BeforeEach
  public void setup() throws IOException
  {
    this.medium = factory.makeSegmentWriteOutMedium(newFolder(temporaryFolder, "junit"));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "medium = {0}")
  public void testSanity(SegmentWriteOutMediumFactory factory) throws IOException
  {
    initSegmentWriteOutMediumTest(factory);
    WriteOutBytes bytes1 = medium.makeWriteOutBytes();
    WriteOutBytes bytes2 = medium.makeWriteOutBytes();

    Assertions.assertTrue(bytes1.isOpen());
    Assertions.assertTrue(bytes2.isOpen());

    Closer closer = medium.getCloser();
    closer.close();

    Assertions.assertFalse(bytes1.isOpen());
    Assertions.assertFalse(bytes2.isOpen());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "medium = {0}")
  public void testChildCloseFreesResourcesButNotParents(SegmentWriteOutMediumFactory factory) throws IOException
  {
    initSegmentWriteOutMediumTest(factory);
    WriteOutBytes bytes1 = medium.makeWriteOutBytes();
    WriteOutBytes bytes2 = medium.makeWriteOutBytes();

    Assertions.assertTrue(bytes1.isOpen());
    Assertions.assertTrue(bytes2.isOpen());

    SegmentWriteOutMedium childMedium = medium.makeChildWriteOutMedium();
    Assertions.assertTrue(childMedium.getClass().equals(medium.getClass()));

    WriteOutBytes bytes3 = childMedium.makeWriteOutBytes();
    WriteOutBytes bytes4 = childMedium.makeWriteOutBytes();

    Assertions.assertTrue(bytes3.isOpen());
    Assertions.assertTrue(bytes4.isOpen());

    Closer childCloser = childMedium.getCloser();
    childCloser.close();

    Assertions.assertFalse(bytes3.isOpen());
    Assertions.assertFalse(bytes4.isOpen());

    Assertions.assertTrue(bytes1.isOpen());
    Assertions.assertTrue(bytes2.isOpen());

    Closer closer = medium.getCloser();
    closer.close();

    Assertions.assertFalse(bytes1.isOpen());
    Assertions.assertFalse(bytes2.isOpen());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "medium = {0}")
  public void testChildNotClosedExplicitlyIsClosedByParent(SegmentWriteOutMediumFactory factory) throws IOException
  {
    initSegmentWriteOutMediumTest(factory);
    WriteOutBytes bytes1 = medium.makeWriteOutBytes();
    WriteOutBytes bytes2 = medium.makeWriteOutBytes();

    Assertions.assertTrue(bytes1.isOpen());
    Assertions.assertTrue(bytes2.isOpen());

    SegmentWriteOutMedium childMedium = medium.makeChildWriteOutMedium();
    Assertions.assertTrue(childMedium.getClass().equals(medium.getClass()));

    WriteOutBytes bytes3 = childMedium.makeWriteOutBytes();
    WriteOutBytes bytes4 = childMedium.makeWriteOutBytes();

    Assertions.assertTrue(bytes3.isOpen());
    Assertions.assertTrue(bytes4.isOpen());

    Closer closer = medium.getCloser();
    closer.close();

    Assertions.assertFalse(bytes1.isOpen());
    Assertions.assertFalse(bytes2.isOpen());

    Assertions.assertFalse(bytes3.isOpen());
    Assertions.assertFalse(bytes4.isOpen());
  }

  private static File newFolder(File root, String... subDirs) throws IOException {
    String subFolder = String.join("/", subDirs);
    File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }
}
