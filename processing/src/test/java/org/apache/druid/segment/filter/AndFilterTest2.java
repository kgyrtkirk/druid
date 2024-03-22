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

package org.apache.druid.segment.filter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.filter.BaseFilterTest2.J5ContextProvider;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import java.util.Collection;
import java.util.List;
import java.util.Map;


public class AndFilterTest2
{
  public static Collection<Object[]> constructorFeeder()
  {
    return BaseFilterTest2.makeConstructors();
  }

  private static final String TIMESTAMP_COLUMN = "timestamp";

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec(TIMESTAMP_COLUMN, "iso", DateTimes.of("2000")),
          DimensionsSpec.EMPTY
      )
  );

  private static final List<InputRow> ROWS = ImmutableList.of(
      PARSER.parseBatch(ImmutableMap.of("dim0", "0", "dim1", "0")).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "1", "dim1", "0")).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "2", "dim1", "0")).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "3", "dim1", "0")).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "4", "dim1", "0")).get(0),
      PARSER.parseBatch(ImmutableMap.of("dim0", "5", "dim1", "0")).get(0)
  );

  private BaseFilterTest2 baseFilterTest;

//  @RegisterExtension
//  public static BaseFilterTest2.J5ContextProvider base = new BaseFilterTest2.J5ContextProvider(ROWS);


  public void initAndFilterTest(
      BaseFilterTest2 config
  )
  {

    baseFilterTest = config;
    }

//  @BeforeEach
//  public void beforeEach(@TempDir File tempDir) throws Exception {
//    baseFilterTest.setUp(tempDir);
//  }
//
//  // FIXME
//  @AfterAll
//  public static void tearDown() throws Exception
//  {
//    BaseFilterTest2.tearDown(AndFilterTest2.class.getName());
//  }
//

  @TestTemplate
  private void sda()
  {
    if (true) {
      throw new RuntimeException("FIXME: Unimplemented!");
    }

  }
static class MyJ5ContextProvider extends J5ContextProvider {

  public MyJ5ContextProvider()
  {
    super(ROWS);
  }

}

  @TestTemplate
  @ExtendWith(MyJ5ContextProvider.class)
//  @MethodSource("constructorFeeder")
//  @ParameterizedTest(name = "{0}")
//  @EnumSource
  public void testAnd(BaseFilterTest2 config)
  {
    initAndFilterTest(config);
    assertFilterMatches(
        new AndDimFilter(ImmutableList.of(
            new SelectorDimFilter("dim0", "0", null),
            new SelectorDimFilter("dim1", "0", null)
        )),
        ImmutableList.of("0")
    );
    assertFilterMatches(
        new AndDimFilter(ImmutableList.of(
            new SelectorDimFilter("dim0", "0", null),
            new SelectorDimFilter("dim1", "1", null)
        )),
        ImmutableList.of()
    );
    assertFilterMatches(
        new AndDimFilter(ImmutableList.of(
            new SelectorDimFilter("dim0", "1", null),
            new SelectorDimFilter("dim1", "0", null)
        )),
        ImmutableList.of("1")
    );
    assertFilterMatches(
        new AndDimFilter(ImmutableList.of(
            new SelectorDimFilter("dim0", "1", null),
            new SelectorDimFilter("dim1", "1", null)
        )),
        ImmutableList.of()
    );
    assertFilterMatches(
        new AndDimFilter(ImmutableList.of(
            new NotDimFilter(new SelectorDimFilter("dim0", "1", null)),
            new NotDimFilter(new SelectorDimFilter("dim1", "1", null))
        )),
        ImmutableList.of("0", "2", "3", "4", "5")
    );
    assertFilterMatches(
        new AndDimFilter(ImmutableList.of(
            new NotDimFilter(new SelectorDimFilter("dim0", "0", null)),
            new NotDimFilter(new SelectorDimFilter("dim1", "0", null))
        )),
        ImmutableList.of()
    );
  }

  protected void assertFilterMatches(
      final DimFilter filter,
      final List<String> expectedRows
  )
  {
    baseFilterTest.assertFilterMatches(filter, expectedRows);
  }

  @TestTemplate
  @ExtendWith(MyJ5ContextProvider.class)
  public void testNotAnd(BaseFilterTest2 config)
  {
    initAndFilterTest(config);
    assertFilterMatches(
        new NotDimFilter(new AndDimFilter(ImmutableList.of(
            new SelectorDimFilter("dim0", "0", null),
            new SelectorDimFilter("dim1", "0", null)
        ))),
        ImmutableList.of("1", "2", "3", "4", "5")
    );
    assertFilterMatches(
        new NotDimFilter(new AndDimFilter(ImmutableList.of(
            new SelectorDimFilter("dim0", "0", null),
            new SelectorDimFilter("dim1", "1", null)
        ))),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
    assertFilterMatches(
        new NotDimFilter(new AndDimFilter(ImmutableList.of(
            new SelectorDimFilter("dim0", "1", null),
            new SelectorDimFilter("dim1", "0", null)
        ))),
        ImmutableList.of("0", "2", "3", "4", "5")
    );
    assertFilterMatches(
        new NotDimFilter(new AndDimFilter(ImmutableList.of(
            new SelectorDimFilter("dim0", "1", null),
            new SelectorDimFilter("dim1", "1", null)
        ))),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
    assertFilterMatches(
        new NotDimFilter(new AndDimFilter(ImmutableList.of(
            new NotDimFilter(new SelectorDimFilter("dim0", "1", null)),
            new NotDimFilter(new SelectorDimFilter("dim1", "1", null))
        ))),
        ImmutableList.of("1")
    );
    assertFilterMatches(
        new NotDimFilter(new AndDimFilter(ImmutableList.of(
            new NotDimFilter(new SelectorDimFilter("dim0", "0", null)),
            new NotDimFilter(new SelectorDimFilter("dim1", "0", null))
        ))),
        ImmutableList.of("0", "1", "2", "3", "4", "5")
    );
  }

  //FIXME
  @TestTemplate
  @ExtendWith(MyJ5ContextProvider.class)
  public void test_equals(BaseFilterTest2 config)
  {
    EqualsVerifier.forClass(AndFilter.class).usingGetClass().withNonnullFields("filters").verify();
  }
}
