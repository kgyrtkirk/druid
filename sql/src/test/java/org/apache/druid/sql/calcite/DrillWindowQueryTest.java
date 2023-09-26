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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.io.ByteStreams;
import com.google.inject.Injector;
import org.apache.commons.io.FileUtils;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.TimestampParser;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.sql.calcite.DecoupledIgnore.DecoupledIgnoreProcessor;
import org.apache.druid.sql.calcite.DecoupledIgnore.Modes;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * These test cases are borrowed from the drill-test-framework at
 * https://github.com/apache/drill-test-framework
 * <p>
 * The Drill data sources are just accessing parquet files directly, we ingest
 * and index the data first via JSON (so that we avoid pulling in the parquet
 * dependencies here) and then run the queries over that.
 * <p>
 * The setup of the ingestion is done via code in this class, so any new data
 * source needs to be added through that manner. That said, these tests are
 * primarily being added to bootstrap our own test coverage of window functions,
 * so it is believed that most iteration on tests will happen through the
 * CalciteWindowQueryTest instead of this class.
 */
public class DrillWindowQueryTest extends BaseCalciteQueryTest
{
  private static final Logger log = new Logger(DrillWindowQueryTest.class);

  private static final ObjectMapper MAPPER = new DefaultObjectMapper();
  private DrillTestCase testCase = null;

  static {
    NullHandling.initializeForTests();
  }

  @Test
  public void ensureAllDeclared() throws Exception
  {
    final URL windowQueriesUrl = ClassLoader.getSystemResource("drill/window/queries/");
    Path windowFolder = new File(windowQueriesUrl.toURI()).toPath();

    Set<String> allCases = FileUtils
        .streamFiles(windowFolder.toFile(), true, "q")
        .map(file -> {
          return windowFolder.relativize(file.toPath()).toString();
        })
        .sorted().collect(Collectors.toSet());

    for (Method method : DrillWindowQueryTest.class.getDeclaredMethods()) {
      DrillTest ann = method.getAnnotation(DrillTest.class);
      if (method.getAnnotation(Test.class) == null || ann == null) {
        continue;
      }
      if (allCases.remove(ann.value() + ".q")) {
        continue;
      }
      fail("found testcase referencing invalid file: " + method.getName());
    }

    for (String string : allCases) {
      string = string.substring(0, string.lastIndexOf('.'));
      System.out.printf(Locale.ENGLISH, "@%s( \"%s\" )\n"
          + "@Test\n"
          + "public void test_%s() throws Exception {\n"
          + "    windowQueryTest();\n"
          + "}\n",
          DrillTest.class.getSimpleName(),
          string,
          string.replace('/', '_'));
    }
    assertEquals("found some non-declared tests; please add the above!", 0, allCases.size());
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD})
  public @interface DrillTest
  {
    /**
     * Name of the file this test should execute.
     */
    String value();
  }

  @Rule
  public DecoupledIgnoreProcessor ignoreProcessor = new DecoupledIgnoreProcessor();

  @Rule
  public DrillTestCaseLoaderRule drillTestCaseRule = new DrillTestCaseLoaderRule();

  class DrillTestCaseLoaderRule implements TestRule
  {

    @Override
    public Statement apply(Statement base, Description description)
    {
      DrillTest annotation = description.getAnnotation(DrillTest.class);
      testCase = (annotation == null) ? null : new DrillTestCase(annotation.value());
      return base;
    }
  }

  static class DrillTestCase
  {
    private final String query;
    private final List<String[]> results;
    private String filename;

    public DrillTestCase(String filename)
    {
      try {
        this.filename = filename;
        this.query = readStringFromResource(".q");
        String resultsStr = readStringFromResource(".e");
        String[] lines = resultsStr.split("\n");
        results = new ArrayList<>();
        for (String string : lines) {
          String[] cols = string.split("\t");
          results.add(cols);
        }
      }
      catch (Exception e) {
        throw new RuntimeException("Encountered exception while loading testcase", e);
      }
    }

    @Nonnull
    private String getQueryString()
    {
      return query;
    }

    @Nonnull
    private List<String[]> getExpectedResults()
    {
      return results;
    }

    @Nonnull
    private String readStringFromResource(String s) throws IOException
    {
      final String query;
      try (InputStream queryIn = ClassLoader.getSystemResourceAsStream("drill/window/queries/" + filename + s)) {
        query = new String(ByteStreams.toByteArray(queryIn), StandardCharsets.UTF_8);
      }
      return query;
    }
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker(
      QueryRunnerFactoryConglomerate conglomerate,
      JoinableFactoryWrapper joinableFactory,
      Injector injector) throws IOException
  {
    final SpecificSegmentsQuerySegmentWalker retVal = super.createQuerySegmentWalker(
        conglomerate,
        joinableFactory,
        injector);

    attachIndex(
        retVal,
        "tblWnulls.parquet",
        new LongDimensionSchema("c1"),
        new StringDimensionSchema("c2"));

    // {"col0":1,"col1":65534,"col2":256.0,"col3":1234.9,"col4":73578580,"col5":1393720082338,"col6":421185052800000,"col7":false,"col8":"CA","col9":"AXXXXXXXXXXXXXXXXXXXXXXXXXCXXXXXXXXXXXXXXXXXXXXXXXXZ"}
    attachIndex(
        retVal,
        "allTypsUniq.parquet",
        new LongDimensionSchema("col0"),
        new LongDimensionSchema("col1"),
        new DoubleDimensionSchema("col2"),
        new DoubleDimensionSchema("col3"),
        new LongDimensionSchema("col4"),
        new LongDimensionSchema("col5"),
        new LongDimensionSchema("col6"),
        new StringDimensionSchema("col7"),
        new StringDimensionSchema("col8"),
        new StringDimensionSchema("col9"));
    attachIndex(
        retVal,
        "smlTbl.parquet",
        // "col_int": 8122,
        new LongDimensionSchema("col_int"),
        // "col_bgint": 817200,
        new LongDimensionSchema("col_bgint"),
        // "col_char_2": "IN",
        new StringDimensionSchema("col_char_2"),
        // "col_vchar_52":
        // "AXXXXXXXXXXXXXXXXXXXXXXXXXCXXXXXXXXXXXXXXXXXXXXXXXXB",
        new StringDimensionSchema("col_vchar_52"),
        // "col_tmstmp": 1409617682418,
        new LongDimensionSchema("col_tmstmp"),
        // "col_dt": 422717616000000,
        new LongDimensionSchema("col_dt"),
        // "col_booln": false,
        new StringDimensionSchema("col_booln"),
        // "col_dbl": 12900.48,
        new DoubleDimensionSchema("col_dbl"),
        // "col_tm": 33109170
        new LongDimensionSchema("col_tm"));
    attachIndex(
        retVal,
        "fewRowsAllData.parquet",
        // "col0":12024,
        new LongDimensionSchema("col0"),
        // "col1":307168,
        new LongDimensionSchema("col1"),
        // "col2":"VT",
        new StringDimensionSchema("col2"),
        // "col3":"DXXXXXXXXXXXXXXXXXXXXXXXXXEXXXXXXXXXXXXXXXXXXXXXXXXF",
        new StringDimensionSchema("col3"),
        // "col4":1338596882419,
        new LongDimensionSchema("col4"),
        // "col5":422705433600000,
        new LongDimensionSchema("col5"),
        // "col6":true,
        new StringDimensionSchema("col6"),
        // "col7":3.95110006277E8,
        new DoubleDimensionSchema("col7"),
        // "col8":67465430
        new LongDimensionSchema("col8"));
    attachIndex(
        retVal,
        "t_alltype.parquet",
        // "c1":1,
        new LongDimensionSchema("c1"),
        // "c2":592475043,
        new LongDimensionSchema("c2"),
        // "c3":616080519999272,
        new LongDimensionSchema("c3"),
        // "c4":"ObHeWTDEcbGzssDwPwurfs",
        new StringDimensionSchema("c4"),
        // "c5":"0sZxIfZ CGwTOaLWZ6nWkUNx",
        new StringDimensionSchema("c5"),
        // "c6":1456290852307,
        new LongDimensionSchema("c6"),
        // "c7":421426627200000,
        new LongDimensionSchema("c7"),
        // "c8":true,
        new StringDimensionSchema("c8"),
        // "c9":0.626179100469
        new DoubleDimensionSchema("c9"));

    return retVal;
  }

  public class TextualResultsVerifier implements ResultsVerifier
  {
    protected final List<String[]> expectedResultsText;
    @Nullable
    protected final RowSignature expectedResultRowSignature;
    private RowSignature currentRowSignature;

    public TextualResultsVerifier(List<String[]> expectedResultsString, RowSignature expectedSignature)
    {
      this.expectedResultsText = expectedResultsString;
      this.expectedResultRowSignature = expectedSignature;
    }

    @Override
    public void verifyRowSignature(RowSignature rowSignature)
    {
      if (expectedResultRowSignature != null) {
        Assert.assertEquals(expectedResultRowSignature, rowSignature);
      }
      currentRowSignature = rowSignature;
    }

    @Override
    public void verify(String sql, List<Object[]> results)
    {
      List<Object[]> expectedResults = parseResults(currentRowSignature, expectedResultsText);
      try {
        Assert.assertEquals(StringUtils.format("result count: %s", sql), expectedResultsText.size(), results.size());
        if (!isOrdered(sql)) {
          results.sort(new ArrayRowCmp());
          expectedResults.sort(new ArrayRowCmp());
        } else {
          assertResultsEquals(sql, expectedResults, results);
        }
      }
      catch (AssertionError e) {
        displayResults(expectedResults);
        System.out.println("query: " + sql);
        displayResults(results);
        throw e;
      }
    }

    private boolean isOrdered(String sql)
    {
      // FIXME: SqlToRelConverter.isOrdered(null) would be better
      sql = sql.toLowerCase(Locale.ENGLISH).replace('\n', ' ');
      sql = sql.substring(sql.lastIndexOf(')'));
      return sql.contains("order");
    }
  }

  static class ArrayRowCmp implements Comparator<Object[]>
  {
    @Override
    public int compare(Object[] arg0, Object[] arg1)
    {
      String s0 = Arrays.toString(arg0);
      String s1 = Arrays.toString(arg1);
      return s0.compareTo(s1);
    }
  }

  private static List<Object[]> parseResults(RowSignature rs, List<String[]> results)
  {
    Predicate<String> longPattern = Pattern.compile("^-?[0-9]+$").asPredicate();
    List<Object[]> ret = new ArrayList<>();
    for (String[] row : results) {
      Object[] newRow = new Object[row.length];
      List<String> cc = rs.getColumnNames();
      for (int i = 0; i < cc.size(); i++) {
        ColumnType type = rs.getColumnType(i).get();
        assertNull(type.getComplexTypeName());
        final String val = row[i];
        final Object newVal;
        if ("null".equals(val)) {
          newVal = null;
        } else {
          switch (type.getType()) {
            case STRING:
              newVal = val;
              break;
            case LONG:
              if (longPattern.test(val)) {
                newVal = Numbers.parseLong(val);
              } else {
                Function<String, DateTime> parser = TimestampParser.createTimestampParser("auto");
                newVal = parser.apply(val);
              }
              break;
            case DOUBLE:
              newVal = Numbers.parseDoubleObject(val);
              break;
            default:
              throw new RuntimeException("unimplemented");
          }
        }
        newRow[i] = newVal;
      }
      ret.add(newRow);
    }
    return ret;
  }

  public void windowQueryTest() throws Exception
  {
    Thread thread = null;
    String oldName = null;
    try {
      thread = Thread.currentThread();
      oldName = thread.getName();
      thread.setName("drillWindowQuery-" + testCase.filename);

      testBuilder()
          .skipVectorize(true)
          .queryContext(ImmutableMap.of(
              PlannerContext.CTX_ENABLE_WINDOW_FNS, true,
              "windowsAllTheWayDown", true,
              QueryContexts.ENABLE_DEBUG, true,
              PlannerConfig.CTX_KEY_USE_APPROXIMATE_COUNT_DISTINCT, false))
          .sql(testCase.getQueryString())
          .expectedResults(new TextualResultsVerifier(testCase.getExpectedResults(), null))
          .run();
    }
    finally {
      if (thread != null && oldName != null) {
        thread.setName(oldName);
      }
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void attachIndex(SpecificSegmentsQuerySegmentWalker texasRanger, String dataSource, DimensionSchema... dims)
      throws IOException
  {
    ArrayList<String> dimensionNames = new ArrayList<>(dims.length);
    for (DimensionSchema dimension : dims) {
      dimensionNames.add(dimension.getName());
    }

    final File tmpFolder = temporaryFolder.newFolder();
    final QueryableIndex queryableIndex = IndexBuilder
        .create()
        .tmpDir(new File(tmpFolder, dataSource))
        .segmentWriteOutMediumFactory(OnHeapMemorySegmentWriteOutMediumFactory.instance())
        .schema(new IncrementalIndexSchema.Builder()
            .withRollup(false)
            .withDimensionsSpec(new DimensionsSpec(Arrays.asList(dims)))
            .build())
        .rows(
            () -> {
              try {
                return Iterators.transform(
                    MAPPER.readerFor(Map.class)
                        .readValues(
                            ClassLoader.getSystemResource("drill/window/datasources/" + dataSource + ".json")),
                    (Function<Map, InputRow>) input -> new MapBasedInputRow(0, dimensionNames, input));
              }
              catch (IOException e) {
                throw new RE(e, "problem reading file");
              }
            })
        .buildMMappedIndex();

    texasRanger.add(
        DataSegment.builder()
            .dataSource(dataSource)
            .interval(Intervals.ETERNITY)
            .version("1")
            .shardSpec(new NumberedShardSpec(0, 0))
            .size(0)
            .build(),
        queryableIndex);
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("aggregates/winFnQry_77")
  @Test
  public void test_aggregates_winFnQry_77() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_76")
  @Test
  public void test_lead_func_lead_Fn_76() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_10")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_int_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_20")
  @Test
  public void test_aggregates_aggOWnFn_20() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bgint_1")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_bgint_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_33")
  @Test
  public void test_ntile_func_ntileFn_33() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_30")
  @Test
  public void test_aggregates_testW_Nulls_30() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_chr_5")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_chr_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_1")
  @Test
  public void test_last_val_lastValFn_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_35")
  @Test
  public void test_last_val_lastValFn_35() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_vchr_5")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_vchr_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_100")
  @Test
  public void test_lead_func_lead_Fn_100() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_7")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_int_7() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/woutPrtnBy_7")
  @Test
  public void test_nestedAggs_woutPrtnBy_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_6")
  @Test
  public void test_aggregates_aggOWnFn_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_22")
  @Test
  public void test_aggregates_winFnQry_22() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_68")
  @Test
  public void test_aggregates_winFnQry_68() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_chr_1")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_chr_1() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lead_func/lead_Fn_63")
  @Test
  public void test_lead_func_lead_Fn_63() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_46")
  @Test
  public void test_lag_func_lag_Fn_46() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_21")
  @Test
  public void test_aggregates_woPrtnBy_21() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_44")
  @Test
  public void test_ntile_func_ntileFn_44() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_25")
  @Test
  public void test_first_val_firstValFn_25() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/rnkNoFrm01")
  @Test
  public void test_frameclause_multipl_wnwds_rnkNoFrm01() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_7")
  @Test
  public void test_lag_func_lag_Fn_7() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause07")
  @Test
  public void test_nestedAggs_frmclause07() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_33")
  @Test
  public void test_aggregates_winFnQry_33() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_79")
  @Test
  public void test_aggregates_winFnQry_79() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_5")
  @Test
  public void test_lead_func_lead_Fn_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_74")
  @Test
  public void test_lead_func_lead_Fn_74() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_46")
  @Test
  public void test_aggregates_aggOWnFn_46() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_10")
  @Test
  public void test_first_val_firstValFn_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_10")
  @Test
  public void test_aggregates_woPrtnBy_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_78")
  @Test
  public void test_lead_func_lead_Fn_78() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_13")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_int_13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_13")
  @Test
  public void test_ntile_func_ntileFn_13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_59")
  @Test
  public void test_ntile_func_ntileFn_59() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dbl_3")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_dbl_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_5")
  @Test
  public void test_ntile_func_ntileFn_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/max_mulwds")
  @Test
  public void test_frameclause_multipl_wnwds_max_mulwds() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_15")
  @Test
  public void test_last_val_lastValFn_15() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_3")
  @Test
  public void test_aggregates_wo_OrdrBy_3() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/woutOby_6")
  @Test
  public void test_nestedAggs_woutOby_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_43")
  @Test
  public void test_lead_func_lead_Fn_43() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int7")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_int7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_35")
  @Test
  public void test_lag_func_lag_Fn_35() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_44")
  @Test
  public void test_aggregates_winFnQry_44() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lag_func/lag_Fn_109")
  @Test
  public void test_lag_func_lag_Fn_109() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_35")
  @Test
  public void test_aggregates_aggOWnFn_35() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_2")
  @Test
  public void test_aggregates_wPrtnOrdrBy_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_89")
  @Test
  public void test_lead_func_lead_Fn_89() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_45")
  @Test
  public void test_aggregates_woPrtnBy_45() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg23")
  @Test
  public void test_nestedAggs_nstdagg23() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_24")
  @Test
  public void test_ntile_func_ntileFn_24() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dbl_2")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_dbl_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_26")
  @Test
  public void test_last_val_lastValFn_26() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_6")
  @Test
  public void test_nestedAggs_emtyOvrCls_6() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.RESULT_PARSE_EXCEPTION)
  @DrillTest("lead_func/lead_Fn_54")
  @Test
  public void test_lead_func_lead_Fn_54() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_54")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_54() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_24")
  @Test
  public void test_lag_func_lag_Fn_24() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_55")
  @Test
  public void test_aggregates_winFnQry_55() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_59")
  @Test
  public void test_lag_func_lag_Fn_59() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_34")
  @Test
  public void test_aggregates_woPrtnBy_34() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_12")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_int_12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dbl_6")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_dbl_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_22")
  @Test
  public void test_aggregates_aggOWnFn_22() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_35")
  @Test
  public void test_ntile_func_ntileFn_35() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg10")
  @Test
  public void test_nestedAggs_nstdagg10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_102")
  @Test
  public void test_lead_func_lead_Fn_102() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/avg_mulwds")
  @Test
  public void test_frameclause_multipl_wnwds_avg_mulwds() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_3")
  @Test
  public void test_last_val_lastValFn_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_37")
  @Test
  public void test_last_val_lastValFn_37() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_41")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_41() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_8")
  @Test
  public void test_aggregates_aggOWnFn_8() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutPrtnBy_5")
  @Test
  public void test_nestedAggs_woutPrtnBy_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/cte_win_01")
  @Test
  public void test_nestedAggs_cte_win_01() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_chr_3")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_chr_3() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NPE)
  @DrillTest("aggregates/winFnQry_20")
  @Test
  public void test_aggregates_winFnQry_20() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_9")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_int_9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_vchar_4")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_vchar_4() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lead_func/lead_Fn_65")
  @Test
  public void test_lead_func_lead_Fn_65() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_66")
  @Test
  public void test_aggregates_winFnQry_66() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_8")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_int_8() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_13")
  @Test
  public void test_lag_func_lag_Fn_13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int3")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_int3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/rnkNoFrm03")
  @Test
  public void test_frameclause_multipl_wnwds_rnkNoFrm03() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_48")
  @Test
  public void test_lag_func_lag_Fn_48() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_2")
  @Test
  public void test_nestedAggs_wPbOb_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_23")
  @Test
  public void test_aggregates_woPrtnBy_23() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_23")
  @Test
  public void test_first_val_firstValFn_23() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_11")
  @Test
  public void test_aggregates_aggOWnFn_11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_46")
  @Test
  public void test_ntile_func_ntileFn_46() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_5")
  @Test
  public void test_lag_func_lag_Fn_5() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause09")
  @Test
  public void test_nestedAggs_frmclause09() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_char_1")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_char_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_30")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_30() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NPE)
  @DrillTest("nestedAggs/basic_10")
  @Test
  public void test_nestedAggs_basic_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bgint_6")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_bgint_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_31")
  @Test
  public void test_aggregates_winFnQry_31() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_3")
  @Test
  public void test_lead_func_lead_Fn_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_30")
  @Test
  public void test_lead_func_lead_Fn_30() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_38")
  @Test
  public void test_aggregates_woPrtnBy_38() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/basic_8")
  @Test
  public void test_nestedAggs_basic_8() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_28")
  @Test
  public void test_lag_func_lag_Fn_28() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_81")
  @Test
  public void test_lag_func_lag_Fn_81() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_59")
  @Test
  public void test_aggregates_winFnQry_59() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_2")
  @Test
  public void test_nestedAggs_emtyOvrCls_2() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NPE)
  @DrillTest("aggregates/winFnQry_13")
  @Test
  public void test_aggregates_winFnQry_13() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_12")
  @Test
  public void test_lead_func_lead_Fn_12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_58")
  @Test
  public void test_lead_func_lead_Fn_58() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_30")
  @Test
  public void test_first_val_firstValFn_30() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_33")
  @Test
  public void test_last_val_lastValFn_33() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bgint_3")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_bgint_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_19")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_19() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dbl_6")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_dbl_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_vchr_3")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_vchr_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_39")
  @Test
  public void test_ntile_func_ntileFn_39() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_17")
  @Test
  public void test_aggregates_testW_Nulls_17() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/cte_win_05")
  @Test
  public void test_nestedAggs_cte_win_05() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/fval_mulwds")
  @Test
  public void test_frameclause_multipl_wnwds_fval_mulwds() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_27")
  @Test
  public void test_aggregates_woPrtnBy_27() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutPrtnBy_1")
  @Test
  public void test_nestedAggs_woutPrtnBy_1() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lag_func/lag_Fn_70")
  @Test
  public void test_lag_func_lag_Fn_70() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_4")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_int_4() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lag_func/lag_Fn_17")
  @Test
  public void test_lag_func_lag_Fn_17() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_24")
  @Test
  public void test_aggregates_winFnQry_24() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_23")
  @Test
  public void test_lead_func_lead_Fn_23() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.MISSING_DESC)
  @DrillTest("lead_func/lead_Fn_69")
  @Test
  public void test_lead_func_lead_Fn_69() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int12")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_int12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bgint_2")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_bgint_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dt_1")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_dt_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_08")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_08() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_vchr_1")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_vchr_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_6")
  @Test
  public void test_aggregates_woPrtnBy_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_char_5")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_char_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_9")
  @Test
  public void test_lag_func_lag_Fn_9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_16")
  @Test
  public void test_aggregates_woPrtnBy_16() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/lval_mulwds")
  @Test
  public void test_frameclause_multipl_wnwds_lval_mulwds() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_7")
  @Test
  public void test_lead_func_lead_Fn_7() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_35")
  @Test
  public void test_aggregates_winFnQry_35() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bln_1")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_bln_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_11")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_int_11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_33")
  @Test
  public void test_lag_func_lag_Fn_33() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_79")
  @Test
  public void test_lag_func_lag_Fn_79() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_107")
  @Test
  public void test_lag_func_lag_Fn_107() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_44")
  @Test
  public void test_aggregates_aggOWnFn_44() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_12")
  @Test
  public void test_first_val_firstValFn_12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_11")
  @Test
  public void test_ntile_func_ntileFn_11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_3")
  @Test
  public void test_ntile_func_ntileFn_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_57")
  @Test
  public void test_ntile_func_ntileFn_57() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_13")
  @Test
  public void test_last_val_lastValFn_13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dbl_5")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_dbl_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_18")
  @Test
  public void test_nestedAggs_wPbOb_18() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause18")
  @Test
  public void test_nestedAggs_frmclause18() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_2")
  @Test
  public void test_aggregates_woPrtnBy_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_46")
  @Test
  public void test_aggregates_winFnQry_46() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_1")
  @Test
  public void test_aggregates_wo_OrdrBy_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_87")
  @Test
  public void test_lead_func_lead_Fn_87() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int5")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_int5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutOby_8")
  @Test
  public void test_nestedAggs_woutOby_8() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_41")
  @Test
  public void test_lead_func_lead_Fn_41() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int5")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_int5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_63")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_63() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_22")
  @Test
  public void test_lag_func_lag_Fn_22() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_68")
  @Test
  public void test_lag_func_lag_Fn_68() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_43")
  @Test
  public void test_aggregates_woPrtnBy_43() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutOby_13")
  @Test
  public void test_nestedAggs_woutOby_13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_33")
  @Test
  public void test_aggregates_aggOWnFn_33() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_22")
  @Test
  public void test_ntile_func_ntileFn_22() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg21")
  @Test
  public void test_nestedAggs_nstdagg21() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_24")
  @Test
  public void test_last_val_lastValFn_24() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_4")
  @Test
  public void test_aggregates_wPrtnOrdrBy_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_52")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_52() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NPE)
  @DrillTest("frameclause/multipl_wnwds/min_mulwds")
  @Test
  public void test_frameclause_multipl_wnwds_min_mulwds() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_8")
  @Test
  public void test_nestedAggs_emtyOvrCls_8() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_11")
  @Test
  public void test_aggregates_winFnQry_11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_57")
  @Test
  public void test_aggregates_winFnQry_57() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bln_2")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_bln_2() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lead_func/lead_Fn_98")
  @Test
  public void test_lead_func_lead_Fn_98() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_52")
  @Test
  public void test_lead_func_lead_Fn_52() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_11")
  @Test
  public void test_lag_func_lag_Fn_11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_57")
  @Test
  public void test_lag_func_lag_Fn_57() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_32")
  @Test
  public void test_aggregates_woPrtnBy_32() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_37")
  @Test
  public void test_ntile_func_ntileFn_37() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_19")
  @Test
  public void test_aggregates_testW_Nulls_19() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/cte_win_03")
  @Test
  public void test_nestedAggs_cte_win_03() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_6")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_int_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_vchar_4")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_vchar_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutPrtnBy_3")
  @Test
  public void test_nestedAggs_woutPrtnBy_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_9")
  @Test
  public void test_aggregates_testW_Nulls_9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_26")
  @Test
  public void test_aggregates_winFnQry_26() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_15")
  @Test
  public void test_lag_func_lag_Fn_15() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_21")
  @Test
  public void test_lead_func_lead_Fn_21() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/rnkNoFrm05")
  @Test
  public void test_frameclause_multipl_wnwds_rnkNoFrm05() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_21")
  @Test
  public void test_first_val_firstValFn_21() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int1")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_int1() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_67")
  @Test
  public void test_lead_func_lead_Fn_67() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dt_3")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_dt_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_06")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_06() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_25")
  @Test
  public void test_aggregates_woPrtnBy_25() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int14")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_int14() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bgint_4")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_bgint_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_48")
  @Test
  public void test_ntile_func_ntileFn_48() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_51")
  @Test
  public void test_aggregates_aggOWnFn_51() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_8")
  @Test
  public void test_aggregates_woPrtnBy_8() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_char_3")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_char_3() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_37")
  @Test
  public void test_aggregates_winFnQry_37() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_32")
  @Test
  public void test_lead_func_lead_Fn_32() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_9")
  @Test
  public void test_ntile_func_ntileFn_9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_11")
  @Test
  public void test_last_val_lastValFn_11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_2")
  @Test
  public void test_first_val_firstValFn_2() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lead_func/lead_Fn_36")
  @Test
  public void test_lead_func_lead_Fn_36() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_42")
  @Test
  public void test_aggregates_aggOWnFn_42() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_14")
  @Test
  public void test_aggregates_woPrtnBy_14() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_17")
  @Test
  public void test_ntile_func_ntileFn_17() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_16")
  @Test
  public void test_nestedAggs_wPbOb_16() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_4")
  @Test
  public void test_aggregates_woPrtnBy_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_1")
  @Test
  public void test_aggregates_winFnQry_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int3")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_int3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_39")
  @Test
  public void test_lag_func_lag_Fn_39() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_92")
  @Test
  public void test_lag_func_lag_Fn_92() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_48")
  @Test
  public void test_aggregates_winFnQry_48() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_49")
  @Test
  public void test_aggregates_woPrtnBy_49() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_61")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_61() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_47")
  @Test
  public void test_lead_func_lead_Fn_47() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_22")
  @Test
  public void test_last_val_lastValFn_22() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_31")
  @Test
  public void test_aggregates_aggOWnFn_31() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_22")
  @Test
  public void test_aggregates_wo_OrdrBy_22() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutOby_2")
  @Test
  public void test_nestedAggs_woutOby_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_28")
  @Test
  public void test_ntile_func_ntileFn_28() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutOby_11")
  @Test
  public void test_nestedAggs_woutOby_11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bgint_7")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_bgint_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_50")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_50() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_4")
  @Test
  public void test_ntile_func_ntileFn_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_1")
  @Test
  public void test_aggregates_woPrtnBy_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_64")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_64() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutOby_5")
  @Test
  public void test_nestedAggs_woutOby_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_88")
  @Test
  public void test_lead_func_lead_Fn_88() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int6")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_int6() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_43")
  @Test
  public void test_aggregates_winFnQry_43() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_4")
  @Test
  public void test_aggregates_wo_OrdrBy_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_42")
  @Test
  public void test_lead_func_lead_Fn_42() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_3")
  @Test
  public void test_aggregates_wPrtnOrdrBy_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_46")
  @Test
  public void test_aggregates_woPrtnBy_46() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_34")
  @Test
  public void test_aggregates_aggOWnFn_34() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/sum_mulwds")
  @Test
  public void test_frameclause_multipl_wnwds_sum_mulwds() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_23")
  @Test
  public void test_ntile_func_ntileFn_23() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg22")
  @Test
  public void test_nestedAggs_nstdagg22() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_25")
  @Test
  public void test_last_val_lastValFn_25() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dbl_3")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_dbl_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bgint_1")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_bgint_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_99")
  @Test
  public void test_lead_func_lead_Fn_99() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_7")
  @Test
  public void test_nestedAggs_emtyOvrCls_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bln_3")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_bln_3() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_25")
  @Test
  public void test_lag_func_lag_Fn_25() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_54")
  @Test
  public void test_aggregates_winFnQry_54() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_53")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_53() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_53")
  @Test
  public void test_lead_func_lead_Fn_53() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_48")
  @Test
  public void test_aggregates_aggOWnFn_48() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_53")
  @Test
  public void test_ntile_func_ntileFn_53() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_17")
  @Test
  public void test_last_val_lastValFn_17() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg13")
  @Test
  public void test_nestedAggs_nstdagg13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_105")
  @Test
  public void test_lead_func_lead_Fn_105() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_12")
  @Test
  public void test_aggregates_testW_Nulls_12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_50")
  @Test
  public void test_aggregates_woPrtnBy_50() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dbl_1")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_dbl_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/frmclause14")
  @Test
  public void test_nestedAggs_frmclause14() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_16")
  @Test
  public void test_first_val_firstValFn_16() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_chr_4")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_chr_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_60")
  @Test
  public void test_lead_func_lead_Fn_60() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int9")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_int9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_52")
  @Test
  public void test_lag_func_lag_Fn_52() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_98")
  @Test
  public void test_lag_func_lag_Fn_98() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_8")
  @Test
  public void test_first_val_firstValFn_8() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_5")
  @Test
  public void test_aggregates_wo_OrdrBy_5() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_65")
  @Test
  public void test_aggregates_winFnQry_65() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int9")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_int9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_21")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_21() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_37")
  @Test
  public void test_aggregates_aggOWnFn_37() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dt_4")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_dt_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_1")
  @Test
  public void test_aggregates_aggOWnFn_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_28")
  @Test
  public void test_last_val_lastValFn_28() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_8")
  @Test
  public void test_aggregates_wPrtnOrdrBy_8() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg02")
  @Test
  public void test_nestedAggs_nstdagg02() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_4")
  @Test
  public void test_lag_func_lag_Fn_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dt_3")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_dt_3() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_71")
  @Test
  public void test_lead_func_lead_Fn_71() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/mtyOvrCluse_03")
  @Test
  public void test_aggregates_mtyOvrCluse_03() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_10")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_56")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_56() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lag_func/lag_Fn_41")
  @Test
  public void test_lag_func_lag_Fn_41() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_87")
  @Test
  public void test_lag_func_lag_Fn_87() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("aggregates/winFnQry_76")
  @Test
  public void test_aggregates_winFnQry_76() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_104")
  @Test
  public void test_lag_func_lag_Fn_104() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_30")
  @Test
  public void test_lag_func_lag_Fn_30() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_1")
  @Test
  public void test_aggregates_testW_Nulls_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dbl_4")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_dbl_4() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_18")
  @Test
  public void test_lead_func_lead_Fn_18() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_14")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_int_14() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_39")
  @Test
  public void test_last_val_lastValFn_39() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_5")
  @Test
  public void test_last_val_lastValFn_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_34")
  @Test
  public void test_aggregates_testW_Nulls_34() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NPE)
  @DrillTest("frameclause/RBUPACR/RBUPACR_chr_1")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_chr_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_3")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_int_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_15")
  @Test
  public void test_aggregates_wo_OrdrBy_15() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_82")
  @Test
  public void test_lead_func_lead_Fn_82() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_41")
  @Test
  public void test_aggregates_winFnQry_41() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_vchar_2")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_vchar_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_43")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_43() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_76")
  @Test
  public void test_lag_func_lag_Fn_76() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dbl_5")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_dbl_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_40")
  @Test
  public void test_ntile_func_ntileFn_40() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_13")
  @Test
  public void test_aggregates_aggOWnFn_13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_4")
  @Test
  public void test_nestedAggs_wPbOb_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_29")
  @Test
  public void test_lead_func_lead_Fn_29() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg24")
  @Test
  public void test_nestedAggs_nstdagg24() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_23")
  @Test
  public void test_aggregates_testW_Nulls_23() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bln_1")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_bln_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/frmclause03")
  @Test
  public void test_nestedAggs_frmclause03() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_29")
  @Test
  public void test_first_val_firstValFn_29() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bgint_3")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_bgint_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_52")
  @Test
  public void test_aggregates_winFnQry_52() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_93")
  @Test
  public void test_lead_func_lead_Fn_93() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dt_1")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_dt_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_32")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_32() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lag_func/lag_Fn_65")
  @Test
  public void test_lag_func_lag_Fn_65() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_55")
  @Test
  public void test_ntile_func_ntileFn_55() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_14")
  @Test
  public void test_first_val_firstValFn_14() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_107")
  @Test
  public void test_lead_func_lead_Fn_107() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_10")
  @Test
  public void test_aggregates_testW_Nulls_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_19")
  @Test
  public void test_last_val_lastValFn_19() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dbl_7")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_dbl_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg15")
  @Test
  public void test_nestedAggs_nstdagg15() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/frmclause16")
  @Test
  public void test_nestedAggs_frmclause16() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_62")
  @Test
  public void test_lead_func_lead_Fn_62() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.MISSING_DESC)
  @DrillTest("aggregates/winFnQry_63")
  @Test
  public void test_aggregates_winFnQry_63() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_7")
  @Test
  public void test_aggregates_wo_OrdrBy_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int7")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_int7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_23")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_23() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_39")
  @Test
  public void test_aggregates_aggOWnFn_39() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_54")
  @Test
  public void test_lag_func_lag_Fn_54() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_41")
  @Test
  public void test_aggregates_woPrtnBy_41() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_89")
  @Test
  public void test_lag_func_lag_Fn_89() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dt_2")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_dt_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dt_1")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_dt_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_20")
  @Test
  public void test_ntile_func_ntileFn_20() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NOT_ENOUGH_RULES)
  @DrillTest("aggregates/aggOWnFn_3")
  @Test
  public void test_aggregates_aggOWnFn_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_2")
  @Test
  public void test_lag_func_lag_Fn_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_6")
  @Test
  public void test_aggregates_wPrtnOrdrBy_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg04")
  @Test
  public void test_nestedAggs_nstdagg04() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_73")
  @Test
  public void test_lead_func_lead_Fn_73() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_58")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_58() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/mtyOvrCluse_05")
  @Test
  public void test_aggregates_mtyOvrCluse_05() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_74")
  @Test
  public void test_aggregates_winFnQry_74() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_12")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_43")
  @Test
  public void test_lag_func_lag_Fn_43() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_30")
  @Test
  public void test_aggregates_woPrtnBy_30() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_106")
  @Test
  public void test_lag_func_lag_Fn_106() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_32")
  @Test
  public void test_lag_func_lag_Fn_32() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_78")
  @Test
  public void test_lag_func_lag_Fn_78() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dbl_2")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_dbl_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_10")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_int_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_31")
  @Test
  public void test_ntile_func_ntileFn_31() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_3")
  @Test
  public void test_aggregates_testW_Nulls_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_chr_3")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_chr_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_32")
  @Test
  public void test_aggregates_testW_Nulls_32() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_7")
  @Test
  public void test_last_val_lastValFn_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_char_2")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_char_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_5")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_int_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_17")
  @Test
  public void test_aggregates_wo_OrdrBy_17() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_45")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_45() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_85")
  @Test
  public void test_aggregates_winFnQry_85() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_84")
  @Test
  public void test_lead_func_lead_Fn_84() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_21")
  @Test
  public void test_lag_func_lag_Fn_21() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_67")
  @Test
  public void test_lag_func_lag_Fn_67() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_50")
  @Test
  public void test_aggregates_winFnQry_50() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dbl_7")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_dbl_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_15")
  @Test
  public void test_aggregates_aggOWnFn_15() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_42")
  @Test
  public void test_ntile_func_ntileFn_42() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bln_2")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_bln_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_21")
  @Test
  public void test_aggregates_testW_Nulls_21() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg26")
  @Test
  public void test_nestedAggs_nstdagg26() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause05")
  @Test
  public void test_nestedAggs_frmclause05() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_21")
  @Test
  public void test_nestedAggs_wPbOb_21() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_27")
  @Test
  public void test_first_val_firstValFn_27() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_34")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_34() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bgint_5")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_bgint_5() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lead_func/lead_Fn_95")
  @Test
  public void test_lead_func_lead_Fn_95() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_6")
  @Test
  public void test_nestedAggs_wPbOb_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_4")
  @Test
  public void test_first_val_firstValFn_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/basic_1")
  @Test
  public void test_nestedAggs_basic_1() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_61")
  @Test
  public void test_aggregates_winFnQry_61() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_9")
  @Test
  public void test_aggregates_wo_OrdrBy_9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_38")
  @Test
  public void test_lead_func_lead_Fn_38() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dbl_5")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_dbl_5() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause10")
  @Test
  public void test_nestedAggs_frmclause10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg17")
  @Test
  public void test_nestedAggs_nstdagg17() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_16")
  @Test
  public void test_aggregates_testW_Nulls_16() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int12")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_int12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_1")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_int_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_10")
  @Test
  public void test_nestedAggs_wPbOb_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_94")
  @Test
  public void test_lag_func_lag_Fn_94() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_28")
  @Test
  public void test_aggregates_woPrtnBy_28() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_25")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_25() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_72")
  @Test
  public void test_aggregates_winFnQry_72() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bgint_1")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_bgint_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_111")
  @Test
  public void test_lag_func_lag_Fn_111() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_49")
  @Test
  public void test_lead_func_lead_Fn_49() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg06")
  @Test
  public void test_nestedAggs_nstdagg06() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_vchr_2")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_vchr_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_19")
  @Test
  public void test_aggregates_winFnQry_19() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bgint_1")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_bgint_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_24")
  @Test
  public void test_aggregates_wo_OrdrBy_24() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_14")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_14() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_83")
  @Test
  public void test_lag_func_lag_Fn_83() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_17")
  @Test
  public void test_aggregates_woPrtnBy_17() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dt_5")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_dt_5() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_14")
  @Test
  public void test_lead_func_lead_Fn_14() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.MISSING_DESC)
  @DrillTest("aggregates/winFnQry_83")
  @Test
  public void test_aggregates_winFnQry_83() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_9")
  @Test
  public void test_last_val_lastValFn_9() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lag_func/lag_Fn_100")
  @Test
  public void test_lag_func_lag_Fn_100() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/multiWin_8")
  @Test
  public void test_nestedAggs_multiWin_8() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_11")
  @Test
  public void test_aggregates_wo_OrdrBy_11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_18")
  @Test
  public void test_ntile_func_ntileFn_18() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_38")
  @Test
  public void test_aggregates_testW_Nulls_38() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_5")
  @Test
  public void test_aggregates_testW_Nulls_5() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_19")
  @Test
  public void test_lag_func_lag_Fn_19() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_47")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_47() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_72")
  @Test
  public void test_lag_func_lag_Fn_72() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_17")
  @Test
  public void test_aggregates_aggOWnFn_17() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_01")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_01() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_8")
  @Test
  public void test_aggregates_winFnQry_8() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_25")
  @Test
  public void test_lead_func_lead_Fn_25() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_char_4")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_char_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int10")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_int10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_29")
  @Test
  public void test_ntile_func_ntileFn_29() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_23")
  @Test
  public void test_nestedAggs_wPbOb_23() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_27")
  @Test
  public void test_aggregates_testW_Nulls_27() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bln_3")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_bln_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_12")
  @Test
  public void test_nestedAggs_emtyOvrCls_12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_36")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_36() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_61")
  @Test
  public void test_lag_func_lag_Fn_61() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bgint_7")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_bgint_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_8")
  @Test
  public void test_nestedAggs_wPbOb_8() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_51")
  @Test
  public void test_ntile_func_ntileFn_51() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/basic_3")
  @Test
  public void test_nestedAggs_basic_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_30")
  @Test
  public void test_last_val_lastValFn_30() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/frmclause12")
  @Test
  public void test_nestedAggs_frmclause12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_14")
  @Test
  public void test_aggregates_testW_Nulls_14() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int14")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_int14() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_12")
  @Test
  public void test_nestedAggs_wPbOb_12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg19")
  @Test
  public void test_nestedAggs_nstdagg19() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dbl_3")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_dbl_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_27")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_27() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_18")
  @Test
  public void test_first_val_firstValFn_18() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_50")
  @Test
  public void test_lag_func_lag_Fn_50() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lag_func/lag_Fn_96")
  @Test
  public void test_lag_func_lag_Fn_96() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_6")
  @Test
  public void test_first_val_firstValFn_6() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_70")
  @Test
  public void test_aggregates_winFnQry_70() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_9")
  @Test
  public void test_aggregates_woPrtnBy_9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_50")
  @Test
  public void test_aggregates_aggOWnFn_50() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_vchr_4")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_vchr_4() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.MISSING_DESC)
  @DrillTest("frameclause/multipl_wnwds/mulwind_06")
  @Test
  public void test_frameclause_multipl_wnwds_mulwind_06() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_17")
  @Test
  public void test_aggregates_winFnQry_17() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg08")
  @Test
  public void test_nestedAggs_nstdagg08() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bgint_3")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_bgint_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dt_5")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_dt_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_26")
  @Test
  public void test_aggregates_wo_OrdrBy_26() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_16")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_16() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_19")
  @Test
  public void test_aggregates_woPrtnBy_19() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_85")
  @Test
  public void test_lag_func_lag_Fn_85() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/multiWin_6")
  @Test
  public void test_nestedAggs_multiWin_6() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_81")
  @Test
  public void test_aggregates_winFnQry_81() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_vchar_2")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_vchar_2() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lag_func/lag_Fn_102")
  @Test
  public void test_lag_func_lag_Fn_102() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_16")
  @Test
  public void test_lead_func_lead_Fn_16() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_36")
  @Test
  public void test_aggregates_testW_Nulls_36() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_28")
  @Test
  public void test_aggregates_winFnQry_28() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_1")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_int_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_80")
  @Test
  public void test_lead_func_lead_Fn_80() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_13")
  @Test
  public void test_aggregates_wo_OrdrBy_13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_03")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_03() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_49")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_49() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_7")
  @Test
  public void test_aggregates_testW_Nulls_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_6")
  @Test
  public void test_aggregates_winFnQry_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_74")
  @Test
  public void test_lag_func_lag_Fn_74() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_19")
  @Test
  public void test_aggregates_aggOWnFn_19() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dt_5")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_dt_5() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lead_func/lead_Fn_27")
  @Test
  public void test_lead_func_lead_Fn_27() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_10")
  @Test
  public void test_nestedAggs_emtyOvrCls_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/frmclause01")
  @Test
  public void test_nestedAggs_frmclause01() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_25")
  @Test
  public void test_aggregates_testW_Nulls_25() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_39")
  @Test
  public void test_aggregates_winFnQry_39() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_25")
  @Test
  public void test_nestedAggs_wPbOb_25() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lead_func/lead_Fn_91")
  @Test
  public void test_lead_func_lead_Fn_91() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bln_3")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_bln_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_38")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_38() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dt_3")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_dt_3() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lag_func/lag_Fn_63")
  @Test
  public void test_lag_func_lag_Fn_63() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/basic_5")
  @Test
  public void test_nestedAggs_basic_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_34")
  @Test
  public void test_lead_func_lead_Fn_34() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_12")
  @Test
  public void test_aggregates_woPrtnBy_12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int1")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_int1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/count_mulwds")
  @Test
  public void test_frameclause_multipl_wnwds_count_mulwds() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_40")
  @Test
  public void test_aggregates_aggOWnFn_40() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dbl_1")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_dbl_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_15")
  @Test
  public void test_ntile_func_ntileFn_15() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_7")
  @Test
  public void test_ntile_func_ntileFn_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_14")
  @Test
  public void test_nestedAggs_wPbOb_14() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_90")
  @Test
  public void test_lag_func_lag_Fn_90() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_3")
  @Test
  public void test_aggregates_winFnQry_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutOby_4")
  @Test
  public void test_nestedAggs_woutOby_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_37")
  @Test
  public void test_lag_func_lag_Fn_37() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_20")
  @Test
  public void test_last_val_lastValFn_20() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lead_func/lead_Fn_45")
  @Test
  public void test_lead_func_lead_Fn_45() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_20")
  @Test
  public void test_aggregates_wo_OrdrBy_20() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_47")
  @Test
  public void test_aggregates_woPrtnBy_47() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_26")
  @Test
  public void test_ntile_func_ntileFn_26() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bgint_5")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_bgint_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/mulwind_08")
  @Test
  public void test_frameclause_multipl_wnwds_mulwind_08() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_26")
  @Test
  public void test_lag_func_lag_Fn_26() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_15")
  @Test
  public void test_aggregates_winFnQry_15() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_10")
  @Test
  public void test_lead_func_lead_Fn_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/basic_6")
  @Test
  public void test_nestedAggs_basic_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_4")
  @Test
  public void test_nestedAggs_emtyOvrCls_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_32")
  @Test
  public void test_first_val_firstValFn_32() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_31")
  @Test
  public void test_last_val_lastValFn_31() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_56")
  @Test
  public void test_lead_func_lead_Fn_56() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_vchr_1")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_vchr_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_17")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_17() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_36")
  @Test
  public void test_aggregates_woPrtnBy_36() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lead_func/lead_Fn_9")
  @Test
  public void test_lead_func_lead_Fn_9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bgint_5")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_bgint_5() throws Exception
  {
    windowQueryTest();

  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_22")
  @Test
  public void test_lead_func_lead_Fn_22() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/rnkNoFrm06")
  @Test
  public void test_frameclause_multipl_wnwds_rnkNoFrm06() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_68")
  @Test
  public void test_lead_func_lead_Fn_68() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_20")
  @Test
  public void test_first_val_firstValFn_20() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_07")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_07() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_49")
  @Test
  public void test_lag_func_lag_Fn_49() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dt_2")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_dt_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_24")
  @Test
  public void test_aggregates_woPrtnBy_24() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int13")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_int13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_49")
  @Test
  public void test_ntile_func_ntileFn_49() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_7")
  @Test
  public void test_aggregates_woPrtnBy_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_26")
  @Test
  public void test_nestedAggs_wPbOb_26() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_char_2")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_char_2() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_38")
  @Test
  public void test_aggregates_winFnQry_38() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bgint_5")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_bgint_5() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lead_func/lead_Fn_8")
  @Test
  public void test_lead_func_lead_Fn_8() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_33")
  @Test
  public void test_lead_func_lead_Fn_33() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_79")
  @Test
  public void test_lead_func_lead_Fn_79() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_13")
  @Test
  public void test_aggregates_woPrtnBy_13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dbl_2")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_dbl_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_14")
  @Test
  public void test_ntile_func_ntileFn_14() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_10")
  @Test
  public void test_aggregates_wPrtnOrdrBy_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_15")
  @Test
  public void test_nestedAggs_wPbOb_15() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_6")
  @Test
  public void test_ntile_func_ntileFn_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_4")
  @Test
  public void test_aggregates_winFnQry_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_49")
  @Test
  public void test_aggregates_winFnQry_49() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_91")
  @Test
  public void test_lag_func_lag_Fn_91() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutOby_3")
  @Test
  public void test_nestedAggs_woutOby_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_48")
  @Test
  public void test_aggregates_woPrtnBy_48() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_05")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_05() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_38")
  @Test
  public void test_lag_func_lag_Fn_38() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_44")
  @Test
  public void test_lead_func_lead_Fn_44() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_1")
  @Test
  public void test_aggregates_wPrtnOrdrBy_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_29")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_29() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_25")
  @Test
  public void test_ntile_func_ntileFn_25() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutOby_10")
  @Test
  public void test_nestedAggs_woutOby_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg09")
  @Test
  public void test_nestedAggs_nstdagg09() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bgint_6")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_bgint_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/mulwind_09")
  @Test
  public void test_frameclause_multipl_wnwds_mulwind_09() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_80")
  @Test
  public void test_lag_func_lag_Fn_80() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_37")
  @Test
  public void test_aggregates_woPrtnBy_37() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_14")
  @Test
  public void test_aggregates_winFnQry_14() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_55")
  @Test
  public void test_lead_func_lead_Fn_55() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_5")
  @Test
  public void test_nestedAggs_emtyOvrCls_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/basic_7")
  @Test
  public void test_nestedAggs_basic_7() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_13")
  @Test
  public void test_lead_func_lead_Fn_13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_59")
  @Test
  public void test_lead_func_lead_Fn_59() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_vchr_2")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_vchr_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_34")
  @Test
  public void test_last_val_lastValFn_34() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_10")
  @Test
  public void test_aggregates_wo_OrdrBy_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dbl_7")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_dbl_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bgint_4")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_bgint_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_18")
  @Test
  public void test_aggregates_testW_Nulls_18() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/cte_win_04")
  @Test
  public void test_nestedAggs_cte_win_04() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_3")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_int_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int10")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_int10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_26")
  @Test
  public void test_aggregates_woPrtnBy_26() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_vchar_5")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_vchar_5() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_16")
  @Test
  public void test_lag_func_lag_Fn_16() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_24")
  @Test
  public void test_lead_func_lead_Fn_24() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int11")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_int11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bgint_3")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_bgint_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_09")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_09() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_13")
  @Test
  public void test_nestedAggs_emtyOvrCls_13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_char_4")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_char_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_15")
  @Test
  public void test_aggregates_woPrtnBy_15() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_6")
  @Test
  public void test_lead_func_lead_Fn_6() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_36")
  @Test
  public void test_aggregates_winFnQry_36() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_35")
  @Test
  public void test_lead_func_lead_Fn_35() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_10")
  @Test
  public void test_last_val_lastValFn_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_8")
  @Test
  public void test_ntile_func_ntileFn_8() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_1")
  @Test
  public void test_first_val_firstValFn_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_41")
  @Test
  public void test_aggregates_aggOWnFn_41() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_16")
  @Test
  public void test_ntile_func_ntileFn_16() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_17")
  @Test
  public void test_nestedAggs_wPbOb_17() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_5")
  @Test
  public void test_aggregates_woPrtnBy_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_2")
  @Test
  public void test_aggregates_winFnQry_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int2")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_int2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_47")
  @Test
  public void test_aggregates_winFnQry_47() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_93")
  @Test
  public void test_lag_func_lag_Fn_93() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_60")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_60() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_21")
  @Test
  public void test_last_val_lastValFn_21() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_46")
  @Test
  public void test_lead_func_lead_Fn_46() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_21")
  @Test
  public void test_aggregates_wo_OrdrBy_21() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutOby_1")
  @Test
  public void test_nestedAggs_woutOby_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_30")
  @Test
  public void test_aggregates_aggOWnFn_30() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_110")
  @Test
  public void test_lag_func_lag_Fn_110() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_27")
  @Test
  public void test_ntile_func_ntileFn_27() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutOby_12")
  @Test
  public void test_nestedAggs_woutOby_12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_29")
  @Test
  public void test_aggregates_testW_Nulls_29() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_29")
  @Test
  public void test_lag_func_lag_Fn_29() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/basic_9")
  @Test
  public void test_nestedAggs_basic_9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_58")
  @Test
  public void test_aggregates_winFnQry_58() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_39")
  @Test
  public void test_aggregates_woPrtnBy_39() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NPE)
  @DrillTest("lag_func/lag_Fn_82")
  @Test
  public void test_lag_func_lag_Fn_82() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_3")
  @Test
  public void test_nestedAggs_emtyOvrCls_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/multiWin_1")
  @Test
  public void test_nestedAggs_multiWin_1() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NPE)
  @DrillTest("aggregates/winFnQry_12")
  @Test
  public void test_aggregates_winFnQry_12() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_11")
  @Test
  public void test_lead_func_lead_Fn_11() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_10")
  @Test
  public void test_lag_func_lag_Fn_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_56")
  @Test
  public void test_lag_func_lag_Fn_56() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_31")
  @Test
  public void test_aggregates_woPrtnBy_31() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_21")
  @Test
  public void test_aggregates_aggOWnFn_21() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bgint_2")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_bgint_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_34")
  @Test
  public void test_ntile_func_ntileFn_34() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_11")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_int_11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_31")
  @Test
  public void test_aggregates_testW_Nulls_31() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_chr_4")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_chr_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_vchr_4")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_vchr_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_36")
  @Test
  public void test_last_val_lastValFn_36() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_101")
  @Test
  public void test_lead_func_lead_Fn_101() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_40")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_40() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_5")
  @Test
  public void test_aggregates_aggOWnFn_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_6")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_int_6() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/woutPrtnBy_6")
  @Test
  public void test_nestedAggs_woutPrtnBy_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_vchar_5")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_vchar_5() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_69")
  @Test
  public void test_aggregates_winFnQry_69() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lead_func/lead_Fn_64")
  @Test
  public void test_lead_func_lead_Fn_64() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_23")
  @Test
  public void test_aggregates_winFnQry_23() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_9")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_int_9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int4")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_int4() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lag_func/lag_Fn_45")
  @Test
  public void test_lag_func_lag_Fn_45() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_20")
  @Test
  public void test_aggregates_woPrtnBy_20() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_10")
  @Test
  public void test_aggregates_aggOWnFn_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_24")
  @Test
  public void test_first_val_firstValFn_24() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/rnkNoFrm02")
  @Test
  public void test_frameclause_multipl_wnwds_rnkNoFrm02() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_45")
  @Test
  public void test_ntile_func_ntileFn_45() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_20")
  @Test
  public void test_aggregates_testW_Nulls_20() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_8")
  @Test
  public void test_lag_func_lag_Fn_8() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/frmclause06")
  @Test
  public void test_nestedAggs_frmclause06() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_4")
  @Test
  public void test_lead_func_lead_Fn_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_75")
  @Test
  public void test_lead_func_lead_Fn_75() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_34")
  @Test
  public void test_aggregates_winFnQry_34() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_12")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_int_12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_12")
  @Test
  public void test_last_val_lastValFn_12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_10")
  @Test
  public void test_ntile_func_ntileFn_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_56")
  @Test
  public void test_ntile_func_ntileFn_56() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_43")
  @Test
  public void test_aggregates_aggOWnFn_43() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_13")
  @Test
  public void test_first_val_firstValFn_13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_19")
  @Test
  public void test_nestedAggs_wPbOb_19() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_2")
  @Test
  public void test_ntile_func_ntileFn_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dbl_6")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_dbl_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/frmclause19")
  @Test
  public void test_nestedAggs_frmclause19() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_3")
  @Test
  public void test_aggregates_woPrtnBy_3() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_45")
  @Test
  public void test_aggregates_winFnQry_45() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_2")
  @Test
  public void test_aggregates_wo_OrdrBy_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_40")
  @Test
  public void test_lead_func_lead_Fn_40() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_86")
  @Test
  public void test_lead_func_lead_Fn_86() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int4")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_int4() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/woutOby_7")
  @Test
  public void test_nestedAggs_woutOby_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_19")
  @Test
  public void test_aggregates_wo_OrdrBy_19() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int6")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_int6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_62")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_62() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.MISSING_DESC)
  @DrillTest("lag_func/lag_Fn_108")
  @Test
  public void test_lag_func_lag_Fn_108() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_34")
  @Test
  public void test_lag_func_lag_Fn_34() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.MISSING_DESC)
  @DrillTest("lag_func/lag_Fn_69")
  @Test
  public void test_lag_func_lag_Fn_69() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_23")
  @Test
  public void test_last_val_lastValFn_23() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg20")
  @Test
  public void test_nestedAggs_nstdagg20() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_21")
  @Test
  public void test_ntile_func_ntileFn_21() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_44")
  @Test
  public void test_aggregates_woPrtnBy_44() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_32")
  @Test
  public void test_aggregates_aggOWnFn_32() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_5")
  @Test
  public void test_aggregates_wPrtnOrdrBy_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dbl_1")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_dbl_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_51")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_51() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_10")
  @Test
  public void test_aggregates_winFnQry_10() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_56")
  @Test
  public void test_aggregates_winFnQry_56() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_51")
  @Test
  public void test_lead_func_lead_Fn_51() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lead_func/lead_Fn_97")
  @Test
  public void test_lead_func_lead_Fn_97() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_9")
  @Test
  public void test_nestedAggs_emtyOvrCls_9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bln_1")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_bln_1() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_23")
  @Test
  public void test_lag_func_lag_Fn_23() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_58")
  @Test
  public void test_lag_func_lag_Fn_58() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_23")
  @Test
  public void test_aggregates_aggOWnFn_23() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_33")
  @Test
  public void test_aggregates_woPrtnBy_33() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NOT_ENOUGH_RULES)
  @DrillTest("first_val/firstValFn_33")
  @Test
  public void test_first_val_firstValFn_33() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dbl_7")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_dbl_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_36")
  @Test
  public void test_ntile_func_ntileFn_36() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_13")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_int_13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg11")
  @Test
  public void test_nestedAggs_nstdagg11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_103")
  @Test
  public void test_lead_func_lead_Fn_103() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_2")
  @Test
  public void test_last_val_lastValFn_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_38")
  @Test
  public void test_last_val_lastValFn_38() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutPrtnBy_4")
  @Test
  public void test_nestedAggs_woutPrtnBy_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_7")
  @Test
  public void test_aggregates_aggOWnFn_7() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NPE)
  @DrillTest("aggregates/winFnQry_21")
  @Test
  public void test_aggregates_winFnQry_21() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_67")
  @Test
  public void test_aggregates_winFnQry_67() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_7")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_int_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_vchar_3")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_vchar_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_chr_2")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_chr_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_8")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_int_8() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_20")
  @Test
  public void test_lead_func_lead_Fn_20() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_66")
  @Test
  public void test_lead_func_lead_Fn_66() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_12")
  @Test
  public void test_lag_func_lag_Fn_12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_42")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_42() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int2")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_int2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/multipl_wnwds/rnkNoFrm04")
  @Test
  public void test_frameclause_multipl_wnwds_rnkNoFrm04() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_47")
  @Test
  public void test_lag_func_lag_Fn_47() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_1")
  @Test
  public void test_nestedAggs_wPbOb_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_22")
  @Test
  public void test_aggregates_woPrtnBy_22() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_22")
  @Test
  public void test_first_val_firstValFn_22() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_12")
  @Test
  public void test_aggregates_aggOWnFn_12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_6")
  @Test
  public void test_lag_func_lag_Fn_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_47")
  @Test
  public void test_ntile_func_ntileFn_47() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause08")
  @Test
  public void test_nestedAggs_frmclause08() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/mtyOvrCluse_01")
  @Test
  public void test_aggregates_mtyOvrCluse_01() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_32")
  @Test
  public void test_aggregates_winFnQry_32() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_78")
  @Test
  public void test_aggregates_winFnQry_78() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bgint_7")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_bgint_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_31")
  @Test
  public void test_lead_func_lead_Fn_31() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_77")
  @Test
  public void test_lead_func_lead_Fn_77() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_31")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_31() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_2")
  @Test
  public void test_lead_func_lead_Fn_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_11")
  @Test
  public void test_aggregates_woPrtnBy_11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_45")
  @Test
  public void test_aggregates_aggOWnFn_45() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_11")
  @Test
  public void test_first_val_firstValFn_11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_14")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_int_14() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_12")
  @Test
  public void test_ntile_func_ntileFn_12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dbl_4")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_dbl_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_58")
  @Test
  public void test_ntile_func_ntileFn_58() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_14")
  @Test
  public void test_last_val_lastValFn_14() throws Exception
  {
    windowQueryTest();

  }

  @DrillTest("ntile_func/ntileFn_30")
  @Test
  public void test_ntile_func_ntileFn_30() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dbl_5")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_dbl_5() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lag_func/lag_Fn_103")
  @Test
  public void test_lag_func_lag_Fn_103() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_19")
  @Test
  public void test_lead_func_lead_Fn_19() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_4")
  @Test
  public void test_last_val_lastValFn_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_35")
  @Test
  public void test_aggregates_testW_Nulls_35() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_2")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_int_2() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NPE)
  @DrillTest("frameclause/RBUPAUF/RBUPAUF_char_1")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_char_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_83")
  @Test
  public void test_lead_func_lead_Fn_83() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_16")
  @Test
  public void test_aggregates_wo_OrdrBy_16() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_vchar_1")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_vchar_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_75")
  @Test
  public void test_lag_func_lag_Fn_75() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_42")
  @Test
  public void test_aggregates_winFnQry_42() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_44")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_44() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_14")
  @Test
  public void test_aggregates_aggOWnFn_14() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_41")
  @Test
  public void test_ntile_func_ntileFn_41() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_3")
  @Test
  public void test_nestedAggs_wPbOb_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bln_3")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_bln_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/frmclause02")
  @Test
  public void test_nestedAggs_frmclause02() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_24")
  @Test
  public void test_aggregates_testW_Nulls_24() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dbl_4")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_dbl_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg25")
  @Test
  public void test_nestedAggs_nstdagg25() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_28")
  @Test
  public void test_first_val_firstValFn_28() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bgint_2")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_bgint_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_33")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_33() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lag_func/lag_Fn_64")
  @Test
  public void test_lag_func_lag_Fn_64() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_53")
  @Test
  public void test_aggregates_winFnQry_53() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lead_func/lead_Fn_94")
  @Test
  public void test_lead_func_lead_Fn_94() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dt_2")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_dt_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_52")
  @Test
  public void test_ntile_func_ntileFn_52() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_47")
  @Test
  public void test_aggregates_aggOWnFn_47() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdWinView01")
  @Test
  public void test_nestedAggs_nstdWinView01() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg12")
  @Test
  public void test_nestedAggs_nstdagg12() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_11")
  @Test
  public void test_aggregates_testW_Nulls_11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_104")
  @Test
  public void test_lead_func_lead_Fn_104() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_16")
  @Test
  public void test_last_val_lastValFn_16() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause15")
  @Test
  public void test_nestedAggs_frmclause15() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_17")
  @Test
  public void test_first_val_firstValFn_17() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_64")
  @Test
  public void test_aggregates_winFnQry_64() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_6")
  @Test
  public void test_aggregates_wo_OrdrBy_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_53")
  @Test
  public void test_lag_func_lag_Fn_53() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lag_func/lag_Fn_99")
  @Test
  public void test_lag_func_lag_Fn_99() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_7")
  @Test
  public void test_first_val_firstValFn_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_int8")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_int8() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_20")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_20() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_chr_5")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_chr_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_40")
  @Test
  public void test_aggregates_woPrtnBy_40() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_36")
  @Test
  public void test_aggregates_aggOWnFn_36() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_9")
  @Test
  public void test_aggregates_wPrtnOrdrBy_9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg01")
  @Test
  public void test_nestedAggs_nstdagg01() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dt_5")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_dt_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_2")
  @Test
  public void test_aggregates_aggOWnFn_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_3")
  @Test
  public void test_lag_func_lag_Fn_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_27")
  @Test
  public void test_last_val_lastValFn_27() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dt_4")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_dt_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/mtyOvrCluse_02")
  @Test
  public void test_aggregates_mtyOvrCluse_02() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lead_func/lead_Fn_70")
  @Test
  public void test_lead_func_lead_Fn_70() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_75")
  @Test
  public void test_aggregates_winFnQry_75() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.MISSING_DESC)
  @DrillTest("frameclause/multipl_wnwds/mulwind_01")
  @Test
  public void test_frameclause_multipl_wnwds_mulwind_01() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_55")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_55() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lag_func/lag_Fn_42")
  @Test
  public void test_lag_func_lag_Fn_42() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_88")
  @Test
  public void test_lag_func_lag_Fn_88() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_1")
  @Test
  public void test_lead_func_lead_Fn_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_31")
  @Test
  public void test_lag_func_lag_Fn_31() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.BIGINT_TO_DATE)
  @DrillTest("aggregates/winFnQry_40")
  @Test
  public void test_aggregates_winFnQry_40() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dbl_3")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_dbl_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_105")
  @Test
  public void test_lag_func_lag_Fn_105() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_32")
  @Test
  public void test_ntile_func_ntileFn_32() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_2")
  @Test
  public void test_aggregates_testW_Nulls_2() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NPE)
  @DrillTest("frameclause/RBUPACR/RBUPACR_chr_2")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_chr_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_1")
  @Test
  public void test_ntile_func_ntileFn_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_33")
  @Test
  public void test_aggregates_testW_Nulls_33() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_6")
  @Test
  public void test_last_val_lastValFn_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_int_4")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_int_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_9")
  @Test
  public void test_aggregates_winFnQry_9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_85")
  @Test
  public void test_lead_func_lead_Fn_85() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_46")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_46() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_18")
  @Test
  public void test_aggregates_wo_OrdrBy_18() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lag_func/lag_Fn_77")
  @Test
  public void test_lag_func_lag_Fn_77() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_5")
  @Test
  public void test_nestedAggs_wPbOb_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dbl_6")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_dbl_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_16")
  @Test
  public void test_aggregates_aggOWnFn_16() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_20")
  @Test
  public void test_lag_func_lag_Fn_20() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_51")
  @Test
  public void test_aggregates_winFnQry_51() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_43")
  @Test
  public void test_ntile_func_ntileFn_43() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_char_3")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_char_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_22")
  @Test
  public void test_aggregates_testW_Nulls_22() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bln_1")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_bln_1() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause04")
  @Test
  public void test_nestedAggs_frmclause04() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_20")
  @Test
  public void test_nestedAggs_wPbOb_20() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bgint_4")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_bgint_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_50")
  @Test
  public void test_lead_func_lead_Fn_50() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_96")
  @Test
  public void test_lead_func_lead_Fn_96() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_26")
  @Test
  public void test_first_val_firstValFn_26() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_35")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_35() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_66")
  @Test
  public void test_lag_func_lag_Fn_66() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_49")
  @Test
  public void test_aggregates_aggOWnFn_49() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_62")
  @Test
  public void test_aggregates_winFnQry_62() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_8")
  @Test
  public void test_aggregates_wo_OrdrBy_8() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_54")
  @Test
  public void test_ntile_func_ntileFn_54() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_18")
  @Test
  public void test_last_val_lastValFn_18() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_106")
  @Test
  public void test_lead_func_lead_Fn_106() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg14")
  @Test
  public void test_nestedAggs_nstdagg14() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause17")
  @Test
  public void test_nestedAggs_frmclause17() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("aggregates/aggOWnFn_4")
  @Test
  public void test_aggregates_aggOWnFn_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_15")
  @Test
  public void test_first_val_firstValFn_15() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_61")
  @Test
  public void test_lead_func_lead_Fn_61() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_9")
  @Test
  public void test_first_val_firstValFn_9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int8")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_int8() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_22")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_22() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/woutOby_9")
  @Test
  public void test_nestedAggs_woutOby_9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_55")
  @Test
  public void test_lag_func_lag_Fn_55() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_38")
  @Test
  public void test_aggregates_aggOWnFn_38() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_73")
  @Test
  public void test_aggregates_winFnQry_73() throws Exception
  {
    windowQueryTest();
  }
  @DrillTest("aggregates/woPrtnBy_42")
  @Test
  public void test_aggregates_woPrtnBy_42() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dt_3")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_dt_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_29")
  @Test
  public void test_last_val_lastValFn_29() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wPrtnOrdrBy_7")
  @Test
  public void test_aggregates_wPrtnOrdrBy_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg03")
  @Test
  public void test_nestedAggs_nstdagg03() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_1")
  @Test
  public void test_lag_func_lag_Fn_1() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_72")
  @Test
  public void test_lead_func_lead_Fn_72() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dt_2")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_dt_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_57")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_57() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/mtyOvrCluse_04")
  @Test
  public void test_aggregates_mtyOvrCluse_04() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_11")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_44")
  @Test
  public void test_lag_func_lag_Fn_44() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_dbl_1")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_dbl_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_84")
  @Test
  public void test_aggregates_winFnQry_84() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lead_func/lead_Fn_15")
  @Test
  public void test_lead_func_lead_Fn_15() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_8")
  @Test
  public void test_last_val_lastValFn_8() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_4")
  @Test
  public void test_aggregates_testW_Nulls_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_12")
  @Test
  public void test_aggregates_wo_OrdrBy_12() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.MISSING_DESC)
  @DrillTest("nestedAggs/multiWin_7")
  @Test
  public void test_nestedAggs_multiWin_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_19")
  @Test
  public void test_ntile_func_ntileFn_19() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_39")
  @Test
  public void test_aggregates_testW_Nulls_39() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_71")
  @Test
  public void test_lag_func_lag_Fn_71() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_02")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_02() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_48")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_48() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_18")
  @Test
  public void test_lag_func_lag_Fn_18() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_18")
  @Test
  public void test_aggregates_aggOWnFn_18() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_7")
  @Test
  public void test_aggregates_winFnQry_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_char_5")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_char_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_28")
  @Test
  public void test_aggregates_testW_Nulls_28() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_22")
  @Test
  public void test_nestedAggs_wPbOb_22() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.RESULT_PARSE_EXCEPTION)
  @DrillTest("lead_func/lead_Fn_90")
  @Test
  public void test_lead_func_lead_Fn_90() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_11")
  @Test
  public void test_nestedAggs_emtyOvrCls_11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_37")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_37() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_60")
  @Test
  public void test_lag_func_lag_Fn_60() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bgint_6")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_bgint_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_bln_2")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_bln_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_7")
  @Test
  public void test_nestedAggs_wPbOb_7() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_60")
  @Test
  public void test_aggregates_winFnQry_60() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_3")
  @Test
  public void test_first_val_firstValFn_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_37")
  @Test
  public void test_lead_func_lead_Fn_37() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/basic_2")
  @Test
  public void test_nestedAggs_basic_2() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause11")
  @Test
  public void test_nestedAggs_frmclause11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg16")
  @Test
  public void test_nestedAggs_nstdagg16() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_15")
  @Test
  public void test_aggregates_testW_Nulls_15() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_11")
  @Test
  public void test_nestedAggs_wPbOb_11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_2")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_int_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int11")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_int11() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dbl_4")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_dbl_4() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.RESULT_PARSE_EXCEPTION)
  @DrillTest("lag_func/lag_Fn_95")
  @Test
  public void test_lag_func_lag_Fn_95() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_29")
  @Test
  public void test_aggregates_woPrtnBy_29() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_24")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_24() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_71")
  @Test
  public void test_aggregates_winFnQry_71() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_48")
  @Test
  public void test_lead_func_lead_Fn_48() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_dt_1")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_dt_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_23")
  @Test
  public void test_aggregates_wo_OrdrBy_23() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_112")
  @Test
  public void test_lag_func_lag_Fn_112() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg05")
  @Test
  public void test_nestedAggs_nstdagg05() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_vchr_3")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_vchr_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_18")
  @Test
  public void test_aggregates_winFnQry_18() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/emtyOvrCls_1")
  @Test
  public void test_nestedAggs_emtyOvrCls_1() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bgint_2")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_bgint_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_13")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_59")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_59() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_84")
  @Test
  public void test_lag_func_lag_Fn_84() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_18")
  @Test
  public void test_aggregates_woPrtnBy_18() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_82")
  @Test
  public void test_aggregates_winFnQry_82() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.CANNOT_APPLY_VIRTUAL_COL)
  @DrillTest("nestedAggs/multiWin_5")
  @Test
  public void test_nestedAggs_multiWin_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_vchar_1")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_vchar_1() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lead_func/lead_Fn_17")
  @Test
  public void test_lead_func_lead_Fn_17() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_39")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_39() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_101")
  @Test
  public void test_lag_func_lag_Fn_101() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_37")
  @Test
  public void test_aggregates_testW_Nulls_37() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_29")
  @Test
  public void test_aggregates_winFnQry_29() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_6")
  @Test
  public void test_aggregates_testW_Nulls_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_14")
  @Test
  public void test_aggregates_wo_OrdrBy_14() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_73")
  @Test
  public void test_lag_func_lag_Fn_73() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_04")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_04() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_81")
  @Test
  public void test_lead_func_lead_Fn_81() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_5")
  @Test
  public void test_aggregates_winFnQry_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_dt_4")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_dt_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_28")
  @Test
  public void test_lead_func_lead_Fn_28() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_28")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_28() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_26")
  @Test
  public void test_aggregates_testW_Nulls_26() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_bln_2")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_bln_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_24")
  @Test
  public void test_nestedAggs_wPbOb_24() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lag_func/lag_Fn_62")
  @Test
  public void test_lag_func_lag_Fn_62() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lead_func/lead_Fn_92")
  @Test
  public void test_lead_func_lead_Fn_92() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/basic_4")
  @Test
  public void test_nestedAggs_basic_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_9")
  @Test
  public void test_nestedAggs_wPbOb_9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dt_4")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_dt_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_5")
  @Test
  public void test_first_val_firstValFn_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_50")
  @Test
  public void test_ntile_func_ntileFn_50() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_39")
  @Test
  public void test_lead_func_lead_Fn_39() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bgint_7")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_bgint_7() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.AGGREGATION_NOT_SUPPORT_TYPE)
  @DrillTest("nestedAggs/frmclause13")
  @Test
  public void test_nestedAggs_frmclause13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_13")
  @Test
  public void test_aggregates_testW_Nulls_13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg18")
  @Test
  public void test_nestedAggs_nstdagg18() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/wPbOb_13")
  @Test
  public void test_nestedAggs_wPbOb_13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_int13")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_int13() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_dbl_2")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_dbl_2() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_26")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_26() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_51")
  @Test
  public void test_lag_func_lag_Fn_51() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_19")
  @Test
  public void test_first_val_firstValFn_19() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.COLUMN_NOT_FOUND)
  @DrillTest("lag_func/lag_Fn_97")
  @Test
  public void test_lag_func_lag_Fn_97() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_vchr_5")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_vchr_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/winFnQry_16")
  @Test
  public void test_aggregates_winFnQry_16() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.MISSING_DESC)
  @DrillTest("frameclause/multipl_wnwds/mulwind_07")
  @Test
  public void test_frameclause_multipl_wnwds_mulwind_07() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/nstdagg07")
  @Test
  public void test_nestedAggs_nstdagg07() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPACR/RBUPACR_bgint_4")
  @Test
  public void test_frameclause_RBUPACR_RBUPACR_bgint_4() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/wo_OrdrBy_25")
  @Test
  public void test_aggregates_wo_OrdrBy_25() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_15")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_15() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_40")
  @Test
  public void test_lag_func_lag_Fn_40() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lag_func/lag_Fn_86")
  @Test
  public void test_lag_func_lag_Fn_86() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("first_val/firstValFn_31")
  @Test
  public void test_first_val_firstValFn_31() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("last_val/lastValFn_32")
  @Test
  public void test_last_val_lastValFn_32() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("lead_func/lead_Fn_57")
  @Test
  public void test_lead_func_lead_Fn_57() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/subQueries/frmInSubQry_18")
  @Test
  public void test_frameclause_subQueries_frmInSubQry_18() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("aggregates/winFnQry_80")
  @Test
  public void test_aggregates_winFnQry_80() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/woPrtnBy_35")
  @Test
  public void test_aggregates_woPrtnBy_35() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/defaultFrame/RBUPACR_bgint_6")
  @Test
  public void test_frameclause_defaultFrame_RBUPACR_bgint_6() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("ntile_func/ntileFn_38")
  @Test
  public void test_ntile_func_ntileFn_38() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/aggOWnFn_9")
  @Test
  public void test_aggregates_aggOWnFn_9() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/cte_win_02")
  @Test
  public void test_nestedAggs_cte_win_02() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("aggregates/testW_Nulls_8")
  @Test
  public void test_aggregates_testW_Nulls_8() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBCRACR/RBCRACR_int_5")
  @Test
  public void test_frameclause_RBCRACR_RBCRACR_int_5() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("frameclause/RBUPAUF/RBUPAUF_vchar_3")
  @Test
  public void test_frameclause_RBUPAUF_RBUPAUF_vchar_3() throws Exception
  {
    windowQueryTest();
  }

  @DrillTest("nestedAggs/woutPrtnBy_2")
  @Test
  public void test_nestedAggs_woutPrtnBy_2() throws Exception
  {
    windowQueryTest();
  }

  @DecoupledIgnore(Modes.NULLS_FIRST_LAST)
  @DrillTest("lag_func/lag_Fn_14")
  @Test
  public void test_lag_func_lag_Fn_14() throws Exception
  {
    windowQueryTest();
  }
}
