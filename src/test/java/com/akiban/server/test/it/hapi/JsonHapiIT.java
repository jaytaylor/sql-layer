/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.test.it.hapi;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.memcache.HapiProcessorFactory;
import com.akiban.server.service.memcache.ParsedHapiGetRequest;
import com.akiban.server.service.memcache.outputter.jsonoutputter.JsonOutputter;
import com.akiban.server.service.session.Session;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.OnlyIf;
import com.akiban.junit.Parameterization;
import com.akiban.server.test.it.ITBase;
import com.akiban.util.Strings;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONString;
import org.json.JSONTokener;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

import static org.junit.Assert.*;

/**
 * <p>Generalized test that's parameterized by .json files.</p>
 *
 * <p>You can write a Hapi sql-and-memcache test by creating a .json file in this directory.
 * Any <tt>*.json</tt> file is considered a test. Files whose names start with "<tt>disabled_</tt>" will be recognized
 * as tests that expect to fail. This is analogous to JUnit's <tt>@Ignore</tt>. A disabled_ file will not even be read;
 * a single test parameterization will be created (and marked as ignored by the runner) with the name
 * "<tt>file: *</tt>". For instance, <tt>disabled_foo.json</tt> will result in an ignored test named "<tt>foo: *</tt>".
 * </>
 *
 * <h2>JSON test file overview</h2>
 *
 * <p>For non-ignored tests, each file must consist of a single JSON object. This may have the following keys (and
 * no other keys):
 * <ul>
 *  <li><b>comment</b> (optional) : value is completely ignored</li>
 *  <li><b>setup</b> : defines a setup common to all tests in this file</li>
 *  <li><b>tests</b> : defines the individual tests</li>
 *  <li><b>passing_default</b> (optional boolean) : whether tests are considered passing by default (see below)</li>
 * </ul>
 *
 * <h2>Setup section</h2>
 *
 * The "setup" key defines setup. It must have the following structure:
 * <ul>
 *  <li><b>tables</b> : a json array that defines tables' DDLs.:
 *      <ul>
 *          <li>each table is defined as an array of strings</li>
 *          <li>the first element is the table's name</li>
 *          <li>subsequent elements will be concatenated to create the table's DDL
 *          <ul>
 *              <li>omit the <tt>CREATE TABLE foo(</tt> and closing parenthesis</li>
 *              <li>do not put commas at the end of each string; they will be appended automatically</li>
 *          </ul>
 *          </li>
 *      </ul>
 *  </li>
 *  <li><b>write_rows</b> : a json object that defines rows to be written before each test (see below).
 *      <ul>
 *          <li>key is table name</li>
 *          <li>value is an array of rows, where each row is an array of column values</li>
 *      </ul>
 *  </li>
 *  <li><b>schema</b> (optional string, default "test") : the schema name to be used for all tables
 * </ul>
 *
 * <p>When defining tables, the order matters: tables will be created in the order you specify. When rows are written
 * to those tables, they'll be written one table at a time, in that same order. Within each table, rows will be written
 * in the order they appear in the json.</p>
 *
 * <p>(In case you're wondering why tables are defined in somewhat awkward array-of-arrays style (and not, say, as
 * a map of tablename-to-DDLs), it's because JSON objects' keys are unordered, and we need to preserve order so that
 * tables aren't created before their parents.)</p>
 *
 * <h2>Tests section</h2>
 *
 * The "tests" key defines individual tests. It must have the following structure:
 * <ul>
 *  <li>key is test name</li>
 *  <li>value is a json object that defines the test (see "test execution" below for more on each of the following):
 *      <ul>
 *          <li><b>passing</b> (optional boolean, default true) : whether the test should be run </li>
 *          <li><b>write_row</b> (optional boolean, default true) : whether rows should be written before the test</li>
 *          <li><b>get</b> (string) : the memcache GET request string (without "<tt>GET </tt>")</li>
 *          <li><b>expect result</b> (semi-optional any value) : the json element you expect to get back</li>
 *          <li><b>expect error</b> (semi-optional string) : the error code name you expect to be thrown</li>
 *          <li><b>comment</b> (optional) : whatever you want; this key-value pair is ignored</li>
 *      </ul>
 *  </li>
 * </ul>
 *
 * The keys <tt>expect result</tt> and <tt>expect error</tt> are semi-optional in that exactly one of them must be
 * specified per test. As you may expect, the not only sets up expected-against-actual comparisons, but also dictates
 * whether the given <tt>GET</tt> request is expected to succeed.
 *
 * <h2>Test execution</h2>
 *
 * <p>The runner will create a parameterization for each of the above tests with a name "<tt>file: test</tt>".
 * For instance, if <tt>my_test.json</tt> defines a test "<tt>hello</tt>", the JUnit test name would be
 * "<tt>my_test: hello</tt>".</p>
 *
 * <p>Tests with <tt>passing=false</tt> will be registered with JUnit, but ignored (like JUnit's <tt>@Ignore</tt>).
 * For those tests, the rest of the test's specification is ignored (so you don't need the other required fields,
 * you can have extra fields, etc).</p>
 *
 * <p>Non-ignored tests must have the fields specified above, and no others. For each test, the following will happen:
 * <ol>
 *  <li>the tables will be created</li>
 *  <li>if <tt>test.write_rows</tt> is true, the rows defined in <tt>setup.write_rows</tt> will be written</li>
 *  <li>the <tt>GET</tt> will be issued to
 *      {@link com.akiban.server.service.memcache.MemcacheService#processRequest(Session, HapiGetRequest,
 *      HapiOutputter, OutputStream)}</li>
 *  <li>the result will be compared against <tt>test.expected</tt>
 * </ol>
 * </p>
 */
@RunWith(NamedParameterizedRunner.class)
public final class JsonHapiIT extends ITBase {
    private static final String SUFFIX_JSON = ".json";
    private static final String PREFIX_DISABLED = "disabled_";

    private static final String COMMENT = "comment";

    private static final String PASSING_DEFAULT = "passing_default";
    private static final String PROCESSORS_DEFAULT = "processors_default";

    private static final String SETUP = "setup";
    private static final String SETUP_SCHEMA = "schema";
    private static final String SETUP_SCHEMA_DEFAULT = "test";
    private static final String SETUP_TABLES = "tables";
    private static final String SETUP_WRITE_ROWS = "write_rows";
    private static final String[] SETUP_KEYS_REQUIRED = {SETUP_TABLES, SETUP_WRITE_ROWS};
    private static final String[] SETUP_KEYS_OPTIONAL = {SETUP_SCHEMA, PROCESSORS_DEFAULT};

    private static final String TESTS = "tests";
    private static final String TEST_WRITE_ROWS = "write_rows";
    private static final boolean TEST_WRITE_ROWS_DEFAULT = true;
    private static final String TEST_PASSING = "passing";
    private static final boolean TEST_PASSING_DEFAULT = true;
    private static final String TEST_GET = "get";
    private static final String TEST_EXPECT = "expect result";
    private static final String TEST_ERROR = "expect error";
    private static final String TEST_INDEX = "expect index";
    private static final String TEST_PROCESSORS = "processors";
    private static final String[] TEST_KEYS_REQUIRED = {TEST_GET};
    private static final String[] TEST_KEYS_OPTIONAL = {TEST_WRITE_ROWS, TEST_PASSING, TEST_EXPECT, TEST_ERROR,
                                                        COMMENT, TEST_PROCESSORS, TEST_INDEX};
    private static final String[] DEFAULT_TEST_PROCESSORS = {"SCANROWS"};

    private static final String[] SECTIONS_REQUIRED = {SETUP, TESTS};
    private static final String[] SECTIONS_OPTIONAL = {COMMENT, PASSING_DEFAULT};

    @NamedParameterizedRunner.TestParameters
    public static List<Parameterization> params() throws IOException {
        List<Parameterization> params = new ArrayList<Parameterization>();
        List<String> testFiles = Strings.dumpResource(JsonHapiIT.class, ".");
        for(String file : testFiles) {
            if (!file.endsWith(SUFFIX_JSON)) {
                continue;
            }
            try {
                List<Parameterization> tests = processFile(file);
                for (Parameterization test : tests) {
                    params.add(test);
                }
            } catch (Throwable e) {
                String name = file.substring(0, file.length() - SUFFIX_JSON.length());
                params.add( Parameterization.create(name + ": initialization error", e, null, null));
            }
        }
        return params;
    }

    private static class TableDesc {
        final String name;
        final String columns;
        
        public TableDesc(String name, String columns) {
            this.name = name;
            this.columns = columns;
        }
    }
    
    private static class IndexDesc {
        final static String INDEX_START = "index(";
        final String tableName;
        final String name;
        final String columns;
        
        public IndexDesc(String tableName, String name, String columns) {
            this.tableName = tableName;
            this.name = name;
            this.columns = columns;
        }
    }
    
    private static class TestSetupInfo {
        final List<String> defaultProcessors;
        final String schema;
        final List<TableDesc> tableDDL;
        final List<IndexDesc> indexDDL;
        final Map<String,JSONArray> writeRows;

        private TestSetupInfo(String schema, List<TableDesc> tableDDL, List<IndexDesc> indexDDL, Map<String,JSONArray> writeRows, JSONArray processors)
        throws JSONException
        {
            this.schema = schema;
            this.tableDDL = tableDDL;
            this.indexDDL = indexDDL;
            this.writeRows = writeRows;
            if (processors == null) {
                this.defaultProcessors = Collections.unmodifiableList( Arrays.asList(DEFAULT_TEST_PROCESSORS));
            }
            else {
                List<String> defaultProcessors = new ArrayList<String>();
                for(int i=0, len=processors.length(); i < len; ++i) {
                    defaultProcessors.add( processors.getString(i) );
                }
                this.defaultProcessors = Collections.unmodifiableList( defaultProcessors );
            }
        }

        @Override
        public String toString() {
            return String.format("TestSetupInfo{schema=%s, ddls=%s, writeRows=%s}", schema, tableDDL, writeRows);
        }
    }

    private static class TestRunInfo {
        final boolean writeRows;
        final String getQuery;
        final Object expect;
        final HapiRequestException.ReasonCode errorExpect;
        final String expectIndexName;
        final boolean expectIndexOnGroup;

        private TestRunInfo(boolean writeRows, String getQuery, Object expect, String errorExpect,
                            String expectIndex, boolean expectIndexOnGroup)
        {
            this.writeRows = writeRows;
            this.getQuery = getQuery;
            this.expect = expect;
            this.errorExpect = errorExpect == null ? null : HapiRequestException.ReasonCode.valueOf(errorExpect);
            this.expectIndexName = expectIndex;
            this.expectIndexOnGroup = expectIndexOnGroup;
        }

        @Override
        public String toString() {
            return String.format("TestRunInfo{write_rows=%s, get=%s, expect=%s}", writeRows, getQuery, expect);
        }
    }

    private static class TestExtraParam {
        final String hapiProcessor;

        private TestExtraParam(String hapiProcessor) {
            this.hapiProcessor = hapiProcessor.toUpperCase();
        }

        String getName(String previousName) {
            return String.format("(%s) %s", hapiProcessor, previousName);
        }

        @Override
        public String toString() {
            return String.format("TestExtraParam{hapiProcessor=%s}", hapiProcessor);
        }
    }

    private static void validateKeys(String description, JSONObject json, String[] required, String[] optional) {
        Set<String> keys = new HashSet<String>(Arrays.asList(JSONObject.getNames(json)));
        keys.removeAll(Arrays.asList(optional));
        Set<String> requiredSet = new HashSet<String>(Arrays.asList(required));
        if (!requiredSet.equals(keys)) {
            Set<String> optionalSet = new TreeSet<String>(Arrays.asList(optional));
            throw new RuntimeException(String.format("%s: required keys %s (optional %s) but found %s",
                    description, requiredSet, optionalSet, keys
            ));
        }
    }

    private static List<Parameterization> processFile(String file) throws IOException, JSONException {
        final String name = file.substring(0, file.length() - SUFFIX_JSON.length());
        final List<Parameterization> params = new ArrayList<Parameterization>();

        if(file.startsWith(PREFIX_DISABLED)) {
            String actualName = name.substring(PREFIX_DISABLED.length());
            Parameterization disableAll = Parameterization.failing(paramName(actualName, "*"), null, null);
            params.add(disableAll);
            return params;
        }

        final JSONObject testPlan = new JSONObject(Strings.join(Strings.dumpResource(JsonHapiIT.class, file)));
        validateKeys("test plan", testPlan, SECTIONS_REQUIRED, SECTIONS_OPTIONAL);

        final boolean passingDefault = testPlan.optBoolean(PASSING_DEFAULT, TEST_PASSING_DEFAULT);
        final TestSetupInfo setupInfo = extractTestSetupInfo(testPlan.getJSONObject(SETUP));

        final JSONObject tests = testPlan.getJSONObject(TESTS);
        String[] testNames = JSONObject.getNames(tests);
        if (testNames == null) {
            throw new RuntimeException("no tests defined");
        }
        for(String testName : testNames) {
            try {
                JSONObject test = tests.getJSONObject(testName);
                final boolean passing = test.optBoolean(TEST_PASSING, passingDefault);
                final Parameterization param;
                if (passing) {
                    validateKeys("test \"" + testName +'"', test, TEST_KEYS_REQUIRED, TEST_KEYS_OPTIONAL);
                    TestRunInfo runInfo = null;
                    Exception exception = null;
                    try {
                        runInfo = extractTestRunInfo(testName, test);
                    } catch (Exception e) {
                            exception = e;
                    }
                    param = Parameterization.create(paramName(name, testName), exception, setupInfo, runInfo);
                }
                else {
                    param = Parameterization.failing(paramName(name, testName), null, null, null);
                }
                List<String> processors = extractProcessors(test, setupInfo);
                addParams(params, param, processors);
            } catch (JSONException e) {
                params.add( Parameterization.create(paramName(name, testName), e, null, null) );
            }
        }
        return params;
    }

    private static void addParams(List<Parameterization> params, Parameterization basic, List<String> processors) {
        for (String hapiProcessor : processors) {
            List<Object> paramArgs = new ArrayList<Object>(basic.getArgsAsList());
            TestExtraParam extraParam = new TestExtraParam(hapiProcessor);
            paramArgs.add(extraParam);
            Parameterization param = new Parameterization(
                    extraParam.getName(basic.getName()),
                    basic.expectedToPass()
            );
            param.getArgsAsList().addAll(paramArgs);
            params.add(param);
        }
    }

    private static List<String> extractProcessors(JSONObject test, TestSetupInfo setupInfo) throws JSONException {
        JSONArray array = test.optJSONArray(TEST_PROCESSORS);
        if (array == null) {
            return setupInfo.defaultProcessors;
        }
        List<String> ret = new ArrayList<String>();
        for(int i=0, len=array.length(); i < len; ++i) {
            ret.add(array.getString(i));
        }
        return ret;
    }

    private static TestRunInfo extractTestRunInfo(String testName, JSONObject test) throws JSONException{
        final boolean writeRows = test.optBoolean(TEST_WRITE_ROWS, TEST_WRITE_ROWS_DEFAULT);
        final String get = test.getString(TEST_GET);
        final Object expect = test.opt(TEST_EXPECT);
        final String error = test.optString(TEST_ERROR, null);
        if ( (expect == null) == (error == null) ) {
            throw new RuntimeException(String.format("test '%s': you must set one (and only one) of '%s' or '%s'",
                    testName, TEST_EXPECT, TEST_ERROR
            ));
        }
        final String expectIndex;
        final boolean expectIndexOnGroup;
        Object expectIndexObj = test.opt(TEST_INDEX);
        if (expectIndexObj == null) {
            expectIndex = null;
            expectIndexOnGroup = false;
        }
        else if (expectIndexObj instanceof String) {
            expectIndex = (String)expectIndexObj;
            expectIndexOnGroup = false;
        }
        else if (expectIndexObj instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject)expectIndexObj;
            if (jsonObject.length() != 1) {
                throw new JSONException(TEST_INDEX + " json object may only have one key, 'group' or 'utable'");
            }
            String key = (String)jsonObject.keys().next();
            if (key.equalsIgnoreCase("group")) {
                expectIndex = jsonObject.getString(key);
                expectIndexOnGroup = true;
            }
            else if (key.equalsIgnoreCase("utable")) {
                expectIndex = jsonObject.getString(key);
                expectIndexOnGroup = false;
            }
            else {
                throw new JSONException(TEST_INDEX + " json object may only have one key, 'group' or 'utable'");
            }

        }
        else {
            throw new JSONException(TEST_INDEX + " must be a String or JSON object");
        }

        return new TestRunInfo(writeRows, get, expect, error, expectIndex, expectIndexOnGroup);
    }

    private static TestSetupInfo extractTestSetupInfo(JSONObject setupJSON) throws JSONException {
        validateKeys("setup", setupJSON, SETUP_KEYS_REQUIRED, SETUP_KEYS_OPTIONAL);
        final String schema;
        final Map<String,JSONArray> writeRows;
        final List<TableDesc> tableDDL = new ArrayList<TableDesc>();
        final List<IndexDesc> indexDDL = new ArrayList<IndexDesc>();

        schema = setupJSON.optString(SETUP_SCHEMA, SETUP_SCHEMA_DEFAULT);

        final JSONArray ddlArrays = setupJSON.getJSONArray(SETUP_TABLES);

        LinkedHashSet<String> tableNames = new LinkedHashSet<String>();
        List<String> ddlComponents = new ArrayList<String>();
        for(int i=0, MAX = ddlArrays.length(); i < MAX; ++i) {
            JSONArray tableDefinition = ddlArrays.getJSONArray(i);
            String tableName = tableDefinition.getString(0);
            tableNames.add(tableName);
            if (tableDefinition.length() < 2) {
                throw new RuntimeException("table " + tableName + " has no DDLs");
            }
            ddlComponents.clear();
            for(int j=1, MAX2 = tableDefinition.length(); j < MAX2; ++j) {
                String def = tableDefinition.getString(j);
                if(def.startsWith(IndexDesc.INDEX_START)) {
                    String indexColumns = def.substring(IndexDesc.INDEX_START.length(), def.length() - 1);
                    String indexName = indexColumns.replace(",", "_");
                    indexDDL.add(new IndexDesc(tableName, indexName, indexColumns));
                } else {
                    ddlComponents.add(tableDefinition.getString(j));
                }
            }
            tableDDL.add(new TableDesc(tableName, Strings.join(ddlComponents, ", ")));
        }

        final JSONObject writeRowsJSON = setupJSON.optJSONObject(SETUP_WRITE_ROWS);
        if (writeRowsJSON == null) {
            writeRows = Collections.emptyMap();
        }
        else {
            Map<String,JSONArray> writeRowsTmp = new LinkedHashMap<String,JSONArray>();
            Set<String> writeRowTables = new TreeSet<String>(Arrays.asList(JSONObject.getNames(writeRowsJSON)));
            if (!tableNames.equals(writeRowTables)) {
                throw new RuntimeException(String.format("write_row tables expected %s but was %s",
                        tableNames, writeRowTables
                ));
            }
            // We iterate over tableNames rather than the writeRowsJSON names so that we can ensure
            // correct ordering of child rows after parent rows.
            for (String tableName : tableNames) {
                // rows is an array of arrays, each sub-array being a row's columns
                JSONArray rows = writeRowsJSON.getJSONArray(tableName);
                writeRowsTmp.put(tableName, rows);
            }
            writeRows = Collections.unmodifiableMap(writeRowsTmp);
        }
        return new TestSetupInfo(
                schema,
                Collections.unmodifiableList(tableDDL),
                Collections.unmodifiableList(indexDDL),
                writeRows,
                setupJSON.optJSONArray(PROCESSORS_DEFAULT)
        );
    }

    private static String paramName(String file, String test) {
        return String.format("%s: %s", file, test);
    }

    private final TestSetupInfo setupInfo;
    private final TestRunInfo runInfo;
    private final HapiProcessorFactory hapiProcessor;

    public JsonHapiIT(Exception testSetupErr, TestSetupInfo setupInfo, TestRunInfo runInfo, TestExtraParam extraParams)
            throws Exception
    {
        if (testSetupErr != null) {
            throw testSetupErr;
        }
        this.setupInfo = setupInfo;
        this.runInfo = runInfo;
        this.hapiProcessor = HapiProcessorFactory.valueOf(extraParams.hapiProcessor);
    }

    @Before
    public void setUp() throws InvalidOperationException, JSONException {
        for(TableDesc desc : setupInfo.tableDDL) {
            createTable(setupInfo.schema, desc.name, desc.columns);
        }
        for(IndexDesc desc : setupInfo.indexDDL) {
            createIndex(setupInfo.schema, desc.tableName, desc.name, desc.columns);
        }
        if (runInfo.writeRows) {
            for(Map.Entry<String,JSONArray> entry : setupInfo.writeRows.entrySet()) {
                int tableId = tableId(setupInfo.schema, entry.getKey());
                JSONArray rows = entry.getValue();
                for(int rowNum=0, ROWS=rows.length(); rowNum < ROWS; ++rowNum) {
                    JSONArray columns = rows.getJSONArray(rowNum);
                    NewRow row = new NiceRow(tableId, store());
                    for(int col=0, COLS=columns.length(); col < COLS; ++col) {
                        Object value = columns.get(col);
                        if (JSONObject.NULL.equals(value)) {
                            value = null;
                        }
                        row.put(col, value);
                    }
                        dml().writeRow(session(), row);
                }
            }
        }
    }

    @Test @OnlyIf("shouldCheckIndex()")
    public void correctIndex() throws HapiRequestException {
        HapiGetRequest request = ParsedHapiGetRequest.parse(runInfo.getQuery);
        Index expectedIndex;
        if (runInfo.expectIndexOnGroup) {
            Table gTable = ddl().getAIS(session()).getUserTable(request.getUsingTable()).getGroup().getGroupTable();
            expectedIndex = gTable.getIndex(runInfo.expectIndexName);
        } else {
            expectedIndex = ddl().getAIS(session()).getTable(request.getUsingTable()).getIndex(runInfo.expectIndexName);
        }
        assertNotNull(
                String.format("no index %s on %s", runInfo.expectIndexName, request.getUsingTable()),
                expectedIndex);
        Index actualIndex = hapi(hapiProcessor).findHapiRequestIndex(session(), request);
        assertNotNull("HapiProcessor couldn't resolve index", actualIndex);
        assertSame("index", expectedIndex, actualIndex);
        assertEquals("index (expected " + runInfo.expectIndexName + ')',
                expectedIndex.getIndexId(),
                actualIndex.getIndexId());
    }

    public boolean shouldCheckIndex() {
        return runInfo.expectIndexName != null;
    }

    @Test
    public void get() throws JSONException, HapiRequestException, IOException {
        try {
            HapiGetRequest request = ParsedHapiGetRequest.parse(runInfo.getQuery);
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream(1024);
            hapi(hapiProcessor).processRequest(session(), request, JsonOutputter.instance(), outputStream);
            outputStream.flush();
            String result = new String(outputStream.toByteArray());
            assertNull("got result but expected error " + runInfo.errorExpect + ": " + result, runInfo.errorExpect);
            assertNotNull("null result", result);
            assertTrue("empty result: >" + result + "< ", result.trim().length() > 1);
            final Object actual;
            try {
                actual = new JSONTokener(result).nextValue();
            } catch (JSONException e) {
                throw new RuntimeException(result, e);
            }
            assertEquals("GET response", jsonString(runInfo.expect), jsonString(actual));
        } catch (HapiRequestException e) {
            if(runInfo.expect != null) {
                throw e;
            }

            if(!runInfo.errorExpect.equals(e.getReasonCode())) {
                String message = String.format("Error reason code expected <%s> but was <%s>",
                        runInfo.errorExpect, e.getReasonCode()
                );
                System.err.println(message);
                e.printStackTrace();
                fail(message);
            }
        }


    }

    private static String jsonString(Object object) throws JSONException {
        if (object == null) {
            return null;
        }
        if (object instanceof JSONObject) {
            return ((JSONObject)object).toString(4);
        }
        if (object instanceof JSONArray) {
            return ((JSONArray)object).toString(4);
        }
        if (object instanceof JSONString) {
            return ((JSONString)object).toJSONString();
        }
        return object.toString();
    }
}
