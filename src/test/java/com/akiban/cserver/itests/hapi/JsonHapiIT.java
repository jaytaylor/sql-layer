package com.akiban.cserver.itests.hapi;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.api.dml.scan.NewRow;
import com.akiban.cserver.api.dml.scan.NiceRow;
import com.akiban.cserver.itests.ApiTestBase;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.util.Strings;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONString;
import org.json.JSONTokener;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
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
 * </ul>
 *
 * <h2>Setup section</h2>
 *
 * The "setup" key defines setup. It must have the following structure:
 * <ul>
 *  <li><b>tables</b> : a json object that defines tables' DDLs:
 *      <ul>
 *          <li>key is table's name</li>
 *          <li>value is an array of strings to be concatenated to create that table
 *          <ul>
 *              <li>ommit the <tt>CREATE TABLE foo(</tt> and closing parenthesis</li>
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
 * <h2>Tests section</h2>
 *
 * The "tests" key defines individual tests. It must have the following structure:
 * <ul>
 *  <li>key is test name</li>
 *  <li>value is a json object that defines the test (see "test execution" below for more on each of the following):
 *      <ul>
 *          <li><b>passing</b> (optional boolean, default true) : whether the test should be run </li>
 *          <li><b>write_row</b> (boolean) : whether rows should be written before the test</li>
 *          <li><b>get</b> (string) : the memcache GET request string (without "<tt>GET </tt>")</li>
 *          <li><b>expect</b> (any value) : the json element you expect to get back
 *      </ul>
 *  </li>
 * </ul>
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
 *      {@link com.akiban.cserver.service.memcache.MemcacheService#processRequest(
 *      com.akiban.cserver.service.session.Session, String)}</li>
 *  <li>the result will be compared against <tt>test.expected</tt>
 * </ol>
 * </p>
 */
@RunWith(NamedParameterizedRunner.class)
public final class JsonHapiIT extends ApiTestBase {
    private static final String SUFFIX_JSON = ".json";
    private static final String PREFIX_DISABLED = "disabled_";

    private static final String SETUP = "setup";
    private static final String SETUP_SCHEMA = "schema";
    private static final String SETUP_SCHEMA_DEFAULT = "test";
    private static final String SETUP_TABLES = "tables";
    private static final String SETUP_WRITE_ROWS = "write_rows";

    private static final String TESTS = "tests";
    private static final String TEST_WRITE_ROWS = "write_rows";
    private static final String TEST_PASSING = "passing";
    private static final boolean TEST_PASSING_DEFAULT = true;
    private static final String TEST_GET = "get";
    private static final String TEST_EXPECT = "expect";

    private static final Set<String> TEST_PLAN_SECTIONS = Collections.unmodifiableSet(
            new HashSet<String>(Arrays.asList(SETUP, TESTS))
    );
    private static final Set<String> TEST_PLAN_OPTIONAL_SECTIONS = Collections.unmodifiableSet(
            new HashSet<String>(Arrays.asList("comment"))
    );

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
                params.add( Parameterization.create(file + " initialization error", e, null, null));
            }
        }
        return params;
    }

    private static class TestSetupInfo {
        final String schema;
        final List<String> ddls;
        final List<NewRow> writeRows;

        private TestSetupInfo(String schema, List<String> ddls, List<NewRow> writeRows) {
            this.schema = schema;
            this.ddls = ddls;
            this.writeRows = writeRows;
        }

        @Override
        public String toString() {
            return String.format("TestSetupInfo{schema=%s, ddls=%s, writeRows=%s", schema, ddls, writeRows);
        }
    }

    private static class TestRunInfo {
        final boolean writeRows;
        final String getQuery;
        final Object expect;

        private TestRunInfo(boolean writeRows, String getQuery, Object expect) {
            this.writeRows = writeRows;
            this.getQuery = getQuery;
            this.expect = expect;
        }

        @Override
        public String toString() {
            return String.format("write_rows=%s, get=%s, expect=%s", writeRows, getQuery, expect);
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
        Set<String> sections = new HashSet<String>(Arrays.asList(JSONObject.getNames(testPlan)));
        sections.removeAll(TEST_PLAN_OPTIONAL_SECTIONS); // including an optional section shouldn't fail the assert
        assertEquals("sections", TEST_PLAN_SECTIONS, sections);

        final TestSetupInfo setupInfo = extractTestSetupInfo(testPlan.getJSONObject(SETUP));

        final JSONObject tests = testPlan.getJSONObject(TESTS);
        for(String testName : JSONObject.getNames(tests)) {
            try {
                TestRunInfo runInfo = null;
                JSONObject test = tests.getJSONObject(testName);
                final boolean passing = test.optBoolean(TEST_PASSING, TEST_PASSING_DEFAULT);
                JSONException exception = null;
                try {
                    runInfo = extractTestRunInfo(test);
                } catch (JSONException e) {
                    if (passing) {
                        exception = e;
                    }
                }
                params.add(
                        new Parameterization(paramName(name, testName), passing, exception, setupInfo, runInfo)
                );
            } catch (JSONException e) {
                params.add( Parameterization.create(paramName(name, testName), e, null, null) );
            }
        }
        return params;
    }

    private static TestRunInfo extractTestRunInfo(JSONObject test) throws JSONException{
        final boolean writeRows = test.getBoolean(TEST_WRITE_ROWS);
        final String get = test.getString(TEST_GET);
        final Object expect = test.getJSONObject(TEST_EXPECT);
        return new TestRunInfo(writeRows, get, expect);
    }

    private static TestSetupInfo extractTestSetupInfo(JSONObject testPlan) throws JSONException {
        final String schema;
        final List<NewRow> writeRows;
        final List<String> ddls = new ArrayList<String>();

        schema = testPlan.optString(SETUP_SCHEMA, SETUP_SCHEMA_DEFAULT);

        JSONObject tableDDLsJSON = testPlan.getJSONObject(SETUP_TABLES);
        for(String tableName : JSONObject.getNames(tableDDLsJSON)) {
            List<String> ddlPortion = new ArrayList<String>();
            JSONArray ddlArray = tableDDLsJSON.getJSONArray(tableName);
            for(int i=0, MAX=ddlArray.length(); i < MAX; ++i) {
                ddlPortion.add(ddlArray.getString(i));
            }
            String definition = Strings.join(ddlPortion, ", ");
            ddls.add(String.format("CREATE TABLE %s (%s)", tableName, definition));
        }

        final JSONObject writeRowsJSON = testPlan.optJSONObject(SETUP_WRITE_ROWS);
        if (writeRowsJSON == null) {
            writeRows = Collections.emptyList();
        }
        else {
            List<NewRow> writeRowsTmp = new ArrayList<NewRow>();
            for (String tableName : JSONObject.getNames(writeRowsJSON)) {
                // rows is an array of arrays, each sub-array being a row's columns
                JSONArray rows = writeRowsJSON.getJSONArray(tableName);
                final TableId tableId = TableId.of(schema, tableName);
                for(int rowNum=0, ROWS=rows.length(); rowNum < ROWS; ++rowNum) {
                    JSONArray columns = rows.getJSONArray(rowNum);
                    NewRow row = new NiceRow(tableId);
                    for(int col=0, COLS=columns.length(); col < COLS; ++col) {
                        row.put(ColumnId.of(col), columns.get(col));
                    }
                    writeRowsTmp.add(row);
                }
            }
            writeRows = Collections.unmodifiableList(writeRowsTmp);
        }
        return new TestSetupInfo(
                schema,
                Collections.unmodifiableList(ddls),
                Collections.unmodifiableList(writeRows)
        );
    }

    private static String paramName(String file, String test) {
        return String.format("%s: %s", file, test);
    }

    private final TestSetupInfo setupInfo;
    private final TestRunInfo runInfo;


    public JsonHapiIT(Throwable testSetupErr, TestSetupInfo setupInfo, TestRunInfo runInfo)
    {
        if (testSetupErr != null) {
            throw new RuntimeException("During setup", testSetupErr);
        }
        this.setupInfo = setupInfo;
        this.runInfo = runInfo;
    }

    @Before
    public void setUp() throws InvalidOperationException {
        for(String ddl : setupInfo.ddls) {
            ddl().createTable(session, setupInfo.schema, ddl);
        }
        if (runInfo.writeRows) {
            for (NewRow row : setupInfo.writeRows) {
                dml().writeRow(session, row);
            }
        }
    }

    @Test
    public void doGet() throws JSONException {
        String result = hapi().processRequest(session, runInfo.getQuery);
        assertNotNull("null result", result);
        assertTrue("empty result: >" + result + "< ", result.trim().length() > 1);

        final Object actual = new JSONTokener(result).nextValue();

        assertEquals("GET response", jsonString(runInfo.expect), jsonString(actual));
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
