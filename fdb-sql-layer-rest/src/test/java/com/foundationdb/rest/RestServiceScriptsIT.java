/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.rest;

import static com.foundationdb.util.JsonUtils.readTree;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.ComparisonFailure;

import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.HttpExchange;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.foundationdb.http.HttpConductor;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.server.service.is.BasicInfoSchemaTablesService;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.sql.RegexFilenameFilter;
import com.foundationdb.util.Strings;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Scripted tests for REST end-points. Code was largely copied from
 * RestServiceFilesIT. Difference is that this version finds files with the
 * suffix ".script" and executes the command stream located in them. Commands
 * are:
 * 
 * <pre>
 * GET address
 * DELETE address
 * QUERY query
 * EXPLAIN query
 * POST address content
 * PUT address content
 * PATCH address content
 * EQUALS expected
 * CONTAINS expected
 * JSONEQ expected
 * HEADERS expected
 * EMPTY
 * NOTEMPTY
 * SHOW
 * DEBUG
 * </pre>
 * 
 * where address is a path relative the resource end-point, content is a string
 * value that is converted to bytes and sent with POST, PUT and PATCH
 * operations, and expected is a value used in comparison with the most recently
 * returned content. The values of the query, content and expected fields may be
 * specified in-line, or as a reference to another file as in @filename. For
 * in-line values, the character sequences "\n", "\t" and "\r" are converted to
 * the corresponding new-line, tab and return characters. This transformation is
 * not done if the value is supplied as a file reference. An empty string can be
 * specified as simply @, e.g.:
 * 
 * <pre>
 * POST    /builder/implode/test.customers @
 * </pre>
 * 
 * The SHOW and DEBUG commands are useful for debugging. SHOW simply prints out
 * the actual content of the last REST response. The DEBUG command calls the
 * static method {@link #debug(int)}. You can set a debugger breakpoint inside
 * that method.
 * 
 * @author peter
 */
@Ignore                         // Presently no scripts.
// Since there are no scripts and this test is ignored, when you re-enable it you may run into csrf protection issues
// take a look at RestServiceFilesIT for an idea for how to fix them
@RunWith(NamedParameterizedRunner.class)
public class RestServiceScriptsIT extends ITBase {

    private static void debug(int lineNumber) {
        // Set a breakpoint here to debug on DEBUG statements
        System.out.println("DEBUG executed on line " + lineNumber);
    }

    private static final Logger LOG = LoggerFactory.getLogger(RestServiceScriptsIT.class.getName());

    private static final File RESOURCE_DIR = new File("src/test/resources/"
            + RestServiceScriptsIT.class.getPackage().getName().replace('.', '/'));

    public static final String SCHEMA_NAME = "test";

    private static class CaseParams {
        public final String subDir;
        public final String caseName;
        public final String script;

        private CaseParams(String subDir, String caseName, String script) {
            this.subDir = subDir;
            this.caseName = caseName;
            this.script = script;
        }
    }

    static class Result {
        HttpExchange conn;
        String output = "<not executed>";
    }

    protected final CaseParams caseParams;
    protected final HttpClient httpClient;
    private final List<String> errors = new ArrayList<>();
    private final Result result = new Result();
    private int lineNumber = 0;

    public RestServiceScriptsIT(CaseParams caseParams) throws Exception {
        this.caseParams = caseParams;
        this.httpClient = new HttpClient();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        httpClient.setMaxConnectionsPerAddress(10);
        httpClient.start();
    }

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
            .require(RestService.class)
            .require(BasicInfoSchemaTablesService.class);
    }

    @Override
    protected Map<String,String> startupConfigProperties() {
        Map<String,String> config = new HashMap<>(super.startupConfigProperties());
        config.put("fdbsql.rest.resource", "entity,fulltext,model,procedurecall,sql,security,version,direct,view");
        config.put("fdbsql.http.csrf_protection.allowed_referers", "https://somewhere.com");
        return config;
    }

    public static File[] gatherRequestFiles(File dir) {
        File[] result = dir.listFiles(new RegexFilenameFilter(".*\\.(script)"));
        Arrays.sort(result, new Comparator<File>() {
            public int compare(File f1, File f2) {
                return f1.getName().compareTo(f2.getName());
            }
        });
        return result;
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> gatherCases() throws Exception {
        Collection<Parameterization> result = new ArrayList<>();
        for (String subDirName : RESOURCE_DIR.list()) {
            File subDir = new File(RESOURCE_DIR, subDirName);
            if (!subDir.isDirectory()) {
                LOG.warn("Skipping unexpected file: {}", subDir);
                continue;
            }
            for (File requestFile : gatherRequestFiles(subDir)) {
                String inputName = requestFile.getName();
                int dotIndex = inputName.lastIndexOf('.');
                String caseName = inputName.substring(0, dotIndex);
                String script = Strings.dumpFileToString(requestFile);

                result.add(Parameterization.create(subDirName + File.separator + caseName, new CaseParams(subDirName,
                        caseName, script)));
            }
        }
        return result;
    }

    private URL getRestURL(String request) throws MalformedURLException {
        int port = serviceManager().getServiceByClass(HttpConductor.class).getPort();
        String context = serviceManager().getServiceByClass(RestService.class).getContextPath();
        return new URL("http", "localhost", port, context + request);
    }

    private void loadDatabase(String subDirName) throws Exception {
        File subDir = new File(RESOURCE_DIR, subDirName);
        File schemaFile = new File(subDir, "schema.ddl");
        if (schemaFile.exists()) {
            loadSchemaFile(SCHEMA_NAME, schemaFile);
        }
        for (File data : subDir.listFiles(new RegexFilenameFilter(".*\\.dat"))) {
            loadDataFile(SCHEMA_NAME, data);
        }
    }

    private static void postContents(HttpExchange httpConn, byte[] request) throws IOException {
        httpConn.setRequestContentType("application/json");
        httpConn.setRequestHeader("Accept", "application/json");
        httpConn.setRequestHeader("Referer", "https://somewhere.com");
        httpConn.setRequestContentSource(new ByteArrayInputStream(request));
    }

    @After
    public void finish() throws Exception {
        httpClient.stop();
    }

    private void error(String message) {
        error(message, result.output);
    }

    private void error(String message, String s) {
        String error = String.format("%s in %s:%d <%s>", message, caseParams.caseName, lineNumber, s);
        errors.add(error);
    }

    @Test
    public void testRequest() throws Exception {
        loadDatabase(caseParams.subDir);

        // Execute lines of script

        result.conn = null;
        result.output = "<not executed>";
        lineNumber = 0;

        try {
            for (String line : caseParams.script.split("\n")) {
                lineNumber++;
                line = line.trim();

                while (line.contains("  ")) {
                    line = line.replace("  ", " ");
                }
                if (line.startsWith("#") || line.isEmpty()) {
                    continue;
                }
                String[] pieces = line.split(" ");
                String command = pieces[0].toUpperCase();

                switch (command) {
                case "DEBUG":
                    debug(lineNumber);
                    break;
                case "GET":
                case "DELETE": {
                    result.conn = null;
                    if (pieces.length < 2) {
                        error("Missing argument");
                        continue;
                    }
                    executeRestCall(command, pieces[1], null);
                    break;
                }
                case "QUERY":
                    result.conn = null;
                    if (pieces.length < 2) {
                        error("Missing argument");
                        continue;
                    }
                    executeRestCall("GET", "/sql/query?q=" + trimAndURLEncode(value(line, 1)), null);
                    break;
                case "EXPLAIN":
                    result.conn = null;
                    if (pieces.length < 2) {
                        error("Missing argument");
                        continue;
                    }
                    executeRestCall("GET", "/sql/explain?q=" + trimAndURLEncode(value(line, 1)), null);
                    break;
                case "POST":
                case "PUT":
                case "PATCH": {
                    result.conn = null;
                    pieces = line.split(" ", 3);
                    if (pieces.length < 3) {
                        error("Missing argument");
                        continue;
                    }
                    String contents = value(line, 2);
                    executeRestCall(command, pieces[1], contents);
                    break;
                }
                case "EQUALS":
                    if (pieces.length < 2) {
                        error("Missing argument");
                        continue;
                    }
                    compareStrings("Incorrect response", value(line, 1), result.output);
                    break;
                case "CONTAINS":
                    if (pieces.length < 2) {
                        error("Missing argument");
                        continue;
                    }
                    if (!result.output.contains(value(line, 1))) {
                        LOG.error("Incorrect value - actual returned value is:\n{}", result.output);
                        error("Incorrect response");
                    }
                    break;
                case "JSONEQ":
                    if (pieces.length < 2) {
                        error("Missing argument");
                        continue;
                    }
                    compareAsJSON("Unexpected response", value(line, 1), result.output);
                    break;
                case "HEADERS":
                    if (pieces.length < 2) {
                        error("Missing argument");
                        continue;
                    }
                    compareHeaders(result.conn, value(line, 1));
                    break;
                case "NOTEMPTY":
                    if (result.output.isEmpty() || result.conn == null) {
                        error("Expected non-empty response");
                        continue;
                    }
                    break;
                case "EMPTY":
                    if (!result.output.isEmpty()) {
                        error("Expected empty response");
                    }
                    break;
                case "SHOW":
                    int status = result.conn == null ? -1 : ((ContentExchange)result.conn).getResponseStatus(); 
                    System.out.printf("At line %d the most recent response status is %d. " + "The value is:\n%s\n",
                            lineNumber, status, result.output);
                    break;
                default:
                    result.conn = null;
                    error("Unknown script command '" + command + "'");
                }
            }
        } finally {
            result.conn = null;
        }
        if (!errors.isEmpty()) {
            String failMessage = "Failed with " + errors.size() + " errors:";
            for (String s : errors) {
                failMessage += "\n  " + s;
            }
            fail(failMessage);
        }
    }

    private void executeRestCall(final String command, final String address, final String contents) throws Exception {
        String[] pieces = address.split("\\|");
        try {
            result.conn = openConnection(pieces[0], command);
            if (contents != null) {
                postContents(result.conn, contents.getBytes());
            }
            // After postContents to override default
            if (pieces.length > 1) {
                result.conn.setRequestContentType(pieces[1]);
            }
            httpClient.send(result.conn);
            result.conn.waitForDone();
            result.output = getOutput(result.conn);
        } catch (Exception e) {
            result.output = e.toString();
            fullyDisconnect(result.conn);
        }
    }

    private HttpExchange openConnection(String address, String requestMethod) throws IOException, URISyntaxException {
        URL url = getRestURL(address);
        HttpExchange exchange = new ContentExchange(true);
        exchange.setURI(url.toURI());
        exchange.setMethod(requestMethod);
        return exchange;
    }

    private String getOutput(HttpExchange httpConn) throws IOException {
        return ((ContentExchange) httpConn).getResponseContent();
    }

    private String value(String line, int index) throws IOException {
        String s = line.split(" ", index + 1)[index];
        if (s.startsWith("@")) {
            if (s.length() == 1) {
                s = "";
            } else {
                s = Strings.dumpFileToString(new File(new File(RESOURCE_DIR, caseParams.subDir), s.substring(1)));
            }
        } else {
            s = s.replace("\\n", "\n").replace("\\n", "\t");
        }
        return s;
    }

    private static String trimAndURLEncode(String s) throws UnsupportedEncodingException {
        return URLEncoder.encode(s.trim().replaceAll("\\s+", " "), "UTF-8");
    }

    private String diff(String a, String b) {
        return new ComparisonFailure("", a, b).getMessage();
    }

    private void compareStrings(String assertMsg, String expected, String actual) {
        if (!expected.equals(actual)) {
            LOG.error("Incorrect value - actual returned value is:\n{}", actual);
            error(assertMsg, diff(expected, actual));
        }
    }

    private void compareAsJSON(String assertMsg, String expected, String actual) throws IOException {
        JsonNode expectedNode = null;
        JsonNode actualNode = null;
        String expectedTrimmed = (expected != null) ? expected.trim() : "";
        String actualTrimmed = (actual != null) ? actual.trim() : "";
        try {
            if (!expectedTrimmed.isEmpty()) {
                expectedNode = readTree(expected);
            }
            if (!actualTrimmed.isEmpty()) {
                actualNode = readTree(actual);
            }
        } catch (JsonParseException e) {
            // Note: This case handles the jsonp tests. Somewhat fragile, but
            // not horrible yet.
        }
        // Try manual equals and then assert strings for pretty print
        if (expectedNode != null && actualNode != null) {
            if (!expectedNode.equals(actualNode)) {
                error(assertMsg, diff(expectedNode.toString(), actualNode.toString()));
            }
        } else {
            compareStrings(assertMsg, expected, actual);
        }
    }

    private void compareHeaders(HttpExchange httpConn, String checkHeaders) throws Exception {
        ContentExchange exch = (ContentExchange) httpConn;

        String[] headerList = checkHeaders.split(Strings.NL);
        for (String header : headerList) {
            String[] nameValue = header.split(":", 2);

            if (nameValue[0].equals("responseCode")) {
                if (Integer.parseInt(nameValue[1].trim()) != exch.getResponseStatus()) {
                    error("Incorrect Response Status",
                            String.format("%d expected %s", exch.getResponseStatus(), nameValue[1]));
                }
            } else {
                if (!nameValue[1].trim().equals(exch.getResponseFields().getStringField(nameValue[0]))) {
                    error("Incorrect Response Header", String.format("%s expected %s", exch.getResponseFields()
                            .getStringField(nameValue[0]), nameValue[1].trim()));
                }
            }
        }
    }

    private void fullyDisconnect(HttpExchange httpConn) throws InterruptedException {
        // If there is a failure, leaving junk in any of the streams can cause
        // cascading issues.
        // Get rid of anything left and disconnect.
        httpConn.waitForDone();
        httpConn.reset();
    }
}
