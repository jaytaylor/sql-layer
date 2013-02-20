/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.rest;

import com.akiban.http.HttpConductor;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.server.service.servicemanager.GuicedServiceManager;
import com.akiban.server.test.it.ITBase;
import com.akiban.sql.RegexFilenameFilter;
import com.akiban.util.Strings;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public class RestServiceFilesIT extends ITBase {
    private static final Logger LOG = LoggerFactory.getLogger(RestServiceFilesIT.class.getName());
    private static final File RESOURCE_DIR = new File(
            "src/test/resources/" + RestServiceFilesIT.class.getPackage().getName().replace('.', '/')
    );
    public static final String SCHEMA_NAME = "test";

    private static class CaseParams {
        public final String subDir;
        public final String caseName;
        public final String requestMethod;
        public final String requestURI;
        public final String requestBody;
        public final String expectedHeader;
        public final String expectedResponse;
        public final String checkURI;
        public final String checkExpected;

        private CaseParams(String subDir, String caseName,
                           String requestMethod, String requestURI, String requestBody,
                           String expectedHeader, String expectedResponse,
                           String checkURI, String checkExpected) {
            this.subDir = subDir;
            this.caseName = caseName;
            this.requestMethod = requestMethod;
            this.requestURI = requestURI;
            this.requestBody = requestBody;
            this.expectedHeader = expectedHeader;
            this.expectedResponse = expectedResponse;
            this.checkURI = checkURI;
            this.checkExpected = checkExpected;
        }
    }

    protected final CaseParams caseParams;

    public RestServiceFilesIT(CaseParams caseParams) {
        this.caseParams = caseParams;
    }

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider().bindAndRequire(RestService.class, RestServiceImpl.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(RestServiceFilesIT.class);
    }

    public static File[] gatherRequestFiles(File dir) {
        File[] result = dir.listFiles(new RegexFilenameFilter(".*\\.(get|put|post|delete|query|explain)"));
        Arrays.sort(result, new Comparator<File>() {
            public int compare(File f1, File f2) {
                return f1.getName().compareTo(f2.getName());
            }
        });
        return result;
    }

    private static String dumpFileIfExists(File file) throws IOException {
        if(file.exists()) {
            return Strings.dumpFileToString(file);
        }
        return null;
    }

    private static String trimAndURLEncode(String s) throws UnsupportedEncodingException {
        return URLEncoder.encode(s.trim().replaceAll("\\s+", " "), "UTF-8");
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> gatherCases() throws Exception {
        Set<String> sawNames = new HashSet<>();
        Collection<Parameterization> result = new ArrayList<>();
        for(String subDirName: RESOURCE_DIR.list()) {
            File subDir = new File(RESOURCE_DIR, subDirName);
            if(!subDir.isDirectory()) {
                LOG.warn("Skipping unexpected file: {}", subDir);
                continue;
            }
            for(File requestFile : gatherRequestFiles(subDir)) {
                String inputName = requestFile.getName();
                int dotIndex = inputName.lastIndexOf('.');
                String caseName = inputName.substring(0, dotIndex);

                if(!sawNames.add(caseName)) {
                    throw new IllegalStateException("Duplicate case names: " + caseName);
                }

                String basePath = requestFile.getParent() + File.separator + caseName;
                String method = inputName.substring(dotIndex + 1).toUpperCase();
                String uri = Strings.dumpFileToString(requestFile).trim();
                String body = dumpFileIfExists(new File(basePath + ".body"));
                String header = dumpFileIfExists(new File(basePath + ".expected_header"));
                String expected = dumpFileIfExists(new File(basePath + ".expected"));
                String checkURI = dumpFileIfExists(new File(basePath + ".check"));
                String checkExpected = dumpFileIfExists(new File(basePath + ".check_expected"));

                if("QUERY".equals(method)) {
                    method = "GET";
                    uri = "/sql/query?q=" + trimAndURLEncode(uri);
                } else if("EXPLAIN".equals(method)) {
                    method = "GET";
                    uri = "/sql/explain?q=" + trimAndURLEncode(uri);
                }

                result.add(Parameterization.create(
                        subDirName + File.separator + caseName,
                        new CaseParams(subDirName, caseName, method, uri, body, header, expected, checkURI, checkExpected)
                ));
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
        if(schemaFile.exists()) {
            loadSchemaFile(SCHEMA_NAME, schemaFile);
        }
        File spaceFile = new File(subDir, "space.json");
        if(spaceFile.exists()) {
            HttpURLConnection httpConn = openConnection(getRestURL("/model/apply/" + SCHEMA_NAME), "POST");
            postContents(httpConn, Strings.dumpFileToString(spaceFile).getBytes());
            StringBuilder builder = new StringBuilder();
            try {
                Strings.readStreamTo(httpConn.getInputStream(), builder, true);
            } catch(Exception e) {
                Strings.readStreamTo(httpConn.getErrorStream(), builder, true);
                throw new RuntimeException("Failing creating initial space: " + builder.toString(), e);
            }
            httpConn.disconnect();
        }
        for (File data : subDir.listFiles(new RegexFilenameFilter(".*\\.dat"))) {
            loadDataFile(SCHEMA_NAME, data);
        }
    }
    
    public void checkRequest() throws Exception {
        if (caseParams.checkURI != null && caseParams.checkExpected != null) {
            HttpURLConnection httpConn = openConnection(getRestURL(caseParams.checkURI.trim()), "GET");
            try {
                String actual = getOutput (httpConn);
                compareExpected (caseParams.caseName + " check expected response ", caseParams.checkExpected, actual);
            } finally {
                httpConn.disconnect();
            }
        }
    }

    private static void postContents(HttpURLConnection httpConn, byte[] request) throws IOException {
        httpConn.setDoInput(true);
        httpConn.setFixedLengthStreamingMode(request.length);
        httpConn.setRequestProperty("Content-Type", "application/json");
        httpConn.setRequestProperty("Accept", "application/json");
        httpConn.getOutputStream().write(request);
    }

    @Test
    public void testRequest() throws Exception {
        loadDatabase(caseParams.subDir);

        HttpURLConnection httpConn = openConnection (getRestURL(caseParams.requestURI), caseParams.requestMethod);

        try {
            // Request
            if (caseParams.requestMethod.equals("POST") || caseParams.requestMethod.equals("PUT")) {
                if (caseParams.requestBody == null) {
                    throw new UnsupportedOperationException ("PUT/POST expects request body (<test>.body)");
                }
                LOG.debug(caseParams.requestBody);
                postContents(httpConn, caseParams.requestBody.getBytes());
            } // else GET || DELETE

            // Response
            String actual = getOutput(httpConn);
            compareExpected(caseParams.requestMethod + " response", caseParams.expectedResponse, actual);
        } finally {
            httpConn.disconnect();
        }
        checkRequest();
    }
    
    private HttpURLConnection openConnection(URL url, String requestMethod) throws IOException {
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setUseCaches(false);
        conn.setDoOutput(true);
        conn.setRequestMethod(requestMethod);
        return conn;
    }
     
    private String getOutput(HttpURLConnection httpConn) throws IOException {
        InputStream is;
        try {
            is = httpConn.getInputStream();
        } catch(Exception e) {
            is = httpConn.getErrorStream();
        }
        if (is == null) return null;
        StringBuilder builder = new StringBuilder();
        Strings.readStreamTo(is, builder, true);
        return builder.toString().length() > 0 ? builder.toString() : null;
    }
    
    private void compareExpected(String assertMsg, String expected, String actual) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode expectedNode = null;
        JsonNode actualNode = null;
        boolean skipNodeCheck = false;
        try {
            if(expected != null) {
                expectedNode = mapper.readTree(expected);
            }
            if(actual != null) {
                actualNode = mapper.readTree(actual);
            }
        } catch(JsonParseException e) {
            assertEquals(assertMsg, expected, actual);
            skipNodeCheck = true;
        }
        // Try manual equals and then assert strings for pretty print
        if(expectedNode != null && actualNode != null && !expectedNode.equals(actualNode)) {
            assertEquals(assertMsg, expectedNode.toString(), actualNode.toString());
        } else if(!skipNodeCheck) {
            assertEquals(assertMsg, expectedNode, actualNode);
        }
    }
}
