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

package com.akiban.server.service.memcache;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiPredicate;
import com.akiban.server.api.HapiProcessedGetRequest;
import com.akiban.server.api.HapiProcessor;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.TestSessionFactory;
import com.thimbleware.jmemcached.CacheElement;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;

import static org.junit.Assert.*;

public final class AkibanCommandHandlerTest {
    private static final Charset CHARSET = Charset.forName("UTF-8");

    private static class MockedHapiProcessor implements HapiProcessor
    {
        private final String expectedSchema;
        private final String expectedTable;
        private final String expectedColumn;
        private final String expectedValue;

        private MockedHapiProcessor(String expectedSchema, String expectedTable, String expectedColumn, String expectedValue) {
            this.expectedSchema = expectedSchema;
            this.expectedTable = expectedTable;
            this.expectedColumn = expectedColumn;
            this.expectedValue = expectedValue;
        }

        @Override
        public void processRequest(Session session, HapiGetRequest request,
                                   HapiOutputter outputter, OutputStream outputStream)
                throws HapiRequestException
        {
            assertEquals("schema", expectedSchema, request.getSchema());
            assertEquals("select table", expectedTable, request.getTable());
            assertEquals("using table", new TableName(expectedSchema, expectedTable), request.getUsingTable());
            assertEquals("predicate count", 1, request.getPredicates().size());
            HapiPredicate predicate = request.getPredicates().get(0);
            assertEquals("predicate column", expectedColumn, predicate.getColumnName());
            assertEquals("predicate value", expectedValue, predicate.getValue());

            try {
                outputter.output(null, true, null, outputStream);
            } catch (IOException e) {
                throw new RuntimeException("unexpected", e);
            }
        }

        @Override
        public Index findHapiRequestIndex(Session session, HapiGetRequest request) throws HapiRequestException {
            return null;
        }
    }

    private static class MockedOutputter implements HapiOutputter
    {
        private final String string;
        private final Charset charset;

        private MockedOutputter(String string) {
            this.string = string;
            this.charset = CHARSET;
        }

        @Override
        public void output(HapiProcessedGetRequest request,
                           boolean hKeyOrdered,
                           Iterable<RowData> rows,
                           OutputStream outputStream) throws IOException
        {
            outputStream.write( string.getBytes(charset) );
        }
    }

    @Test
    public void testTwice() throws HapiRequestException {
        Session session = session();
        testWriteBytes(session, "first test");
        testWriteBytes(session, "second test");
    }

    @Test // bug 720970
    public void ignoreTrailingEmptyKey() throws HapiRequestException {
        final MockedHapiProcessor processor = new MockedHapiProcessor("schema", "table", "column", "value");
        final String testString = "hi there";
        final HapiOutputter outputter = new MockedOutputter(testString);

        CacheElement[] result = AkibanCommandHandler.handleGetKeys(
                Arrays.asList("schema:table:column=value", ""),
                session(), processor, outputter);

        final byte[] expectedBytes = testString.getBytes(CHARSET);
        assertArrayEquals("bytes", expectedBytes, result[0].getData());
        assertEquals("result elements", 1, result.length);
    }

    @Test
    public void onlyOneKey() throws HapiRequestException {
        final MockedHapiProcessor processor = new MockedHapiProcessor("schema", "table", "column", "value");
        final String testString = "hi there";
        final HapiOutputter outputter = new MockedOutputter(testString);

        CacheElement[] result = AkibanCommandHandler.handleGetKeys(
                Arrays.asList("schema:table:column=value"),
                session(), processor, outputter);

        final byte[] expectedBytes = testString.getBytes(CHARSET);
        assertArrayEquals("bytes", expectedBytes, result[0].getData());
        assertEquals("result elements", 1, result.length);
    }

    @Test(expected=HapiRequestException.class)
    public void firstKeyEmpty() throws HapiRequestException {
        final MockedHapiProcessor processor = new MockedHapiProcessor("schema", "table", "column", "value");
        final String testString = "hi there";
        final HapiOutputter outputter = new MockedOutputter(testString);

        CacheElement[] result = AkibanCommandHandler.handleGetKeys(
                Arrays.asList("", "schema:table:column=value"),
                session(), processor, outputter);

        final byte[] expectedBytes = testString.getBytes(CHARSET);
        assertArrayEquals("bytes", expectedBytes, result[0].getData());
        assertEquals("result elements", 1, result.length);
    }

    private static void testWriteBytes(Session session, String testString) throws HapiRequestException {
        final byte[] expectedBytes = testString.getBytes(CHARSET);

        final MockedHapiProcessor processor = new MockedHapiProcessor("schema", "table", "column", "value");
        final HapiOutputter outputter = new MockedOutputter(testString);

        final byte[] actualBytes = AkibanCommandHandler.getBytesForGets(
                session, "schema:table:column=value",
                processor, outputter
        );

        assertArrayEquals("bytes", expectedBytes, actualBytes);
    }

    private static Session session() {
        return TestSessionFactory.get().createSession();
    }
}
