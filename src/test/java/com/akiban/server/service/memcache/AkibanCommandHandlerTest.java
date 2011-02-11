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

package com.akiban.server.service.memcache;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.server.RowData;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiProcessedGetRequest;
import com.akiban.server.api.HapiProcessor;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.List;

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
            HapiGetRequest.Predicate predicate = request.getPredicates().get(0);
            assertEquals("predicate column", expectedColumn, predicate.getColumnName());
            assertEquals("predicate value", expectedValue, predicate.getValue());

            try {
                outputter.output(null, null, outputStream);
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
        public void output(HapiProcessedGetRequest request, List<RowData> rows,
                           OutputStream outputStream) throws IOException
        {
            outputStream.write( string.getBytes(charset) );
        }
    }

    @Test
    public void testTwice() throws HapiRequestException {
        Session session = new SessionImpl();
        testWriteBytes(session, "first test");
        testWriteBytes(session, "second test");
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
}
