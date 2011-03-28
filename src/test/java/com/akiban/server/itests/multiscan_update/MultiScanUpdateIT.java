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

package com.akiban.server.itests.multiscan_update;

import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.BufferFullException;
import com.akiban.server.api.dml.scan.BufferedLegacyOutputRouter;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.LegacyOutputConverter;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;

import static org.junit.Assert.assertTrue;

@Ignore("bug 744400")
public final class MultiScanUpdateIT extends ApiTestBase {
    private final static String SCHEMA = "sch";
    private final static String TABLE = "tbl";
    private final static int MAX_ID = 1000;
    private int tableId;

    @Before
    public void setUp() throws InvalidOperationException {
        tableId = createTable(
                SCHEMA, TABLE,
                "id int key",
                "name varchar (255)",
                "nickname varchar (255)"
        );
        StringBuilder builder = new StringBuilder(255);

        for (int i = 1; i <= MAX_ID; ++i) {
            for (int c = 0; c < 255; ++c) {
                builder.append( i % 9 );
            }
            String name = builder.toString();
            builder.setLength(0);

            for (int c = 0; c < 255; ++c) {
                builder.append((i % 9) + 1);
            }
            String nickname = builder.toString();
            builder.setLength(0);
            
            writeRows(
                    createNewRow(tableId, (long) i, name, nickname)
            );
        }
    }

    @Test
    public void updateWithMultiScans() throws InvalidOperationException {
        Iterator<NewRow> scanIterator = new ScanIterator(dml(), 1024, scanAllRequest(tableId));

        ColumnSelector selector = new ColumnSelector() {
            @Override
            public boolean includesColumn(int columnPosition) {
                return true;
            }
        };
        while (scanIterator.hasNext()) {
            NewRow oldRow = scanIterator.next();
            long id = get(oldRow, 0, Long.class);
            assertTrue("saw updated row: " + oldRow, id <= MAX_ID);
            NiceRow newRow = new NiceRow(oldRow);
            newRow.put(0, id + (MAX_ID * 10) );
            dml().updateRow(session(), oldRow, newRow, selector);
        }
    }

    private final static class ScanIterator implements Iterator<NewRow> {
        private final BufferedLegacyOutputRouter router;
        private final ListRowOutput output;
        private Iterator<NewRow> outputIterator;
        boolean hasMore;
        private final CursorId cursorId;
        private final Session session = new SessionImpl();
        private final DMLFunctions dml;

        ScanIterator(DMLFunctions dml, int bufferSize, ScanRequest request) throws InvalidOperationException {
            router = new BufferedLegacyOutputRouter(bufferSize, false);
            LegacyOutputConverter converter = new LegacyOutputConverter(dml);
            output = new ListRowOutput();
            converter.setOutput(output);
            converter.setColumnsToScan(new HashSet<Integer>(Arrays.asList(0, 1)));
            router.addHandler(converter);
            hasMore = true;
            cursorId = dml.openCursor(session(), request);
            outputIterator = Collections.<NewRow>emptyList().iterator();
            this.dml = dml;
            getNextBatch();
        }

        @Override
        public boolean hasNext() {
            return outputIterator.hasNext() || hasMore;
        }

        @Override
        public NewRow next() {
            if (!outputIterator.hasNext()) {
                getNextBatch();
            }
            return outputIterator.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private void getNextBatch() {
            assert hasMore;
            assert ! (outputIterator != null && outputIterator.hasNext());
            output.clear();
            try {
                dml().scanSome(session(), cursorId, router);
                hasMore = false;
            } catch (BufferFullException e) {
                if (output.getRows().isEmpty()) {
                    throw new RuntimeException(e); // couldn't pick up even a single row!
                }
                router.reset(0);
            } catch (InvalidOperationException e) {
                throw new RuntimeException(e);
            }
            outputIterator = output.getRows().iterator();
        }

        protected final Session session() {
            return session;
        }

        protected DMLFunctions dml() {
            return dml;
        }
    }
}
