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

import com.akiban.ais.model.TableName;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.dml.scan.BufferFullException;
import com.akiban.server.api.dml.scan.BufferedLegacyOutputRouter;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.LegacyOutputConverter;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertTrue;

@RunWith(NamedParameterizedRunner.class)
public final class MultiScanUpdateIT extends ApiTestBase {

    private final static String SCHEMA = "sch";
    private final static String TABLE = "tbl";
    private final static int MAX_ID = 1000;
    private static final TableName TABLE_NAME = new TableName(SCHEMA, TABLE);

    @SuppressWarnings("unused") // accessed via WhichIndex.values
    private enum WhichIndex {
        PK {
            @Override
            public void updateInPlace(NewRow row) {
                long id = get(row, 0, Long.class);
                row.put(0, id + (MAX_ID * 10));
            }

            @Override
            boolean isPKUpdated() {
                return true;
            }
        },
        NAME {
            @Override
            public void updateInPlace(NewRow row) {
                    String name = get(row, 1, String.class);
                    row.put(1, name.substring(0, 10));
            }

            @Override
            boolean isPKUpdated() {
                return false;
            }
        },
        NONE {
            @Override
            public void updateInPlace(NewRow row) {
                    String nickname = get(row, 2, String.class);
                    row.put(2, nickname.substring(0, 11));
            }

            @Override
            boolean isPKUpdated() {
                return false;
            }
        },
        ALL {
            @Override
            void updateInPlace(NewRow row) {
                PK.updateInPlace(row);
                NAME.updateInPlace(row);
                NONE.updateInPlace(row);
            }

            @Override
            boolean isPKUpdated() {
                return true;
            }
        }
        ;

        abstract void updateInPlace(NewRow row);
        abstract boolean isPKUpdated();
    }

    @NamedParameterizedRunner.TestParameters
    public static List<Parameterization> params() {
        ParameterizationBuilder builder = new ParameterizationBuilder();

        for (WhichIndex scanIndex : Arrays.asList(WhichIndex.PK, WhichIndex.NAME)) {
            for (WhichIndex updateIndex : WhichIndex.values()) {
                builder.create(
                        String.format("scan %s update %s", scanIndex, updateIndex),
                        ! updateIndex.isPKUpdated(),
                        scanIndex,
                        updateIndex);
            }
        }

        return builder.asList();
    }

    private final WhichIndex scanIndex;
    private final WhichIndex updateColumn;
    private int tableId;

    public MultiScanUpdateIT(WhichIndex scanIndex, WhichIndex updateColumn) {
        this.scanIndex = scanIndex;
        this.updateColumn = updateColumn;
    }

    @Before
    public void setUp() throws InvalidOperationException {
        tableId = createTable(
                SCHEMA, TABLE,
                "id int key",
                "name varchar (255)",
                "nickname varchar (255)",
                "key (name)"
        );

        for (int i = 1; i <= MAX_ID; ++i) {
            writeRows( getRow(i) );
        }
    }

    private NewRow getRow(int i) {
        StringBuilder builder = new StringBuilder();
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
        return createNewRow(tableId, (long) i, name, nickname);
    }

    @Test
    public void test() throws InvalidOperationException {
        final String scanIndexName;
        switch (scanIndex) {
            case PK:
                scanIndexName = "PRIMARY";
                break;
            case NAME:
                scanIndexName = "NAME";
                break;
            default:
                throw new RuntimeException(scanIndex.name());
        }
        int scanIndexId = ddl().getUserTable(session(), TABLE_NAME).getIndex(scanIndexName).getIndexId();

        ScanRequest request = new ScanAllRequest(tableId, set(0, 1, 2), scanIndexId, null, ScanLimit.NONE);
        Iterator<NewRow> scanIterator = new ScanIterator(dml(), 1024, request);

        while (scanIterator.hasNext()) {
            NewRow oldRow = scanIterator.next();
            assertTrue("saw updated row: " + oldRow, get(oldRow, 0, Long.class) <= MAX_ID);
            NewRow newRow = new NiceRow(oldRow);
            updateColumn.updateInPlace(newRow);
            dml().updateRow(session(), oldRow, newRow, ALL_COLUMNS);
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
            converter.setColumnsToScan(new HashSet<Integer>(Arrays.asList(0, 1, 2)));
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
