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

package com.foundationdb.server.test.it.multiscan_update;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.OnlyIf;
import com.foundationdb.junit.OnlyIfNot;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.DMLFunctions;
import com.foundationdb.server.api.dml.ConstantColumnSelector;
import com.foundationdb.server.api.dml.scan.BufferFullException;
import com.foundationdb.server.api.dml.scan.BufferedLegacyOutputRouter;
import com.foundationdb.server.api.dml.scan.CursorId;
import com.foundationdb.server.api.dml.scan.LegacyOutputConverter;
import com.foundationdb.server.api.dml.scan.NewRow;
import com.foundationdb.server.api.dml.scan.NiceRow;
import com.foundationdb.server.api.dml.scan.ScanAllRequest;
import com.foundationdb.server.api.dml.scan.ScanLimit;
import com.foundationdb.server.api.dml.scan.ScanRequest;
import com.foundationdb.server.error.ConcurrentScanAndUpdateException;
import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Ignore("Accesses disposed transaction")
@RunWith(NamedParameterizedRunner.class)
public class MultiScanUpdateIT extends ITBase {

    private final static String SCHEMA = "sch";
    private final static String TABLE = "tbl";
    private final static int MAX_ID = 1000;
    private static final TableName TABLE_NAME = new TableName(SCHEMA, TABLE);
    private static final int COL_WIDTH = 255; // must  be >= 11 for WhichIndex.updateInPlace

    protected enum WhichIndex {
        PK {
            @Override
            public void updateInPlace(NewRow row) {
                long id = get(row, 0, Integer.class);
                row.put(0, id + (MAX_ID * 10));
            }

        },
        NAME {
            @Override
            public void updateInPlace(NewRow row) {
                    String name = get(row, 1, String.class);
                    row.put(1, name.substring(0, 10));
            }

        },
        NONE {
            @Override
            public void updateInPlace(NewRow row) {
                    String nickname = get(row, 2, String.class);
                    row.put(2, nickname.substring(0, 11));
            }

        },
        ALL {
            @Override
            void updateInPlace(NewRow row) {
                PK.updateInPlace(row);
                NAME.updateInPlace(row);
                NONE.updateInPlace(row);
            }

        }
        ;

        abstract void updateInPlace(NewRow row);
    }

    protected enum TestMode {
        IT,
        MT
    }

    @NamedParameterizedRunner.TestParameters
    public static List<Parameterization> params() {
        return params(TestMode.IT);
    }

    protected static List<Parameterization> params(TestMode testMode) {
        ParameterizationBuilder builder = new ParameterizationBuilder();

        for (WhichIndex scanIndex : Arrays.asList(WhichIndex.PK, WhichIndex.NAME)) {
            for (WhichIndex updateIndex : WhichIndex.values()) {
                builder.add(
                        String.format("scan %s update %s", scanIndex, updateIndex),
                        testMode,
                        scanIndex,
                        updateIndex);
            }
        }

        return builder.asList();
    }

    private final WhichIndex scanIndex;
    private final WhichIndex updateColumn;
    private int tableId;

    public MultiScanUpdateIT(TestMode testMode, WhichIndex scanIndex, WhichIndex updateColumn) {
        super(testMode.name());
        this.scanIndex = scanIndex;
        this.updateColumn = updateColumn;
    }

    @Before
    public void setUp() throws InvalidOperationException {
        tableId = createTable(
                SCHEMA, TABLE,
                "id int not null primary key",
                "name varchar (255)",
                "nickname varchar (255)"
        );
        createIndex(SCHEMA, TABLE, "name", "name");

        for (int i = 1; i <= MAX_ID; ++i) {
            writeRows(getRow(i));
        }
    }

    private Row getRow(int i) {
        StringBuilder builder = new StringBuilder();
        for (int c = 0; c < COL_WIDTH; ++c) {
            builder.append( i % 9 );
        }
        String name = builder.toString();
        builder.setLength(0);

        for (int c = 0; c < COL_WIDTH; ++c) {
            builder.append((i % 9) + 1);
        }
        String nickname = builder.toString();
        builder.setLength(0);
        return row(tableId, i, name, nickname);
    }

    @Test(expected=ConcurrentScanAndUpdateException.class)
    @OnlyIf("exceptionExpected()")
    public void expectException() throws InvalidOperationException{
        test();
    }

    @Test
    @OnlyIfNot("exceptionExpected()")
    public void expectSuccess() throws InvalidOperationException{
        test();
    }

    public boolean exceptionExpected() {
        return
                WhichIndex.ALL.equals(updateColumn)
                || scanIndex.equals(updateColumn)
                || WhichIndex.PK.equals(updateColumn);
    }

    public void test() throws InvalidOperationException {
        final String scanIndexName;
        switch (scanIndex) {
            case PK:
                scanIndexName = "PRIMARY";
                break;
            case NAME:
                scanIndexName = "name";
                break;
            default:
                throw new RuntimeException(scanIndex.name());
        }
        int scanIndexId = ddl().getTable(session(), TABLE_NAME).getIndex(scanIndexName).getIndexId();

        ScanRequest request = new ScanAllRequest(tableId, set(0, 1, 2), scanIndexId, null, ScanLimit.NONE);
        ScanIterator scanIterator = new ScanIterator(dml(), aisGeneration(), 1024, request, session());

        try {
            while (scanIterator.hasNext()) {
                NewRow oldRow = scanIterator.next();
                assertTrue("saw updated row: " + oldRow, get(oldRow, 0, Integer.class) <= MAX_ID);
                NewRow newRow = new NiceRow(oldRow);
                updateColumn.updateInPlace(newRow);
                doUpdate(oldRow, newRow);
            }
        } catch (ConcurrentScanAndUpdateException e) {
            assertEquals("calls to scanSome", 2, scanIterator.getScanSomeCalls());
            throw e;
        } finally {
            scanIterator.close();
        }
    }

    protected void doUpdate(NewRow oldRow, NewRow newRow) {
        dml().updateRow(session(), oldRow, newRow, ConstantColumnSelector.ALL_ON);
    }


    private final static class ScanIterator implements Iterator<NewRow> {
        private final BufferedLegacyOutputRouter router;
        private final ListRowOutput output;
        private Iterator<NewRow> outputIterator;
        boolean hasMore;
        private final CursorId cursorId;
        private final Session session;
        private final DMLFunctions dml;
        private int scanSomeCalls;

        ScanIterator(DMLFunctions dml, int aisGeneration, int bufferSize, ScanRequest request, Session session)
                throws InvalidOperationException
        {
            this.scanSomeCalls = 0;
            this.session = session;
            router = new BufferedLegacyOutputRouter(bufferSize, false);
            LegacyOutputConverter converter = new LegacyOutputConverter(dml);
            output = new ListRowOutput();
            converter.reset(session, output, new HashSet<>(Arrays.asList(0, 1, 2)));
            router.addHandler(converter);
            hasMore = true;
            cursorId = dml.openCursor(session(), aisGeneration, request);
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
            ++scanSomeCalls;
            try {
                dml().scanSome(session(), cursorId, router);
                hasMore = false;
            } catch (BufferFullException e) {
                if (output.getRows().isEmpty()) {
                    throw new RuntimeException(e); // couldn't pick up even a single row!
                }
                router.reset(0);
            }
            outputIterator = output.getRows().iterator();
        }

        protected final Session session() {
            return session;
        }

        protected DMLFunctions dml() {
            return dml;
        }

        private int getScanSomeCalls() {
            return scanSomeCalls;
        }

        public void close() {
            dml().closeCursor(session(), cursorId);
        }
    }
}
