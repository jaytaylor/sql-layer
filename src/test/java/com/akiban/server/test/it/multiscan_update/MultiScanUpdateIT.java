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

package com.akiban.server.test.it.multiscan_update;

import com.akiban.ais.model.TableName;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.OnlyIf;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.dml.ConstantColumnSelector;
import com.akiban.server.api.dml.scan.BufferFullException;
import com.akiban.server.api.dml.scan.BufferedLegacyOutputRouter;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.LegacyOutputConverter;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.server.api.dml.scan.ScanRequest;
import com.akiban.server.error.ConcurrentScanAndUpdateException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.service.session.Session;
import com.akiban.server.test.it.ITBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
                long id = get(row, 0, Long.class);
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

    private NewRow getRow(int i) {
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
        return createNewRow(tableId, (long) i, name, nickname);
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
                scanIndexName = "NAME";
                break;
            default:
                throw new RuntimeException(scanIndex.name());
        }
        int scanIndexId = ddl().getUserTable(session(), TABLE_NAME).getIndex(scanIndexName).getIndexId();

        ScanRequest request = new ScanAllRequest(tableId, set(0, 1, 2), scanIndexId, null, ScanLimit.NONE);
        ScanIterator scanIterator = new ScanIterator(dml(), aisGeneration(), 1024, request, session());

        try {
            while (scanIterator.hasNext()) {
                NewRow oldRow = scanIterator.next();
                assertTrue("saw updated row: " + oldRow, get(oldRow, 0, Long.class) <= MAX_ID);
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
            converter.reset(session, output, new HashSet<Integer>(Arrays.asList(0, 1, 2)));
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
