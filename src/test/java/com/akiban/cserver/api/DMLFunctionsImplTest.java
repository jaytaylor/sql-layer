package com.akiban.cserver.api;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.cserver.api.dml.scan.LegacyRowOutput;
import com.akiban.cserver.api.dml.scan.*;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import com.akiban.cserver.store.RowCollector;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import static com.akiban.cserver.api.DMLFunctionsImpl.doScan;
import static junit.framework.Assert.*;

public final class DMLFunctionsImplTest {
    private static class StringRowCollector implements RowCollector {
        private final List<String> strings;
        private final int tableId;
        private boolean open;
        private int deliveredRows;

        public StringRowCollector(int tableId, String... strings) {
            this.tableId = tableId;
            this.strings = new ArrayList<String>(Arrays.asList(strings));
            open = true;
        }

        @Override
        public boolean collectNextRow(ByteBuffer payload) {
            checkOpen();
            if (strings.isEmpty()) {
                throw new NoSuchElementException();
            }
            String string = strings.remove(0);
            payload.putInt(string.length());
            payload.put(string.getBytes());
            ++deliveredRows;

            return ! strings.isEmpty();
        }

        @Override
        public boolean hasMore() throws Exception {
            checkOpen();
            return ! strings.isEmpty();
        }

        private void checkOpen() {
            if (!open) {
                throw new IllegalStateException("not open");
            }
        }

        @Override
        public void close() {
            checkOpen();
            open = false;
        }

        @Override
        public int getDeliveredRows() {
            return deliveredRows;
        }

        @Override
        public int getDeliveredBuffers() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getRepeatedRows() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getDeliveredBytes() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getTableId() {
            return tableId;
        }

        @Override
        public long getId() {
            throw new UnsupportedOperationException();
        }
    }

    private static class StringRowOutput implements LegacyRowOutput {
        private final ByteBuffer buffer;

        private StringRowOutput() {
            buffer = ByteBuffer.allocate(2048); // should be plenty
            buffer.putInt(0);
        }

        @Override
        public ByteBuffer getOutputBuffer() {
            return buffer;
        }

        public List<String> getStrings() {
            int rowsLeft = getRowsCount();
            final List<String> ret = new ArrayList<String>(rowsLeft);
            int pos = Integer.SIZE / 8; // first int is the number of rows
            while ( (rowsLeft--) > 0) {
                int length = buffer.getInt(pos);
                pos += Integer.SIZE / 8;
                byte[] bytes = new byte[length];
                for (int off=0; off < length; ++off) {
                    bytes[off] = buffer.get(pos + off);
                }
                pos += length;
                ret.add(new String(bytes));
            }
            return ret;
        }

        @Override
        public void wroteRow() {
            buffer.putInt(0, 1 + getRowsCount() );
        }

        @Override
        public int getRowsCount() {
            return buffer.getInt(0);
        }
    }

    private static class TestingStruct {
        final String[] stringsArray;
        final StringRowCollector collector;
        final StringRowOutput output;
        final Cursor cursor;
        final CursorId cursorId;

        TestingStruct(String... strings) {
            final int TABLE_ID = 3;
            stringsArray = strings;
            collector = new StringRowCollector(TABLE_ID, strings);
            output = new StringRowOutput();
            cursor = new Cursor(collector);
            cursorId = new CursorId(5, TABLE_ID);
        }
    }

    private static class TestDML extends DMLFunctionsImpl {
        private final StringRowCollector collector;

        private TestDML(Session session, String... rowsToCollect) {
            super("DEBUG", session);
            collector = new StringRowCollector(1, rowsToCollect);
        }

        @Override
        protected RowCollector getRowCollector(ScanRequest request) {
            return collector;
        }
    }

    @Test(expected= CursorIsFinishedException.class)
    public void scansNoLimit() throws InvalidOperationException {
        final TestingStruct s = new TestingStruct("Hi", "there", "pooh bear", "how are you there");

        try {
            assertFalse("expected end", doScan(s.cursor, s.cursorId, s.output, -1));

            assertEquals("rc rows delivered", s.stringsArray.length, s.collector.getDeliveredRows());
            assertEquals("output rows written", s.stringsArray.length, s.output.getRowsCount());

            assertEquals("rows seen", Arrays.asList(s.stringsArray), s.output.getStrings());
        } catch (InvalidOperationException e) {
            throw new RuntimeException(e);
        }

        doScan(s.cursor, s.cursorId, s.output, 0);
    }

    @Test(expected= CursorIsFinishedException.class)
    public void scansWithLimit() throws InvalidOperationException {
        final TestingStruct s = new TestingStruct("hi", "world", "and", "universe", "too");

        try {
            assertTrue("expected more", doScan(s.cursor, s.cursorId, s.output, 1));
            assertEquals("rc rows delivered", 1, s.collector.getDeliveredRows());
            assertEquals("output rows written", 1, s.output.getRowsCount());
            assertEquals("rows seen", Arrays.asList("hi"), s.output.getStrings());

            assertTrue("expected more", doScan(s.cursor, s.cursorId, s.output, 2));
            assertEquals("rc rows delivered", 3, s.collector.getDeliveredRows());
            assertEquals("output rows written", 3, s.output.getRowsCount());
            assertEquals("rows seen", Arrays.asList("hi", "world", "and"), s.output.getStrings());

            assertTrue("expected more", doScan(s.cursor, s.cursorId, s.output, 0));
            assertEquals("rc rows delivered", 3, s.collector.getDeliveredRows());
            assertEquals("output rows written", 3, s.output.getRowsCount());
            assertEquals("rows seen", Arrays.asList("hi", "world", "and"), s.output.getStrings());

            assertTrue("expected more", doScan(s.cursor, s.cursorId, s.output, 0));
            assertEquals("rc rows delivered", 3, s.collector.getDeliveredRows());
            assertEquals("output rows written", 3, s.output.getRowsCount());
            assertEquals("rows seen", Arrays.asList("hi", "world", "and"), s.output.getStrings());

            assertFalse("expected end", doScan(s.cursor, s.cursorId, s.output, -1));
            assertEquals("rc rows delivered", 5, s.collector.getDeliveredRows());
            assertEquals("output rows written", 5, s.output.getRowsCount());
            assertEquals("rows seen", Arrays.asList(s.stringsArray), s.output.getStrings());
        } catch (InvalidOperationException e) {
            throw new RuntimeException(e);
        }

        doScan(s.cursor, s.cursorId, s.output, 1);
    }

    @Test(expected= CursorIsFinishedException.class)
    public void scanEmptyRC() throws InvalidOperationException {
        final TestingStruct s = new TestingStruct();
        try {
            assertFalse("expected end", doScan(s.cursor, s.cursorId, s.output, 0));
        } catch (InvalidOperationException e) {
            throw new RuntimeException(e);
        }

        doScan(s.cursor, s.cursorId, s.output, 0);
    }

    @Test
    public void testSessionLifecycle() throws InvalidOperationException {
        Session session = new SessionImpl();
        TestDML testDML = new TestDML(session, "Hi there poohbear".split(" "));

        final StringRowOutput output = new StringRowOutput();
        final CursorId cursorId = testDML.openCursor((ScanRequest)null);

        assertTrue("expected more", testDML.scanSome(cursorId, output, 0));
        assertEquals("rows collected", 0, output.getRowsCount());

        assertTrue("expected more", testDML.scanSome(cursorId, output, 1));
        assertEquals("rows collected", 1, output.getRowsCount());

        assertFalse("expected more", testDML.scanSome(cursorId, output, -1));
        assertEquals("rows collected", 3, output.getRowsCount());

        testDML.closeCursor(cursorId);

        boolean sawException = false;
        try {
            testDML.closeCursor(cursorId);
        } catch (CursorIsUnknownException e) {
            sawException = true;
        }
        assertTrue("expected exception", sawException);


    }

    @Test
    public void testTheTestClasses() {
        final String[] stringsArray = new String[] {"Hello", "world", "how are you"};
        StringRowCollector rc = new StringRowCollector(4, stringsArray);
        StringRowOutput output = new StringRowOutput();

        while (rc.collectNextRow(output.getOutputBuffer())) {
            output.wroteRow();
        }
        output.wroteRow();

        assertEquals("rc rows collected", 3, rc.getDeliveredRows());
        assertEquals("rows written", 3, output.getRowsCount());

        assertEquals("rows", Arrays.asList(stringsArray), output.getStrings());
    }
}
