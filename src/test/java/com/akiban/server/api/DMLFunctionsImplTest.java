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

package com.akiban.server.api;

import static com.akiban.server.api.DMLFunctionsImpl.doScan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

import com.akiban.server.RowData;
import com.akiban.server.api.dml.scan.BufferFullException;
import org.junit.Assert;
import org.junit.Test;

import com.akiban.server.AkServerTestCase;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.dml.scan.Cursor;
import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.CursorIsFinishedException;
import com.akiban.server.api.dml.scan.LegacyRowOutput;
import com.akiban.server.store.RowCollector;

public final class DMLFunctionsImplTest extends AkServerTestCase {
    
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
                return false;
            }
            String string = strings.remove(0);
            payload.putInt(string.length());
            payload.put(string.getBytes());
            ++deliveredRows;

            return true;
        }

        @Override
        public RowData collectNextRow() throws Exception
        {
            Assert.fail();
            return null;
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

    @Test(expected= CursorIsFinishedException.class)
    public void scansNoLimit() throws InvalidOperationException, BufferFullException {
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
    public void scansWithLimit() throws InvalidOperationException, BufferFullException {
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
    public void scanEmptyRC() throws InvalidOperationException, BufferFullException {
        final TestingStruct s = new TestingStruct();
        try {
            assertFalse("expected end", doScan(s.cursor, s.cursorId, s.output, 0));
        } catch (InvalidOperationException e) {
            throw new RuntimeException(e);
        }

        doScan(s.cursor, s.cursorId, s.output, 0);
    }
}
