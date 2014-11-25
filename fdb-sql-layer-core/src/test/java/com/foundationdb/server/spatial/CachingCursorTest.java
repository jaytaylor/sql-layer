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

package com.foundationdb.server.spatial;

import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.operator.CursorBase;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.value.ValueSource;
import com.geophile.z.Cursor;
import com.geophile.z.DuplicateRecordException;
import com.geophile.z.Index;
import com.geophile.z.Record;
import com.geophile.z.RecordFilter;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class CachingCursorTest
{
    @Test
    public void test0() throws IOException, InterruptedException
    {
        CachingCursor cursor = cachingCursor(0);
        assertNull(cursor.next());
        cursor.jump(START, null);
        assertNull(cursor.next());
    }

    @Test
    public void test1() throws IOException, InterruptedException
    {
        CachingCursor cursor = cachingCursor(1);
        TestRecord record;
        // Get first record
        record = (TestRecord) cursor.next();
        assertNotNull(record);
        assertEquals(0, record.id);
        // Reset and get it again
        cursor.jump(START, null);
        record = (TestRecord) cursor.next();
        assertNotNull(record);
        assertEquals(0, record.id);
        // There should be nothing else
        assertNull(cursor.next());
        // Reset should fail
        try {
            cursor.jump(START, null);
            fail();
        } catch (CachingCursor.NotResettableException e) {
            // Expected
        }
    }

    @Test
    public void testMoreThanOne() throws IOException, InterruptedException
    {
        for (int n = 2; n < 10; n++) {
            CachingCursor cursor = cachingCursor(n);
            TestRecord record;
            // Get first record
            record = (TestRecord) cursor.next();
            assertNotNull(record);
            assertEquals(0, record.id);
            // Reset and get it again
            cursor.jump(START, null);
            record = (TestRecord) cursor.next();
            assertNotNull(record);
            assertEquals(0, record.id);
            // Get the next record
            record = (TestRecord) cursor.next();
            assertNotNull(record);
            assertEquals(1, record.id);
            // Reset should not work
            try {
                cursor.jump(START, null);
                fail();
            } catch (CachingCursor.NotResettableException e) {
                // Expected
            }
        }
    }

    @Test
    public void testBadReset() throws IOException, InterruptedException
    {
        CachingCursor cursor = cachingCursor(1);
        TestRecord record;
        // Get first record
        record = (TestRecord) cursor.next();
        assertNotNull(record);
        assertEquals(0, record.id);
        // Reset and get it again
        TestRecord notStart = new TestRecord(0);
        notStart.z(Z + 1);
        try {
            cursor.jump(notStart, null);
            fail();
        } catch (CachingCursor.NotResettableException e) {
            // Expected
        }
    }

    private CachingCursor cachingCursor(int n) throws IOException, InterruptedException
    {
        TestIndex index = new TestIndex();
        for (int id = 0; id < n; id++) {
            index.add(new TestRecord(id));
        }
        TestInputCursor inputCursor = new TestInputCursor(index.records);
        return new CachingCursor(Z, inputCursor);
    }

    private static final long Z = 0;
    private static final TestRecord START = new TestRecord(0);
    static
    {
        START.z(0);
    }

    private static class TestRecord implements Row
    {
        @Override
        public RowType rowType()
        {
            fail();
            return null;
        }

        @Override
        public HKey hKey()
        {
            fail();
            return null;
        }

        @Override
        public HKey ancestorHKey(Table table)
        {
            fail();
            return null;
        }

        @Override
        public boolean ancestorOf(Row that)
        {
            fail();
            return false;
        }

        @Override
        public boolean containsRealRowOf(Table table)
        {
            fail();
            return false;
        }

        @Override
        public Row subRow(RowType subRowType)
        {
            fail();
            return null;
        }

        @Override
        public boolean isBindingsSensitive()
        {
            fail();
            return false;
        }

        @Override
        public int compareTo(Row row, int leftStartIndex, int rightStartIndex, int fieldCount)
        {
            fail();
            return 0;
        }

        @Override
        public ValueSource value(int index)
        {
            fail();
            return null;
        }

        @Override
        public long z()
        {
            return z;
        }

        @Override
        public void z(long z)
        {
            this.z = z;
        }

        @Override
        public void copyTo(Record record)
        {
            fail();
        }

        public TestRecord(int id)
        {
            this.id = id;
        }

        private final int id;
        private long z;
    }

    private static class TestIndex extends Index<TestRecord>
    {
        @Override
        public void add(TestRecord record)
            throws IOException, InterruptedException, DuplicateRecordException
        {
            records.add(record);
        }

        @Override
        public boolean remove(long z, RecordFilter<TestRecord> recordFilter)
            throws IOException, InterruptedException
        {
            fail();
            return false;
        }

        @Override
        public Cursor<TestRecord> cursor() throws IOException, InterruptedException
        {
            fail();
            return null;
        }

        @Override
        public TestRecord newRecord()
        {
            return null;
        }

        @Override
        public boolean blindUpdates()
        {
            return false;
        }

        @Override
        public boolean stableRecords()
        {
            return true;
        }

        List<TestRecord> records = new ArrayList<>();
    }

    private static class TestInputCursor implements CursorBase<TestRecord>
    {
        @Override
        public void open()
        {
            fail();
        }

        @Override
        public TestRecord next()
        {
            return
                iterator.hasNext()
                ? iterator.next()
                : null;
        }

        @Override
        public void close()
        {
            fail();
        }

        @Override
        public boolean isIdle()
        {
            fail();
            return false;
        }

        @Override
        public boolean isActive()
        {
            fail();
            return false;
        }

        @Override
        public boolean isClosed()
        {
            fail();
            return false;
        }

        @Override
        public void setIdle()
        {
            fail();
        }

        public TestInputCursor(List<TestRecord> records)
        {
            iterator = records.iterator();
        }

        final Iterator<TestRecord> iterator;
    }
}
