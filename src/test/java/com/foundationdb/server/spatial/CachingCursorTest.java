package com.foundationdb.server.spatial;

import com.foundationdb.qp.operator.CursorBase;
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
        CachingCursor<TestRecord> cursor = cachingCursor(0);
        assertNull(cursor.next());
        cursor.goTo(START);
        assertNull(cursor.next());
    }

    @Test
    public void test1() throws IOException, InterruptedException
    {
        CachingCursor<TestRecord> cursor = cachingCursor(1);
        TestRecord record;
        // Get first record
        record = cursor.next();
        assertNotNull(record);
        assertEquals(0, record.id);
        // Reset and get it again
        cursor.goTo(START);
        record = cursor.next();
        assertNotNull(record);
        assertEquals(0, record.id);
        // There should be nothing else
        assertNull(cursor.next());
        // Reset should fail
        try {
            cursor.goTo(START);
            fail();
        } catch (CachingCursor.NotResettableException e) {
            // Expected
        }
    }

    @Test
    public void testMoreThanOne() throws IOException, InterruptedException
    {
        for (int n = 2; n < 10; n++) {
            CachingCursor<TestRecord> cursor = cachingCursor(n);
            TestRecord record;
            // Get first record
            record = cursor.next();
            assertNotNull(record);
            assertEquals(0, record.id);
            // Reset and get it again
            cursor.goTo(START);
            record = cursor.next();
            assertNotNull(record);
            assertEquals(0, record.id);
            // Get the next record
            record = cursor.next();
            assertNotNull(record);
            assertEquals(1, record.id);
            // Reset should not work
            try {
                cursor.goTo(START);
                fail();
            } catch (CachingCursor.NotResettableException e) {
                // Expected
            }
        }
    }

    @Test
    public void testBadReset() throws IOException, InterruptedException
    {
        CachingCursor<TestRecord> cursor = cachingCursor(1);
        TestRecord record;
        // Get first record
        record = cursor.next();
        assertNotNull(record);
        assertEquals(0, record.id);
        // Reset and get it again
        TestRecord notStart = new TestRecord(0);
        notStart.z(Z + 1);
        try {
            cursor.goTo(notStart);
            fail();
        } catch (CachingCursor.NotResettableException e) {
            // Expected
        }
    }

    private CachingCursor<TestRecord> cachingCursor(int n) throws IOException, InterruptedException
    {
        TestIndex index = new TestIndex();
        for (int id = 0; id < n; id++) {
            index.add(new TestRecord(id));
        }
        TestInputCursor inputCursor = new TestInputCursor(index.records);
        return new CachingCursor<>(index, Z, inputCursor);
    }

    private static final long Z = 0;
    private static final TestRecord START = new TestRecord(0);
    static
    {
        START.z(Z);
    }

    private static class TestRecord implements Record
    {
        @Override
        public int hashCode()
        {
            fail();
            return 0;
        }

        @Override
        public boolean equals(Object obj)
        {
            return obj != null && id == ((TestRecord) obj).id;
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
            id = ((TestRecord) record).id;
        }

        public TestRecord(int id)
        {
            this.id = id;
        }

        int id;
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