/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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

import com.geophile.z.Cursor;
import com.geophile.z.DuplicateRecordException;
import com.geophile.z.Index;

import java.util.Iterator;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TreeIndex implements the {@link com.geophile.z.Index} interface in terms of a {@link java.util.TreeMap}.
 * A TreeIndex is not safe for use for simultaneous use by multiple threads.
 */

public class TreeIndex extends Index<TestRecord>
{
    // Object interface

    @Override
    public String toString()
    {
        return name;
    }

    // Index interface

    @Override
    public void add(TestRecord TestRecord)
    {
        TestRecord copy = newRecord();
        TestRecord.copyTo(copy);
        boolean added = tree.add(copy);
        if (!added) {
            throw new DuplicateRecordException(copy);
        }
    }

    @Override
    public boolean remove(long z, TestRecord.Filter<TestRecord> filter)
    {
        boolean foundTestRecord = false;
        boolean zMatch = true;
        Iterator<TestRecord> iterator = tree.tailSet(key(z)).iterator();
        while (zMatch && iterator.hasNext() && !foundTestRecord) {
            TestRecord TestRecord = iterator.next();
            if (TestRecord.z() == z) {
                foundTestRecord = filter.select(TestRecord);
            } else {
                zMatch = false;
            }
        }
        if (foundTestRecord) {
            iterator.remove();
        }
        return foundTestRecord;
    }

    @Override
    public Cursor<TestRecord> cursor()
    {
        return new TreeIndexCursor(this);
    }

    @Override
    public TestRecord newRecord()
    {
        return new TestRecord();
    }

    @Override
    public boolean blindUpdates()
    {
        return false;
    }

    @Override
    public boolean stableRecords()
    {
        return false;
    }

    // TreeIndex

    public TreeIndex()
    {
        this.tree = new TreeSet<>(TestRecord.COMPARATOR);
    }

    // For use by this package

    TreeSet<TestRecord> tree()
    {
        return tree;
    }

    // For use by this class

    private TestRecord key(long z)
    {
        TestRecord keyTestRecord = newRecord();
        keyTestRecord.z(z);
        return keyTestRecord;
    }

    // Class state

    private static final AtomicInteger idGenerator = new AtomicInteger(0);

    // Object state

    private final String name = String.format("TreeIndex(%s)", idGenerator.getAndIncrement());
    private final TreeSet<TestRecord> tree;
}
