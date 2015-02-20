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

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

public class TreeIndexCursor extends Cursor<TestRecord>
{
    // Cursor interface

    @Override
    public TestRecord next() throws IOException, InterruptedException
    {
        return neighbor();
    }

    @Override
    public void goTo(TestRecord key)
    {
        this.startAt = key;
        state(State.NEVER_USED);
    }

    @Override
    public boolean deleteCurrent() throws IOException, InterruptedException
    {
        boolean removed = false;
        if (state() == State.IN_USE) {
            treeIterator.remove();
            removed = true;
        }
        return removed;
    }

    // TreeIndexCursor interface

    public TreeIndexCursor(TreeIndex treeIndex)
    {
        super(treeIndex);
        this.tree = treeIndex.tree();
    }

    // For use by this class

    private TestRecord neighbor() throws IOException, InterruptedException
    {
        switch (state()) {
            case NEVER_USED:
                startIteration(true);
                break;
            case IN_USE:
                break;
            case DONE:
                assert current() == null;
                return null;
        }
        if (treeIterator.hasNext()) {
            TestRecord neighbor = treeIterator.next();
            current(neighbor);
            startAt = neighbor;
            state(State.IN_USE);
        } else {
            close();
        }
        return current();
    }

    private void startIteration(boolean includeStartKey)
    {
        treeIterator = tree.tailSet(startAt, includeStartKey).iterator();
    }

    // Object state

    private final TreeSet<TestRecord> tree;
    private TestRecord startAt;
    private Iterator<TestRecord> treeIterator;
}
