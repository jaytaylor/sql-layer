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

package com.akiban.server.service.memcache.outputter.jsonoutputter;

import com.akiban.server.RowData;

import java.util.Iterator;

public class AncestorDiscoveryIterator implements Iterator<RowData>
{
    // Iterator interface

    @Override
    public boolean hasNext()
    {
        return current != null;
    }

    @Override
    public RowData next()
    {
        if (current != null) {
            current.differsFromPredecessorAtKeySegment(
                previous == null
                ? 0
                : current.hKey().firstUniqueSegmentDepth(previous.hKey()));
            previous = current;
            current = input.hasNext() ? input.next() : null;
        } else {
            previous = null;
        }
        return previous;
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    // AncestorDiscoveryIterator interface

    AncestorDiscoveryIterator(Iterator<RowData> input)
    {
        this.input = input;
        if (input.hasNext()) {
            current = input.next();
        }
    }

    // Object state

    private final Iterator<RowData> input;
    private RowData previous;
    private RowData current;
}
