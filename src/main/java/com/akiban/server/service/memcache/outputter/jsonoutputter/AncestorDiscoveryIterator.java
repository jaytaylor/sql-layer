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

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.UserTable;
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
            // Find the hkey segment at which the current row differs from the previous. This is needed to
            // fill in missing ancestors of orphan rows later. The input iterator provides rows in index order
            // for the predicate, with each row of the predicate type followed by hkey ordered descendents.
            // The divergence position is only relevant for the hkey-ordered runs of the input. So for rows
            // shallower than the predicate table, set divergence position to 0.
            int divergencePosition =
                ais.getUserTable(current.getRowDefId()).getDepth() < predicateTableDepth || previous == null
                ? 0
                : current.hKey().firstUniqueSegmentDepth(previous.hKey());
            current.differsFromPredecessorAtKeySegment(divergencePosition);
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

    AncestorDiscoveryIterator(UserTable predicateTable, Iterator<RowData> input)
    {
        this.ais = predicateTable.getAIS();
        this.predicateTableDepth = predicateTable.getDepth();
        this.input = input;
        if (input.hasNext()) {
            this.current = input.next();
        }
    }

    // Object state

    private final AkibanInformationSchema ais;
    private final int predicateTableDepth;
    private final Iterator<RowData> input;
    private RowData previous;
    private RowData current;
}
