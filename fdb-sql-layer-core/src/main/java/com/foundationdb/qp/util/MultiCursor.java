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

package com.foundationdb.qp.util;

import com.foundationdb.qp.operator.BindingsAwareCursor;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.RowCursorImpl;
import com.foundationdb.qp.row.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MultiCursor extends RowCursorImpl implements BindingsAwareCursor
{
    // Cursor interface

    @Override
    public void open()
    {
        super.open();
        sealed = true;
        if (openAll) {
            for (RowCursor cursor : cursors) {
                cursor.open();
            }
        }
        cursorIterator = cursors.iterator();
        startNextCursor();
    }

    @Override
    public Row next()
    {
        Row next;
        do {
            next = current != null ? current.next() : null;
            if (next == null) {
                startNextCursor();
            }
        } while (next == null && current != null);
        return next;
    }


    @Override
    public void close()
    {
        try {
            if (current != null) {
                current.close();
            }
            while (cursorIterator.hasNext()) {
                current = cursorIterator.next();
                current.close();
            }
        } finally {
            current = null;
            super.close();
        }
    }

    @Override
    public void rebind(QueryBindings bindings)
    {
        for (RowCursor cursor : cursors) {
            if (cursor instanceof BindingsAwareCursor) {
                ((BindingsAwareCursor)cursor).rebind(bindings);
            }
        }
    }

    // MultiCursor interface

    public MultiCursor() {
        this(false);
    }

    public MultiCursor(boolean openAll) {
        this.openAll = openAll;
    }

    public void addCursor(RowCursor cursor)
    {
        if (sealed) {
            throw new IllegalStateException();
        }
        cursors.add(cursor);
    }

    // For use by this class

    private void startNextCursor()
    {
        if (cursorIterator.hasNext()) {
            if (current != null) {
                current.close();
            }
            current = cursorIterator.next();
            if (!openAll) {
                current.open();
            }
        } else {
            current = null;
            setIdle();
        }
    }

    // Object state

    private final List<RowCursor> cursors = new ArrayList<>();
    private final boolean openAll;
    private boolean sealed = false;
    private Iterator<RowCursor> cursorIterator;
    private RowCursor current;

}
