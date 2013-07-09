/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.qp.util;

import com.akiban.qp.operator.BindingsAwareCursor;
import com.akiban.qp.operator.QueryBindings;
import com.akiban.qp.operator.RowCursor;
import com.akiban.qp.row.Row;
import com.akiban.server.api.dml.ColumnSelector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MultiCursor implements BindingsAwareCursor
{
    // Cursor interface

    @Override
    public void open()
    {
        sealed = false;
        // TODO: Have a mode where all the cursors get opened so that
        // they can start in parallel.
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
    public void jump(Row row, ColumnSelector columnSelector)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
        if (current != null) {
            current.close();
        }
        while (cursorIterator.hasNext()) {
            current = cursorIterator.next();
            current.close();
        }
        current = null;
    }

    @Override
    public void destroy()
    {
        cursorIterator = null;
    }

    @Override
    public boolean isIdle()
    {
        return !isDestroyed() && current == null;
    }

    @Override
    public boolean isActive()
    {
        return !isDestroyed() && current != null;
    }

    @Override
    public boolean isDestroyed()
    {
        return cursorIterator == null;
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
            current.open();
        } else {
            current = null;
        }
    }

    // Object state

    private final List<RowCursor> cursors = new ArrayList<>();
    private boolean sealed = false;
    private Iterator<RowCursor> cursorIterator;
    private RowCursor current;

}
