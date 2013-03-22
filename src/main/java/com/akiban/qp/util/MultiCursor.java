
package com.akiban.qp.util;

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.row.Row;
import com.akiban.server.api.dml.ColumnSelector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MultiCursor implements Cursor
{
    // Cursor interface

    @Override
    public void open()
    {
        sealed = false;
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

    // MultiCursor interface

    public void addCursor(Cursor cursor)
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

    private final List<Cursor> cursors = new ArrayList<>();
    private boolean sealed = false;
    private Iterator<Cursor> cursorIterator;
    private Cursor current;

}
