package com.foundationdb.server.util;

import com.foundationdb.qp.operator.RowCursorImpl;
import com.foundationdb.qp.row.Row;

import java.util.Iterator;

public class IteratorToCursorAdapter extends RowCursorImpl
{
    // Cursor interface

    @Override
    public Row next()
    {
        return iterator.hasNext() ? (Row) iterator.next() : null;
    }


    // IteratorToCursorAdapter interface

    public IteratorToCursorAdapter(Iterator iterator)
    {
        this.iterator = iterator;
    }

    // Object state

    private final Iterator iterator;

}
