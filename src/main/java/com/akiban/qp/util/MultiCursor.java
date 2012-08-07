/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
        current = cursorIterator.hasNext() ? cursorIterator.next() : null;
    }

    @Override
    public Row next()
    {
        Row next;
        do {
            next = current != null ? current.next() : null;
            if (next == null) {
                current = cursorIterator.hasNext() ? cursorIterator.next() : null;
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

    // Object state

    private final List<Cursor> cursors = new ArrayList<Cursor>();
    private boolean sealed = false;
    private Iterator<Cursor> cursorIterator;
    private Cursor current;

}
