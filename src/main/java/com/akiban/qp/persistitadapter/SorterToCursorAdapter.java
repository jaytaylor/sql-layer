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

package com.akiban.qp.persistitadapter;

import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.CursorLifecycle;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.sort.Sorter;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.util.tap.InOutTap;
import com.persistit.exception.PersistitException;

// Cursors are reusable but Sorters are not. This class creates a new Sorter each time a new cursor scan is started.

class SorterToCursorAdapter implements Cursor
{
    // Cursor interface

    @Override
    public void open()
    {
        CursorLifecycle.checkIdle(this);
        try {
            sorter = new Sorter(context, input, rowType, ordering, sortOption, loadTap, usePValues);
            cursor = sorter.sort();
            cursor.open();
        } catch (PersistitException e) {
            adapter.handlePersistitException(e);
        }
    }

    @Override
    public Row next()
    {
        CursorLifecycle.checkIdleOrActive(this);
        return cursor == null ? null : cursor.next();
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void close()
    {
        CursorLifecycle.checkIdleOrActive(this);
        if (cursor != null) {
            cursor.close();
            // Destroy here because Sorters can only be used once.
            cursor.destroy();
            cursor = null;
        }
        if (sorter != null) {
            sorter.close();
            sorter = null;
        }
    }

    @Override
    public void destroy()
    {
        close();
        destroyed = true;
    }

    @Override
    public boolean isIdle()
    {
        return !destroyed && cursor == null;
    }

    @Override
    public boolean isActive()
    {
        return !destroyed && cursor != null;
    }

    @Override
    public boolean isDestroyed()
    {
        return destroyed;
    }

    // SorterToCursorAdapter interface

    public SorterToCursorAdapter(PersistitAdapter adapter,
                                 QueryContext context,
                                 Cursor input,
                                 RowType rowType,
                                 API.Ordering ordering,
                                 API.SortOption sortOption,
                                 InOutTap loadTap,
                                 boolean usePValues)
    {
        this.adapter = adapter;
        this.context = context;
        this.input = input;
        this.rowType = rowType;
        this.ordering = ordering;
        this.sortOption = sortOption;
        this.loadTap = loadTap;
        this.usePValues = usePValues;
    }

    private final PersistitAdapter adapter;
    private final QueryContext context;
    private final Cursor input;
    private final RowType rowType;
    private final API.Ordering ordering;
    private final API.SortOption sortOption;
    private final InOutTap loadTap;
    private final boolean usePValues;
    private Sorter sorter;
    private Cursor cursor;
    private boolean destroyed = false;
}
