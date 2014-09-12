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

package com.foundationdb.qp.operator;

import com.foundationdb.qp.row.Row;
import com.foundationdb.server.api.dml.ColumnSelector;

import java.util.ArrayDeque;
import java.util.Queue;

/** An {@link OperatorCursor} that opens a single {@link BindingsAwareCursor}
* for each {@link QueryBindings} with lookahead.
*/
public abstract class LookaheadLeafCursor<C extends BindingsAwareCursor> extends OperatorCursor
{
    // Cursor interface

    @Override
    public void open() {
        super.open();
        if (currentCursor != null) {
            currentCursor.open();
        }
        else if (pendingCursor != null) {
            currentCursor = pendingCursor;
            pendingCursor = null;
        }
        else {
            // At the very beginning, the pipeline isn't started.
            currentCursor = openACursor(currentBindings, false);
        }
        while (!cursorPool.isEmpty() && !bindingsExhausted) {
            QueryBindings bindings = bindingsCursor.nextBindings();
            if (bindings == null) {
                bindingsExhausted = true;
                break;
            }
            C cursor = null;
            if (bindings.getDepth() == currentBindings.getDepth()) {
                cursor = openACursor(bindings, true);
            }
            pendingBindings.add(new BindingsAndCursor<C>(bindings, cursor));
        }
    }

    @Override
    public Row next() {
        if (CURSOR_LIFECYCLE_ENABLED) {
            CursorLifecycle.checkIdleOrActive(this);
        }
        checkQueryCancelation();
        Row row = currentCursor.next();
        if (row == null) {
            currentCursor.setIdle();
        }
        return row;
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector) {
        currentCursor.jump(row, columnSelector);
    }

    @Override
    public void close() {
        if (currentCursor != null) {
            currentCursor.close();
        }
        super.close();
    }

    @Override
    public boolean isIdle() {
        return ((currentCursor != null) && currentCursor.isIdle());
    }

    @Override
    public boolean isActive() {
        return ((currentCursor != null) && currentCursor.isActive());
    }
    
    @Override
    public boolean isClosed() {
        return ((currentCursor != null) && currentCursor.isClosed());
    }

    @Override
    public void openBindings() {
        recyclePending();
        bindingsCursor.openBindings();
        bindingsExhausted = false;
        currentCursor = pendingCursor = null;
    }

    @Override
    public QueryBindings nextBindings() {
        if (currentCursor != null) {
            currentCursor.close();
            cursorPool.add(currentCursor);
            currentCursor = null;
        }
        if (pendingCursor != null) {
            pendingCursor.close(); // Abandoning lookahead.
            cursorPool.add(pendingCursor);
            pendingCursor = null;
        }
        BindingsAndCursor<C> bandc = pendingBindings.poll();
        if (bandc != null) {
            currentBindings = bandc.bindings;
            pendingCursor = bandc.cursor;
        } else {
            currentBindings = bindingsCursor.nextBindings();
            if (currentBindings == null) {
                bindingsExhausted = true;
            }
        }
        return currentBindings;
    }

    @Override
    public void closeBindings() {
        bindingsCursor.closeBindings();
        recyclePending();
    }

    @Override
    public void cancelBindings(QueryBindings bindings) {
        CursorLifecycle.checkClosed(this);
        
        if ((currentBindings != null) && currentBindings.isAncestor(bindings)) {
            if (currentCursor != null) {
                currentCursor.close();
                cursorPool.add(currentCursor);
                currentCursor = null;
            }
            if (pendingCursor != null) {
                pendingCursor.close();
                cursorPool.add(pendingCursor);
                pendingCursor = null;
            }
            currentBindings = null;
        }
        while (true) {
            BindingsAndCursor<C> bandc = pendingBindings.peek();
            if (bandc == null) break;
            if (!bandc.bindings.isAncestor(bindings)) break;
            bandc = pendingBindings.remove();
            if (bandc.cursor != null) {
                bandc.cursor.close();
                cursorPool.add(bandc.cursor);
            }
        }
        bindingsCursor.cancelBindings(bindings);
    }

    // LookaheadLeafCursor interface

    LookaheadLeafCursor(QueryContext context, QueryBindingsCursor bindingsCursor, 
                        StoreAdapter adapter, int quantum) {
        super(context);
        this.bindingsCursor = bindingsCursor;
        this.pendingBindings = new ArrayDeque<>(quantum+1);
        this.cursorPool = new ArrayDeque<>(quantum);
        for (int i = 0; i < quantum; i++) {
            C cursor = newCursor(context, adapter);
            cursorPool.add(cursor);
        }
    }

    // Implemented by subclass

    protected abstract C newCursor(QueryContext context, StoreAdapter adapter);

    // Inner classes

    protected static final class BindingsAndCursor<C extends BindingsAwareCursor> {
        QueryBindings bindings;
        C cursor;
            
        BindingsAndCursor(QueryBindings bindings, C cursor) {
            this.bindings = bindings;
            this.cursor = cursor;
        }
    }

    // For use by this class

    protected void recyclePending() {
        while (true) {
            BindingsAndCursor<C> bandc = pendingBindings.poll();
            if (bandc == null) break;
            if (bandc.cursor != null) {
                bandc.cursor.close();
                cursorPool.add(bandc.cursor);
            }
        }
    }

    protected C openACursor(QueryBindings bindings, boolean lookahead) {
        C cursor = cursorPool.remove();
        cursor.rebind(bindings);
        cursor.open();
        return cursor;
    }

    // Object state

    protected final QueryBindingsCursor bindingsCursor;
    protected final Queue<BindingsAndCursor<C>> pendingBindings;
    protected final Queue<C> cursorPool;
    protected QueryBindings currentBindings;
    protected C pendingCursor, currentCursor;
    protected boolean bindingsExhausted;// destroyed;
}
