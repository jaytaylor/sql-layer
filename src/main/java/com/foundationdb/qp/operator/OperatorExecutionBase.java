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
import com.foundationdb.server.error.QueryCanceledException;

/**
 *  Abstract implementation of RowCursor. 
 *  
 *  Implements the state checking for the CursorBase, but not any of the
 *  operations methods. If you change this, also change @See RowCursorImpl, 
 *  as the two set of state implementations should match. 
 *
 */

public abstract class OperatorExecutionBase extends ExecutionBase implements RowCursor
{
    @Override
    public void open() {
        CursorLifecycle.checkClosed(this);
        state = CursorLifecycle.CursorState.ACTIVE;
    }

    @Override
    public Row next()
    {
        throw new UnsupportedOperationException(getClass().getName());
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
        state = CursorLifecycle.CursorState.CLOSED;
    }

    @Override
    public void setIdle() {
        state = CursorLifecycle.CursorState.IDLE;
    }

    @Override
    public boolean isIdle()
    {
        return state == CursorLifecycle.CursorState.IDLE;
    }

    @Override
    public boolean isActive()
    {
        return state == CursorLifecycle.CursorState.ACTIVE;
    }

    @Override
    public boolean isClosed()
    {
        return state == CursorLifecycle.CursorState.CLOSED;
    }

    
    protected void checkQueryCancelation()
    {
        try {
            context.checkQueryCancelation();
        } catch (QueryCanceledException e) {
            //close();
            throw e;
        }
    }

    protected OperatorExecutionBase(QueryContext context)
    {
        super(context);
    }
    
    protected CursorLifecycle.CursorState state = CursorLifecycle.CursorState.CLOSED;

}
