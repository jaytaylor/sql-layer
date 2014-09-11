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

public abstract class OperatorExecutionBase extends ExecutionBase implements RowOrientedCursorBase<Row>
{
    @Override
    public boolean isIdle()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public boolean isActive()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public boolean isClosed() 
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void open()
    {
        throw new UnsupportedOperationException(getClass().getName());
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
        throw new UnsupportedOperationException(getClass().getName());
    }

    @Override
    public void setIdle() 
    {
        throw new UnsupportedOperationException(getClass().getName());
    }
    
    protected void checkQueryCancelation()
    {
        try {
            context.checkQueryCancelation();
        } catch (QueryCanceledException e) {
            close();
            throw e;
        }
    }

    protected OperatorExecutionBase(QueryContext context)
    {
        super(context);
    }
}
