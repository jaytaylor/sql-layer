
package com.akiban.qp.operator;

import com.akiban.qp.row.Row;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.error.QueryCanceledException;

public abstract class OperatorExecutionBase extends ExecutionBase implements RowOrientedCursorBase<Row>
{
    @Override
    public void destroy()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

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
    public boolean isDestroyed()
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
