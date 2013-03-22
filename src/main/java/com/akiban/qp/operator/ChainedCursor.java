
package com.akiban.qp.operator;

import com.akiban.qp.row.Row;
import com.akiban.server.api.dml.ColumnSelector;

public class ChainedCursor extends OperatorExecutionBase implements Cursor
{
    protected final Cursor input;

    protected ChainedCursor(QueryContext context, Cursor input) {
        super(context);
        this.input = input;
    }

    @Override
    public void open() {
        input.open();
    }

    @Override
    public Row next()
    {
        return input.next();
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector)
    {
        input.jump(row, columnSelector);
    }

    @Override
    public void close() {
        input.close();
    }

    @Override
    public void destroy()
    {
        input.destroy();
    }

    @Override
    public boolean isIdle()
    {
        return input.isIdle();
    }

    @Override
    public boolean isActive()
    {
        return input.isActive();
    }

    @Override
    public boolean isDestroyed()
    {
        return input.isDestroyed();
    }
}
