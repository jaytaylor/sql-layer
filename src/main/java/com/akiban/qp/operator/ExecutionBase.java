
package com.akiban.qp.operator;

import com.akiban.ais.model.UserTable;

public abstract class ExecutionBase
{
    protected StoreAdapter adapter()
    {
        return context.getStore();
    }

    protected StoreAdapter adapter(UserTable name)
    {
        return context.getStore(name);
    }

    protected void checkQueryCancelation()
    {
        context.checkQueryCancelation();
    }

    public ExecutionBase(QueryContext context)
    {
        this.context = context;
    }

    protected QueryContext context;

    protected static final boolean LOG_EXECUTION = false;
    protected static final boolean TAP_NEXT_ENABLED = false;
    protected static final boolean CURSOR_LIFECYCLE_ENABLED = false;
}
