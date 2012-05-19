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

package com.akiban.qp.operator;

import com.akiban.ais.model.TableName;
import com.akiban.qp.row.Row;
import com.akiban.server.error.QueryCanceledException;

public abstract class OperatorExecutionBase /* TODO: Temporary */ implements CursorBase<Row>
{
    // TODO: Implementation of new cursor methods is temporary, so that I don't have to add them to
    // TODO: all Cursor implementations before testing any of them.


    @Override
    public void destroy()
    {
        assert false : this;
    }

    @Override
    public boolean isIdle()
    {
        assert false : this;
        return false;
    }

    @Override
    public boolean isActive()
    {
        assert false : this;
        return false;
    }

    @Override
    public boolean isDestroyed()
    {
        assert false : this;
        return false;
    }

    @Override
    public void open()
    {
        assert false : this;
    }

    @Override
    public Row next()
    {
        assert false : this;
        return null;
    }

    @Override
    public void close()
    {
        assert false : this;
    }

    // Operators that implement cursors have a context at construction time
    protected OperatorExecutionBase(QueryContext context)
    {
        this.context = context;
    }

    // Update operators don't get the context until later
    protected OperatorExecutionBase()
    {
    }

    protected void context(QueryContext context)
    {
        this.context = context;
    }

    protected void checkQueryCancelation()
    {
        try {
            adapter().checkQueryCancelation(context.getStartTime());
        } catch (QueryCanceledException e) {
            close();
            throw e;
        }
    }

    protected StoreAdapter adapter() {
        return context.getStore();
    }
    
    protected StoreAdapter adapter (TableName name) {
        return context.getStore(name);
    }

    protected QueryContext context;
}
