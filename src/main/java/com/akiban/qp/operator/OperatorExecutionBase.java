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

import com.akiban.ais.model.UserTable;
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
