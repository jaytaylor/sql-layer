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

package com.akiban.qp.loadableplan;

import com.akiban.qp.operator.CursorBase;
import java.util.List;

/** A cursor that returns column values directly.
 * Return columns from <code>next</code>. If an empty list is
 * returned, any buffered rows will be flushed and <code>next</code>
 * will be called again. If <code>null</code> is returned, the cursor
 * is exhausted and will be closed.
 */
public abstract class DirectObjectCursor implements CursorBase<List<?>>
{
    // These cursors are used outside of execution plans. These methods should not be called.

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
}
