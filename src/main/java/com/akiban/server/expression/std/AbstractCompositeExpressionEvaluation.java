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

package com.akiban.server.expression.std;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.util.ValueHolder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractCompositeExpressionEvaluation extends ExpressionEvaluation.Base {

    // ExpressionEvaluation interface

    @Override
    public void of(Row row)
    {
        for (int i = 0; i < nChildren; i++) {
            children.get(i).of(row);
        }
    }

    @Override
    public void of(QueryContext context)
    {
        for (int i = 0; i < nChildren; i++) {
            children.get(i).of(context);
        }
        this.context = context;
    }

    @Override
    public void destroy()
    {
        for (int i = 0; i < nChildren; i++) {
            children.get(i).destroy();
        }
    }

    public AbstractCompositeExpressionEvaluation(List<? extends ExpressionEvaluation> children) {
        this.children = children.isEmpty()
                ? Collections.<ExpressionEvaluation>emptyList()
                : Collections.unmodifiableList(new ArrayList<ExpressionEvaluation>(children));
        this.nChildren = this.children.size();
    }

    // Shareable interface

    @Override
    public void acquire()
    {
        for (int i = 0; i < nChildren; i++) {
            children.get(i).acquire();
        }
    }

    @Override
    public boolean isShared()
    {
        for (int i = 0; i < nChildren; i++) {
            if (children.get(i).isShared()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void release() {
        for (int i = 0; i < nChildren; i++) {
            children.get(i).release();
        }
    }


    // for use by subclasses

    protected final List<? extends ExpressionEvaluation> children() {
        return children;
    }
    
    protected ValueHolder valueHolder() {
        return valueHolder == null ? valueHolder = new ValueHolder() : valueHolder ;
    }
    
    protected QueryContext queryContext() {
        return context;
    }
    
    // object state

    private final List<? extends ExpressionEvaluation> children;
    private final int nChildren;
    private ValueHolder valueHolder;
    protected QueryContext context;
}
