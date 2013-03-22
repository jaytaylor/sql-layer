
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
                : Collections.unmodifiableList(new ArrayList<>(children));
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
    private QueryContext context;
}
