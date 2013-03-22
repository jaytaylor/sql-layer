
package com.akiban.server.expression.std;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.util.ValueHolder;

public abstract class AbstractNoArgExpressionEvaluation extends ExpressionEvaluation.Base {
    @Override
    public void of(Row row) {
    }

    @Override
    public void of(QueryContext context) {
    }

    @Override
    public void acquire() {
    }

    @Override
    public boolean isShared() {
        return false;
    }

    @Override
    public void release() {
    }

    // for use by subclasses

    protected AbstractNoArgExpressionEvaluation() {
    }

    protected ValueHolder valueHolder () {
        return valueHolder == null ? valueHolder = new ValueHolder() : valueHolder;
    }

    private ValueHolder valueHolder;
}
