
package com.akiban.server.expression.std;

import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.row.Row;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.util.ValueHolder;

public abstract class AbstractUnaryExpressionEvaluation extends ExpressionEvaluation.Base
{
    @Override
    public void of(Row row) {
        operandEvaluation.of(row);
    }

    @Override
    public void of(QueryContext context) {
        operandEvaluation.of(context);
        this.context = context;
    }

    @Override
    public void destroy() {
        operandEvaluation.destroy();
    }

    @Override
    public void acquire() {
        operandEvaluation.acquire();
    }

    @Override
    public boolean isShared() {
        return operandEvaluation.isShared();
    }

    @Override
    public void release() {
        operandEvaluation.release();
    }

    // for use by subclasses
    protected final ValueSource operand() {
        return operandEvaluation.eval();
    }

    protected final ExpressionEvaluation operandEvaluation() {
        return operandEvaluation;
    }

    protected AbstractUnaryExpressionEvaluation(ExpressionEvaluation operandEvaluation) {
        this.operandEvaluation = operandEvaluation;
    }

    protected ValueHolder valueHolder(){
        return valueHolder == null ? valueHolder = new ValueHolder() : valueHolder;
    }
    
    protected QueryContext queryContext() {
        return context;
    }
    
    // object state
    private final ExpressionEvaluation operandEvaluation;
    private ValueHolder valueHolder;
    private QueryContext context;
}
