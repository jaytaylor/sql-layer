package com.foundationdb.sql.optimizer.plan;

public interface KnownValueExpression extends ExpressionNode {
    public Object getValue();
}
