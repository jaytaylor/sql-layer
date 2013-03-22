
package com.akiban.sql.optimizer.plan;

public abstract class FullTextQuery extends BasePlanElement
{
    public abstract boolean accept(ExpressionVisitor v);
    public abstract void accept(ExpressionRewriteVisitor v);
}
