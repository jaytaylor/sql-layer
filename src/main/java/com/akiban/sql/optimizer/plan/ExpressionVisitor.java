
package com.akiban.sql.optimizer.plan;

/** A hierarchical visitor on expression tree. */
public interface ExpressionVisitor
{
    public boolean visitEnter(ExpressionNode n);
    public boolean visitLeave(ExpressionNode n);
    public boolean visit(ExpressionNode n);
}
