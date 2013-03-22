
package com.akiban.sql.optimizer.plan;

/** A visitor that can rewrite portions of the expression as it goes. */
public interface ExpressionRewriteVisitor
{
    /** Return a replacement for this node (or the node itself). */
    public ExpressionNode visit(ExpressionNode n);
    /** Return <code>true</code> to visit the children first, <code>false</code> the node first. */
    public boolean visitChildrenFirst(ExpressionNode n);
}
