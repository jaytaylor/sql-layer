
package com.akiban.sql.optimizer.plan;

/** A hierarchical visitor on expression tree. */
public interface PlanVisitor
{
    public boolean visitEnter(PlanNode n);
    public boolean visitLeave(PlanNode n);
    public boolean visit(PlanNode n);
}
