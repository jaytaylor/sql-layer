
package com.akiban.sql.optimizer.plan;

/** A node in the tree of tables and their joins. */
public interface Joinable extends PlanNode
{
    public boolean isTable();
    public boolean isGroup();
    public boolean isJoin();
    public boolean isInnerJoin();
}
