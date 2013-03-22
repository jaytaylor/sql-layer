
package com.akiban.sql.optimizer.plan;

/** A node that has referencable columns. */
public interface ColumnSource extends PlanNode
{
    public String getName();
}
