
package com.akiban.sql.optimizer.plan;

public interface PlanWithInput extends PlanNode
{
    public void replaceInput(PlanNode oldInput, PlanNode newInput);
}
