
package com.akiban.sql.optimizer.plan;

/** The inside-tip of an anti join. */
public class OnlyIfEmpty extends BasePlanWithInput
{
    public OnlyIfEmpty(PlanNode input) {
        super(input);
    }

}
