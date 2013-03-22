
package com.akiban.sql.optimizer.plan;

/** The inside-tip of an outer join (can move from there). */
public class NullIfEmpty extends BasePlanWithInput
{
    public NullIfEmpty(PlanNode input) {
        super(input);
    }

}
