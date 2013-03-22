
package com.akiban.sql.optimizer.plan;

/** Make results distinct. */
public class Distinct extends BasePlanWithInput
{
    public static enum Implementation {
        PRESORTED, SORT, HASH, TREE, EXPLICIT_SORT
    }

    private Implementation implementation;

    public Distinct(PlanNode input) {
        super(input);
    }

    public Distinct(PlanNode input, Implementation implementation) {
        super(input);
        this.implementation = implementation;
    }

    public Implementation getImplementation() {
        return implementation;
    }
    public void setImplementation(Implementation implementation) {
        this.implementation = implementation;
    }

}
