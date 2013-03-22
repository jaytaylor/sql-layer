
package com.akiban.sql.optimizer.plan;

public abstract class BaseScan extends BasePlanNode
{
    // Estimated cost of using this scan.
    private CostEstimate costEstimate;

    public CostEstimate getCostEstimate() {
        return costEstimate;
    }
    public void setCostEstimate(CostEstimate costEstimate) {
        this.costEstimate = costEstimate;
    }

}
