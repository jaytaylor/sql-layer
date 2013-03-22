
package com.akiban.sql.optimizer.plan;

public abstract class BasePlanNode extends BasePlanElement implements PlanNode
{
    private PlanWithInput output;

    protected BasePlanNode() {
    }

    @Override
    public PlanWithInput getOutput() {
        return output;
    }

    @Override
    public void setOutput(PlanWithInput output) {
        this.output = output;
    }

    @Override
    public String summaryString() {
        return getClass().getSimpleName() + "@" + Integer.toString(hashCode(), 16);
    }

    @Override
    public String toString() {
        return PlanToString.of(this);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        // Do not copy output or put it in the map: rely on copying
        // input to set back pointer.
    }

}
