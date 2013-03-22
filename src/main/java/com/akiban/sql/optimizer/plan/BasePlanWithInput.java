
package com.akiban.sql.optimizer.plan;

public abstract class BasePlanWithInput extends BasePlanNode implements PlanWithInput
{
    private PlanNode input;

    protected BasePlanWithInput(PlanNode input) {
        this.input = input;
        if (input != null)
            input.setOutput(this);
    }

    public PlanNode getInput() {
        return input;
    }
    public void setInput(PlanNode input) {
        this.input = input;
        input.setOutput(this);
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        if (input == oldInput) {
            input = newInput;
            input.setOutput(this);
        }
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (input != null)
                input.accept(v);
        }
        return v.visitLeave(this);
    }
    
    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        if (input != null)
            setInput((PlanNode)input.duplicate(map)); // Which takes care of setting input's output.
    }

}
