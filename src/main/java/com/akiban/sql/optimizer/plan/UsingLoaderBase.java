
package com.akiban.sql.optimizer.plan;

/** A context with some kind of loaded object. */
public abstract class UsingLoaderBase extends BasePlanWithInput
{
    private PlanNode loader;

    public UsingLoaderBase(PlanNode loader, PlanNode input) {
        super(input);
        this.loader = loader;
        loader.setOutput(this);
    }

    public PlanNode getLoader() {
        return loader;
    }
    public void setLoader(PlanNode loader) {
        this.loader = loader;
        loader.setOutput(this);
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        super.replaceInput(oldInput, newInput);
        if (loader == oldInput) {
            loader = newInput;
            loader.setOutput(this);
        }
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            loader.accept(v);
            getInput().accept(v);
        }
        return v.visitLeave(this);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        loader = (PlanNode)loader.duplicate(map);
    }

}
