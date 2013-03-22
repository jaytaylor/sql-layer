
package com.akiban.sql.optimizer.plan;

import java.util.List;

/** A product type join among several subplans. */
public class Product extends BasePlanNode implements PlanWithInput
{
    private TableNode ancestor;
    private List<PlanNode> subplans;

    public Product(TableNode ancestor, List<PlanNode> subplans) {
        this.ancestor = ancestor;
        this.subplans = subplans;
        for (PlanNode subplan : subplans)
          subplan.setOutput(this);
    }

    public TableNode getAncestor() {
        return ancestor;
    }

    public List<PlanNode> getSubplans() {
        return subplans;
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        int index = subplans.indexOf(oldInput);
        if (index >= 0) {
            subplans.set(index, newInput);
            newInput.setOutput(this);
        }
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            for (PlanNode subplan : subplans) {
                if (!subplan.accept(v))
                    break;
            }
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        if (ancestor != null) {
            str.append("(").append(ancestor).append(")");
        }
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        subplans = duplicateList(subplans, map);
    }

}
