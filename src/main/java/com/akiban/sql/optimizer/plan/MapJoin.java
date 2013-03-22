
package com.akiban.sql.optimizer.plan;

import com.akiban.sql.optimizer.plan.JoinNode.JoinType;

import java.util.*;

/** A join implementation using Map. */
public class MapJoin extends BasePlanNode implements PlanWithInput
{
    // This is non-null only until the map has been folded.
    private JoinType joinType;
    private PlanNode outer, inner;

    public MapJoin(JoinType joinType, PlanNode outer, PlanNode inner) {
        this.joinType = joinType;
        this.outer = outer;
        outer.setOutput(this);
        this.inner = inner;
        inner.setOutput(this);
    }

    public JoinType getJoinType() {
        return joinType;
    }
    public void setJoinType(JoinType joinType) {
        this.joinType = joinType;
    }

    public PlanNode getOuter() {
        return outer;
    }
    public void setOuter(PlanNode outer) {
        this.outer = outer;
        outer.setOutput(this);
    }
    public PlanNode getInner() {
        return inner;
    }
    public void setInner(PlanNode inner) {
        this.inner = inner;
        inner.setOutput(this);
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        if (outer == oldInput) {
            outer = newInput;
            outer.setOutput(this);
        }
        if (inner == oldInput) {
            inner = newInput;
            inner.setOutput(this);
        }
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (outer.accept(v))
                inner.accept(v);
        }
        return v.visitLeave(this);
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        if (joinType != null) {
            str.append(joinType);
        }
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        outer = (PlanNode)outer.duplicate(map);
        inner = (PlanNode)inner.duplicate(map);
    }

}
