/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer.plan;

/** A union of two subqueries. */
public class Union extends BasePlanNode implements PlanWithInput
{
    private PlanNode left, right;
    private boolean all;

    public Union(PlanNode left, PlanNode right, boolean all) {
        this.left = left;
        left.setOutput(this);
        this.right = right;
        right.setOutput(this);
        this.all = all;
    }

    public PlanNode getLeft() {
        return left;
    }
    public void setLeft(PlanNode left) {
        this.left = left;
        left.setOutput(this);
    }
    public PlanNode getRight() {
        return right;
    }
    public void setRight(PlanNode right) {
        this.right = right;
        right.setOutput(this);
    }

    public boolean isAll() {
        return all;
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        if (left == oldInput)
            left = newInput;
        if (right == oldInput)
            right = newInput;
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (left.accept(v))
                right.accept(v);
        }
        return v.visitLeave(this);
    }
    
    @Override
    public String summaryString() {
        if (all)
            return super.summaryString() + "(ALL)";
        else
            return super.summaryString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        left = (PlanNode)left.duplicate(map);
        right = (PlanNode)right.duplicate(map);
    }

}
