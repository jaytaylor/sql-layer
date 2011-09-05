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

import com.akiban.ais.model.Join;
import com.akiban.qp.physicaloperator.API.JoinType;

import java.util.*;

/** A join between two tables / subjoins. */
public class JoinNode extends BaseJoinable implements PlanWithInput
{
    private Joinable left, right;
    private JoinType joinType;
    private List<ConditionExpression> joinConditions;
    private Join groupJoin;
    private ConditionExpression groupJoinCondition;

    public JoinNode(Joinable left, Joinable right, JoinType joinType) {
        this.left = left;
        left.setOutput(this);
        this.right = right;
        right.setOutput(this);
        this.joinType = joinType;
    }

    public Joinable getLeft() {
        return left;
    }
    public void setLeft(Joinable left) {
        this.left = left;
        left.setOutput(this);
    }
    public Joinable getRight() {
        return right;
    }
    public void setRight(Joinable right) {
        this.right = right;
        right.setOutput(this);
    }

    public JoinType getJoinType() {
        return joinType;
    }

    @Override
    public boolean isJoin() {
        return true;
    }

    @Override
    public boolean isInnerJoin() {
        return (joinType == JoinType.INNER_JOIN);
    }

    public List<ConditionExpression> getJoinConditions() {
        return joinConditions;
    }
    public void setJoinConditions(List<ConditionExpression> joinConditions) {
        this.joinConditions = joinConditions;
    }

    public Join getGroupJoin() {
        return groupJoin;
    }
    public void setGroupJoin(Join groupJoin) {
        this.groupJoin = groupJoin;
    }

    public ConditionExpression getGroupJoinCondition() {
        return groupJoinCondition;
    }
    public void setGroupJoinCondition(ConditionExpression groupJoinCondition) {
        this.groupJoinCondition = groupJoinCondition;
    }

    /** Reverse operands and outer join direction if necessary. */
    public void reverse() {
        Joinable temp = left;
        left = right;
        right = temp;
        switch (joinType) {
        case LEFT_JOIN:
            joinType = JoinType.RIGHT_JOIN;
            break;
        case RIGHT_JOIN:
            joinType = JoinType.LEFT_JOIN;
            break;
        }
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        if (left == oldInput)
            left = (Joinable)newInput;
        if (right == oldInput)
            right = (Joinable)newInput;
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
        return super.summaryString() + 
            "(" + joinType.toString() + joinConditions.toString() + ")";
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        left = (Joinable)left.duplicate(map);
        right = (Joinable)right.duplicate(map);
        int pos = -1;
        if (groupJoinCondition != null)
            pos = joinConditions.indexOf(groupJoinCondition);
        joinConditions = duplicateList(joinConditions, map);
        if (groupJoinCondition != null)
            groupJoinCondition = joinConditions.get(pos);
    }

}
