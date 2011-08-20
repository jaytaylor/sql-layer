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

package com.akiban.sql.optimizer.query;

import com.akiban.ais.model.Join;
import com.akiban.qp.physicaloperator.API.JoinType;

import java.util.*;

// A join between two tables / subjoins.
public class JoinJoinNode extends BaseJoinNode 
{
    private BaseJoinNode left, right;
    private JoinType joinType;
    private List<BooleanExpression> joinConditions;
    private Join groupJoin;
    private BooleanExpression groupJoinCondition;

    public JoinJoinNode(BaseJoinNode left, BaseJoinNode right,
                        JoinType joinType) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
    }

    public BaseJoinNode getLeft() {
        return left;
    }
    public void setLeft(BaseJoinNode left) {
        this.left = left;
    }
    public BaseJoinNode getRight() {
        return right;
    }
    public void setRight(BaseJoinNode right) {
        this.right = right;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public boolean isInnerJoin() {
        return (joinType == JoinType.INNER_JOIN);
    }

    public List<BooleanExpression> getJoinConditions() {
        return joinConditions;
    }
    public void setJoinConditions(List<BooleanExpression> joinConditions) {
        this.joinConditions = joinConditions;
    }

    public Join getGroupJoin() {
        return groupJoin;
    }
    public void setGroupJoin(Join groupJoin) {
        this.groupJoin = groupJoin;
    }

    public BooleanExpression getGroupJoinCondition() {
        return groupJoinCondition;
    }
    public void setGroupJoinCondition(BooleanExpression groupJoinCondition) {
        this.groupJoinCondition = groupJoinCondition;
    }

    /** Reverse operands and outer join direction if necessary. */
    public void reverse() {
        BaseJoinNode temp = left;
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

    public String toString() {
        return joinType + "(" + left + "," + right + ")";
    }
}
