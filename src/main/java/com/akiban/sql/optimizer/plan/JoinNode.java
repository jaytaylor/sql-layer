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

import com.akiban.server.error.AkibanInternalException;

import java.util.*;

/** A join between two tables / subjoins. */
public class JoinNode extends BaseJoinable implements PlanWithInput
{
    public static enum JoinType {
        INNER,
        LEFT,
        RIGHT,
        FULL_OUTER,
        // These are beyond what flatten supports, used to represent EXISTS (sometimes).
        SEMI,
        ANTI
    }
    public static enum Implementation {
        GROUP,
        NESTED_LOOPS,
        MERGE                   // TODO: Not implemented. Probably needs thought.
    }
    private Joinable left, right;
    private JoinType joinType;
    private Implementation implementation;
    private ConditionList joinConditions;
    private TableGroupJoin groupJoin;

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

    public void setJoinType(JoinType joinType) {
        this.joinType = joinType;
    }

    @Override
    public boolean isJoin() {
        return true;
    }

    @Override
    public boolean isInnerJoin() {
        return (joinType == JoinType.INNER);
    }

    public ConditionList getJoinConditions() {
        return joinConditions;
    }
    public void setJoinConditions(ConditionList joinConditions) {
        this.joinConditions = joinConditions;
    }

    public boolean hasJoinConditions() {
        return ((joinConditions != null) && !joinConditions.isEmpty());
    }

    public TableGroupJoin getGroupJoin() {
        return groupJoin;
    }
    public void setGroupJoin(TableGroupJoin groupJoin) {
        this.groupJoin = groupJoin;
        this.implementation = Implementation.GROUP;
    }

    /** Get the condition that implements groupJoin. */
    public ConditionExpression getGroupJoinCondition() {
        for (ConditionExpression condition : joinConditions) {
            if (condition.getImplementation() == ConditionExpression.Implementation.GROUP_JOIN)
                return condition;
        }
        return null;
    }

    public Implementation getImplementation() {
        return implementation;
    }
    public void setImplementation(Implementation implementation) {
        this.implementation = implementation;
    }

    // TODO: Maybe it would be better to move this to MapJoin and
    // convert over sooner.  See how other kinds of joins work out.
    public static interface JoinReverseHook {
        public boolean canReverse(JoinNode join);
        public void beforeReverse(JoinNode join);
        public void didNotReverse(JoinNode join);
    }

    private JoinReverseHook reverseHook;

    public JoinReverseHook getReverseHook() {
        return reverseHook;
    }
    public void setReverseHook(JoinReverseHook reverseHook) {
        this.reverseHook = reverseHook;
    }

    /** Reverse operands and outer join direction if necessary. */
    public void reverse() {
        if (reverseHook != null)
            reverseHook.beforeReverse(this);
        switch (joinType) {
        case INNER:
        case FULL_OUTER:
            break;
        case LEFT:
            joinType = JoinType.RIGHT;
            break;
        case RIGHT:
            joinType = JoinType.LEFT;
            break;
        default:
            throw new AkibanInternalException("Cannot reverse " + joinType + " join");
        }
        Joinable temp = left;
        left = right;
        right = temp;
    }

    @Override
    public void replaceInput(PlanNode oldInput, PlanNode newInput) {
        if (left == oldInput) {
            left = (Joinable)newInput;
            left.setOutput(this);
        }
        if (right == oldInput) {
            right = (Joinable)newInput;
            right.setOutput(this);
        }
    }

    @Override
    public boolean accept(PlanVisitor v) {
        if (v.visitEnter(this)) {
            if (left.accept(v) && 
                right.accept(v) &&
                (joinConditions != null)) {
                if (v instanceof ExpressionRewriteVisitor) {
                    joinConditions.accept((ExpressionRewriteVisitor)v);
                }
                else if (v instanceof ExpressionVisitor) {
                    joinConditions.accept((ExpressionVisitor)v);
                }
            }
        }
        return v.visitLeave(this);
    }
    
    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        str.append(joinType);
        if (implementation != null) {
            str.append("/");
            str.append(implementation);
        }
        if (joinConditions != null)
            str.append(joinConditions.toString());
        if (groupJoin != null) {
            str.append(" - ");
            str.append(groupJoin);
        }
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        left = (Joinable)left.duplicate(map);
        right = (Joinable)right.duplicate(map);
        joinConditions = joinConditions.duplicate(map);
    }

}
