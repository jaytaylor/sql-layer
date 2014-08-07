/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.server.error.AkibanInternalException;

/** A join between two tables / subjoins. */
public class JoinNode extends BaseJoinable implements PlanWithInput
{
    public static enum JoinType {
        INNER,
        LEFT,
        RIGHT,
        FULL_OUTER,
        // These are beyond what flatten supports, used to represent EXISTS or IN (sometimes).
        SEMI,
        ANTI,
        // These are intermediate to represent when a semi-join can be
        // turned into a regular join.
        SEMI_INNER_ALREADY_DISTINCT,
        SEMI_INNER_IF_DISTINCT,
        INNER_NEED_DISTINCT;

        public final boolean isInner() {
            return ((this == INNER) ||
                    (this == INNER_NEED_DISTINCT));
        }

        public final boolean isOuter() {
            return ((this == LEFT) ||
                    (this == RIGHT) ||
                    (this == FULL_OUTER));
        }

        public final boolean isSemi() {
            return ((this == SEMI) ||
                    (this == SEMI_INNER_ALREADY_DISTINCT) ||
                    (this == SEMI_INNER_IF_DISTINCT));
        }
    }
    public static enum Implementation {
        GROUP,
        NESTED_LOOPS,
        BLOOM_FILTER, 
        MERGE                   // TODO: Not implemented. Probably needs thought.
    }
    private Joinable left, right;
    private JoinType joinType;
    private Implementation implementation;
    private ConditionList joinConditions;
    private TableGroupJoin groupJoin;
    private TableFKJoin fkJoin;

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
        if (groupJoin != null)
            this.implementation = Implementation.GROUP;
    }

    public TableFKJoin getFKJoin() { 
        return fkJoin;
    }
    
    public void setFKJoin (TableFKJoin fkJoin) {
        this.fkJoin = fkJoin;
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

    /** Reverse operands and outer join direction if necessary. */
    public void reverse() {
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
            if (acceptPlans(v) &&
                (joinConditions != null)) {
                acceptConditions(v);
            }
        }
        return v.visitLeave(this);
    }
    
    protected boolean acceptPlans(PlanVisitor v) {
        return (left.accept(v) && right.accept(v));
    }

    protected void acceptConditions(PlanVisitor v) {
        if (v instanceof ExpressionRewriteVisitor) {
            joinConditions.accept((ExpressionRewriteVisitor)v);
        }
        else if (v instanceof ExpressionVisitor) {
            joinConditions.accept((ExpressionVisitor)v);
        }
    }

    @Override
    public String summaryString() {
        StringBuilder str = new StringBuilder(super.summaryString());
        str.append("(");
        summarizeJoins(str);
        str.append(")");
        return str.toString();
    }

    protected void summarizeJoins(StringBuilder str) {
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
        if (fkJoin != null) {
            str.append(" - ");
            str.append(fkJoin);
        }
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        left = (Joinable)left.duplicate(map);
        right = (Joinable)right.duplicate(map);
        joinConditions = joinConditions.duplicate(map);
    }

}
