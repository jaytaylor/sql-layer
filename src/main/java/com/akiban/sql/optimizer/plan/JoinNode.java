/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        left = (Joinable)left.duplicate(map);
        right = (Joinable)right.duplicate(map);
        joinConditions = joinConditions.duplicate(map);
    }

}
