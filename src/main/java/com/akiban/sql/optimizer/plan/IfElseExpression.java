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

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

/** IF ... THEN ... ELSE ...
 * Normally loaded from CASE
 */
public class IfElseExpression extends BaseExpression
{
    private ConditionList testConditions;
    private ExpressionNode thenExpression, elseExpression;
    
    public IfElseExpression(ConditionList testConditions,
                            ExpressionNode thenExpression, ExpressionNode elseExpression,
                            DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, sqlSource);
        this.testConditions = testConditions;
        this.thenExpression = thenExpression;
        this.elseExpression = elseExpression;
    }

    public ConditionList getTestConditions() {
        return testConditions;
    }
    public ExpressionNode getThenExpression() {
        return thenExpression;
    }
    public ExpressionNode getElseExpression() {
        return elseExpression;
    }

    public void setThenExpression(ExpressionNode thenExpression) {
        this.thenExpression = thenExpression;
    }

    public void setElseExpression(ExpressionNode elseExpression) {
        this.elseExpression = elseExpression;
    }

    /** Get the single condition (after compaction). */
    public ConditionExpression getTestCondition() {
        assert (testConditions.size() == 1);
        return testConditions.get(0);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IfElseExpression)) return false;
        IfElseExpression other = (IfElseExpression)obj;
        return (testConditions.equals(other.testConditions) &&
                thenExpression.equals(other.thenExpression) &&
                elseExpression.equals(other.elseExpression));
    }

    @Override
    public int hashCode() {
        int hash = testConditions.hashCode();
        hash += thenExpression.hashCode();
        hash += elseExpression.hashCode();
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            if (testConditions.accept(v) &&
                thenExpression.accept(v))
                elseExpression.accept(v);
        }
        return v.visitLeave(this);
    }

    @Override
    public ExpressionNode accept(ExpressionRewriteVisitor v) {
        boolean childrenFirst = v.visitChildrenFirst(this);
        if (!childrenFirst) {
            ExpressionNode result = v.visit(this);
            if (result != this) return result;
        }
        testConditions.accept(v);
        thenExpression = thenExpression.accept(v);
        elseExpression = elseExpression.accept(v);
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder("IF(");
        str.append(testConditions);
        str.append(", ");
        str.append(thenExpression);
        str.append(", ");
        str.append(elseExpression);
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        testConditions = testConditions.duplicate(map);
        thenExpression = (ExpressionNode)thenExpression.duplicate(map);
        elseExpression = (ExpressionNode)elseExpression.duplicate(map);
    }

}
