
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
