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

import com.akiban.server.types.AkType;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

import java.util.List;

/** This is what IN gets turned into late in the game after
 * optimizations have been exhausted. */
public class InListCondition extends BaseExpression implements ConditionExpression 
{
    private ExpressionNode operand;
    private List<ExpressionNode> expressions;

    public InListCondition(ExpressionNode operand, List<ExpressionNode> expressions,
                           DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, AkType.BOOL, sqlSource);
        this.operand = operand;
        this.expressions = expressions;
    }

    public ExpressionNode getOperand() {
        return operand;
    }
    public void setOperand(ExpressionNode operand) {
        this.operand = operand;
    }

    public List<ExpressionNode> getExpressions() {
        return expressions;
    }
    public void setExpressions(List<ExpressionNode> expressions) {
        this.expressions = expressions;
    }

    @Override
    public Implementation getImplementation() {
        return Implementation.NORMAL;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof InListCondition)) return false;
        InListCondition other = (InListCondition)obj;
        return (operand.equals(other.operand) &&
                expressions.equals(other.expressions));
    }

    @Override
    public int hashCode() {
        int hash = operand.hashCode();
        hash += expressions.hashCode();
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            if (operand.accept(v)) {
                for (ExpressionNode expression : expressions) {
                    if (!expression.accept(v))
                        break;
                }
            }
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
        operand = operand.accept(v);
        for (int i = 0; i < expressions.size(); i++)
            expressions.set(i, expressions.get(i).accept(v));
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        return "IN(" + operand + ", " + expressions + ")";
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        operand = (ExpressionNode)operand.duplicate();
        expressions = duplicateList(expressions, map);
    }

}
