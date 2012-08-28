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
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;
import com.akiban.sql.parser.OrderByList;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;
import java.util.List;

/** An expression representing the result (total) of an aggregate function.
 */
public class AggregateFunctionExpression extends BaseExpression 
{
    private String function;
    private ExpressionNode operand;
    private boolean distinct;
    private Object option;
    private List<OrderByExpression> orderBy;

    public AggregateFunctionExpression(String function, ExpressionNode operand,
                                       boolean distinct, 
                                       DataTypeDescriptor sqlType, ValueNode sqlSource,
                                       Object option, List<OrderByExpression> orderBy) {
        super(sqlType, "COUNT".equals(function) ? AkType.LONG : operand.getAkType(), sqlSource);
        this.function = function;
        this.operand = operand;
        this.distinct = distinct;
        this.option = option;
        this.orderBy = orderBy;
    }

    public String getFunction() {
        return function;
    }
    public ExpressionNode getOperand() {
        return operand;
    }
    public boolean isDistinct() {
        return distinct;
    }

    public void setOperand(ExpressionNode operand) {
        this.operand = operand;
    }

    public Object getOption()
    {
        return option;
    }
    
    public List<OrderByExpression> getOrderBy()
    {
        return orderBy;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AggregateFunctionExpression)) return false;
        AggregateFunctionExpression other = (AggregateFunctionExpression)obj;
        return (function.equals(other.function) &&
                ((operand == null) ? 
                 (other.operand == null) :
                 operand.equals(other.operand)) &&
                (distinct == other.distinct) &&
                (option == null ? other.option == null : option.equals(other.option)));
    }

    @Override
    public int hashCode() {
        int hash = function.hashCode();
        if (operand != null)
            hash += operand.hashCode();
        if (distinct) hash ^= 1;
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            if (operand != null) 
                operand.accept(v);
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
        if (operand != null)
            operand = operand.accept(v);
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(function);
        str.append("(");
        if (distinct)
            str.append("DISTINCT ");
        if (operand == null)
            str.append("*");
        else
            str.append(operand);
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        if (operand != null)
            operand = (ExpressionNode)operand.duplicate(map);
    }

}
