
package com.akiban.sql.optimizer.plan;

import com.akiban.server.types.AkType;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.texpressions.TValidatedAggregator;
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

import java.util.List;

/** An expression representing the result (total) of an aggregate function.
 */
public class AggregateFunctionExpression extends BaseExpression implements ResolvableExpression<TValidatedAggregator>
{
    private String function;
    private ExpressionNode operand;
    private boolean distinct;
    private Object option;
    private List<OrderByExpression> orderBy;
    private TValidatedAggregator tAggregator;

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

    @Override
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
    public void setResolved(TValidatedAggregator resolved) {
        this.tAggregator = resolved;
    }

    @Override
    public TValidatedAggregator getResolved() {
        return tAggregator;
    }

    @Override
    public TPreptimeContext getPreptimeContext() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setPreptimeContext(TPreptimeContext context) {
        throw new UnsupportedOperationException();
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
