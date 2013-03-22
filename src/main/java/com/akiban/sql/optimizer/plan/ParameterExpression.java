
package com.akiban.sql.optimizer.plan;

import com.akiban.server.types.AkType;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

/** An operand with a parameter value. */
public class ParameterExpression extends BaseExpression 
{
    private int position;

    public ParameterExpression(int position, 
                               DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, sqlSource);
        this.position = position;
    }

    public ParameterExpression(int position, 
                               DataTypeDescriptor sqlType, AkType akType, 
                               ValueNode sqlSource) {
        super(sqlType, akType, sqlSource);
        this.position = position;
    }

    public int getPosition() {
        return position;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ParameterExpression)) return false;
        ParameterExpression other = (ParameterExpression)obj;
        return (position == other.position);
    }

    @Override
    public int hashCode() {
        return position;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        return v.visit(this);
    }

    @Override
    public ExpressionNode accept(ExpressionRewriteVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return "$" + position;
    }

}
