
package com.akiban.sql.optimizer.plan;

import com.akiban.server.types.AkType;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

/** An indicator that an index equality operand is actually IS NULL. 
 * <code>null</code> could be confused with the absence of an operand.
 * <code>ConstantExpression</code> could be confused with a literal
 * NULL, which is never equal.
 */
public class IsNullIndexKey extends BaseExpression 
{
    public IsNullIndexKey(DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, AkType.NULL, sqlSource);
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof IsNullIndexKey);
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public String toString() {
        return "NULL";
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        return v.visit(this);
    }

    @Override
    public ExpressionNode accept(ExpressionRewriteVisitor v) {
        return v.visit(this);
    }

}
