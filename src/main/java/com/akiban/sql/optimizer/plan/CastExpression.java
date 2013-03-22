
package com.akiban.sql.optimizer.plan;

import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

/** Cast the result of expression evaluation to a given type.
 */
public class CastExpression extends BaseExpression 
{
    private ExpressionNode inner;

    public CastExpression(ExpressionNode inner, 
                          DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, sqlSource);
        this.inner = inner;
    }

    public CastExpression(ExpressionNode inner, 
                          DataTypeDescriptor sqlType, AkType akType, ValueNode sqlSource) {
        super(sqlType, akType, sqlSource);
        this.inner = inner;
    }

    public ExpressionNode getOperand() {
        return inner;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CastExpression)) return false;
        CastExpression other = (CastExpression)obj;
        return (getSQLtype().equals(other.getSQLtype()) &&
                inner.equals(other.inner));
    }

    @Override
    public int hashCode() {
        int hash = getSQLtype().hashCode();
        hash += inner.hashCode();
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            inner.accept(v);
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
        inner = inner.accept(v);
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        Object typeDescriptor;
        TPreptimeValue tpv = getPreptimeValue();
        if (tpv != null) {
            TInstance instance = tpv.instance();
            typeDescriptor = instance == null ? "<unknown>" : instance;
        }
        else {
            typeDescriptor = getSQLtype();
        }
        return "Cast(" + inner + " AS " + typeDescriptor + ")";
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        inner = (ExpressionNode)inner.duplicate(map);
    }

}
