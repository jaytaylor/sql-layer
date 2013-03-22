
package com.akiban.sql.optimizer.plan;

import com.akiban.server.types.AkType;

import java.util.List;

/** A special function used in an index. 
 * For now, limited to 2D spatial.
 */
public class SpecialIndexExpression extends BaseExpression 
{
    public static enum Function { Z_ORDER_LAT_LON };
    private Function function;
    private List<ExpressionNode> operands;

    public SpecialIndexExpression(Function function, List<ExpressionNode> operands) {
        super(null, AkType.LONG, null);
        this.function = function;
        this.operands = operands;
    }

    public Function getFunction() {
        return function;
    }
    public List<ExpressionNode> getOperands() {
        return operands;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof SpecialIndexExpression)) return false;
        SpecialIndexExpression other = (SpecialIndexExpression)obj;
        return ((function == other.function) && operands.equals(other.operands));
    }

    @Override
    public int hashCode() {
        int hash = function.hashCode();
        hash += operands.hashCode();
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            for (ExpressionNode operand : operands) {
                if (!operand.accept(v))
                    break;
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
        for (int i = 0; i < operands.size(); i++) {
            operands.set(i, operands.get(i).accept(v));
        }
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(function.name());
        str.append("(");
        boolean first = true;
        for (ExpressionNode operand : operands) {
            if (first) first = false; else str.append(",");
            str.append(operand);
        }
        str.append(")");
        return str.toString();
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        operands = duplicateList(operands, map);
    }

}
