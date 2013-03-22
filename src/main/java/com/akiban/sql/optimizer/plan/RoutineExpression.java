
package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.Routine;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

import java.util.List;

/** A call to a function.
 */
public class RoutineExpression extends BaseExpression
{
    private Routine routine;
    private List<ExpressionNode> operands;

    public RoutineExpression(Routine routine,
                             List<ExpressionNode> operands,
                             DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, sqlSource);
        this.routine = routine;
        this.operands = operands;
        if (routine.getParameters().size() != operands.size())
            throw new WrongExpressionArityException(routine.getParameters().size(),
                                                    operands.size());
    }

    public Routine getRoutine() {
        return routine;
    }
    public List<ExpressionNode> getOperands() {
        return operands;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof RoutineExpression)) return false;
        RoutineExpression other = (RoutineExpression)obj;
        return (routine.equals(other.routine) &&
                operands.equals(other.operands));
    }

    @Override
    public int hashCode() {
        int hash = routine.getName().hashCode();
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
        StringBuilder str = new StringBuilder(routine.getName().toString());
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
