/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.optimizer.plan;

import com.foundationdb.ais.model.Routine;
import com.foundationdb.server.error.WrongExpressionArityException;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.parser.ValueNode;

import java.util.List;

/** A call to a function.
 */
public class RoutineExpression extends BaseExpression
{
    private Routine routine;
    private List<ExpressionNode> operands;

    public RoutineExpression(Routine routine,
                             List<ExpressionNode> operands,
                             DataTypeDescriptor sqlType, ValueNode sqlSource,
                             TInstance type) {
        super(sqlType, sqlSource, type);
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
