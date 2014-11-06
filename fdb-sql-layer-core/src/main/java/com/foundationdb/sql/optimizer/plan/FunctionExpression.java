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

import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.texpressions.TValidatedScalar;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.parser.ValueNode;
import com.foundationdb.util.SparseArray;

import java.util.List;

/** A call to a function.
 */
public class FunctionExpression extends BaseExpression implements ResolvableExpression<TValidatedScalar>
{
    private String function;
    private List<ExpressionNode> operands;
    private TValidatedScalar overload;
    private SparseArray<Object> preptimeValues;
    private TPreptimeContext preptimeContext;

    public FunctionExpression(String function,
                              List<ExpressionNode> operands,
                              DataTypeDescriptor sqlType, ValueNode sqlSource,
                              TInstance type) {
        super(sqlType, sqlSource, type);
        this.function = function;
        this.operands = operands;
    }

    @Override
    public String getFunction() {
        return function;
    }
    public List<ExpressionNode> getOperands() {
        return operands;
    }

    @Override
    public void setResolved(TValidatedScalar resolved) {
        this.overload = resolved;
    }

    @Override
    public TValidatedScalar getResolved() {
        return overload;
    }

    public SparseArray<Object> getPreptimeValues() {
        return preptimeValues;
    }

    public void setPreptimeValues(SparseArray<Object> values) {
        this.preptimeValues = values;
    }

    @Override
    public TPreptimeContext getPreptimeContext() {
        return preptimeContext;
    }

    @Override
    public void setPreptimeContext(TPreptimeContext context) {
        this.preptimeContext = context;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof FunctionExpression)) return false;
        FunctionExpression other = (FunctionExpression)obj;
        return (function.equals(other.function) &&
                operands.equals(other.operands));
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
        StringBuilder str = new StringBuilder(function);
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
