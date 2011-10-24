/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.sql.optimizer.plan;

import com.akiban.server.types.AkType;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

/** An expression representing the result (total) of an aggregate function.
 */
public class AggregateFunctionExpression extends BaseExpression 
{
    private String function;
    private ExpressionNode operand;
    private boolean distinct;
    
    public AggregateFunctionExpression(String function, ExpressionNode operand,
                                       boolean distinct, 
                                       DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, "COUNT".equals(function) ? AkType.LONG : operand.getAkType(), sqlSource);
        this.function = function;
        this.operand = operand;
        this.distinct = distinct;
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

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof AggregateFunctionExpression)) return false;
        AggregateFunctionExpression other = (AggregateFunctionExpression)obj;
        return (function.equals(other.function) &&
                ((operand == null) ? 
                 (other.operand == null) :
                 operand.equals(other.operand)) &&
                (distinct == other.distinct));
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
