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
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.parser.ValueNode;

import java.util.List;

/** This is what IN gets turned into late in the game after
 * optimizations have been exhausted. */
public class InListCondition extends BaseExpression implements ConditionExpression 
{
    private ExpressionNode operand;
    private List<ExpressionNode> expressions;
    private ComparisonCondition comparison;

    public InListCondition(ExpressionNode operand, List<ExpressionNode> expressions,
                           DataTypeDescriptor sqlType, ValueNode sqlSource,
                           TInstance type) {
        super(sqlType, sqlSource, type);
        this.operand = operand;
        this.expressions = expressions;
    }

    public ExpressionNode getOperand() {
        return operand;
    }
    public void setOperand(ExpressionNode operand) {
        this.operand = operand;
    }

    public List<ExpressionNode> getExpressions() {
        return expressions;
    }
    public void setExpressions(List<ExpressionNode> expressions) {
        this.expressions = expressions;
    }

    public ComparisonCondition getComparison() {
        return comparison;
    }
    public void setComparison(ComparisonCondition comparison) {
        this.comparison = comparison;
    }

    @Override
    public Implementation getImplementation() {
        return Implementation.NORMAL;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof InListCondition)) return false;
        InListCondition other = (InListCondition)obj;
        return (operand.equals(other.operand) &&
                expressions.equals(other.expressions));
    }

    @Override
    public int hashCode() {
        int hash = operand.hashCode();
        hash += expressions.hashCode();
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            if (operand.accept(v)) {
                for (ExpressionNode expression : expressions) {
                    if (!expression.accept(v))
                        break;
                }
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
        operand = operand.accept(v);
        for (int i = 0; i < expressions.size(); i++)
            expressions.set(i, expressions.get(i).accept(v));
        return (childrenFirst) ? v.visit(this) : this;
    }

    @Override
    public String toString() {
        return "IN(" + operand + ", " + expressions + ")";
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        operand = (ExpressionNode)operand.duplicate();
        expressions = duplicateList(expressions, map);
    }

}
