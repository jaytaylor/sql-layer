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

import com.akiban.server.error.UnsupportedSQLException;

import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.parser.ValueNode;

import com.akiban.qp.expression.Expression;

import java.util.List;

/** IF ... THEN ... ELSE ...
 * Normally loaded from CASE
 */
public class IfElseExpression extends BaseExpression
{
    private ConditionExpression testCondition;
    private ExpressionNode thenExpression, elseExpression;
    
    public IfElseExpression(ConditionExpression testCondition,
                            ExpressionNode thenExpression, ExpressionNode elseExpression,
                            DataTypeDescriptor sqlType, ValueNode sqlSource) {
        super(sqlType, sqlSource);
        this.testCondition = testCondition;
        this.thenExpression = thenExpression;
        this.elseExpression = elseExpression;
    }

    public ConditionExpression getTestCondition() {
        return testCondition;
    }
    public ExpressionNode getThenExpression() {
        return thenExpression;
    }
    public ExpressionNode getElseExpression() {
        return elseExpression;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof IfElseExpression)) return false;
        IfElseExpression other = (IfElseExpression)obj;
        return (testCondition.equals(other.testCondition) &&
                thenExpression.equals(other.thenExpression) &&
                elseExpression.equals(other.elseExpression));
    }

    @Override
    public int hashCode() {
        int hash = testCondition.hashCode();
        hash += thenExpression.hashCode();
        hash += elseExpression.hashCode();
        return hash;
    }

    @Override
    public boolean accept(ExpressionVisitor v) {
        if (v.visitEnter(this)) {
            if (testCondition.accept(v) &&
                thenExpression.accept(v))
                elseExpression.accept(v);
        }
        return v.visitLeave(this);
    }

    @Override
    public ExpressionNode accept(ExpressionRewriteVisitor v) {
        ExpressionNode result = v.visit(this);
        if (result != this) return result;
        testCondition = (ConditionExpression)testCondition.accept(v);
        thenExpression = thenExpression.accept(v);
        elseExpression = elseExpression.accept(v);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder("IF(");
        str.append(testCondition);
        str.append(", ");
        str.append(thenExpression);
        str.append(", ");
        str.append(elseExpression);
        str.append(")");
        return str.toString();
    }

    @Override
    public Expression generateExpression(ColumnExpressionToIndex fieldOffsets) {
        throw new UnsupportedSQLException("NIY", null);
    }

    @Override
    protected void deepCopy(DuplicateMap map) {
        super.deepCopy(map);
        testCondition = (ConditionExpression)testCondition.duplicate(map);
        thenExpression = (ExpressionNode)thenExpression.duplicate(map);
        elseExpression = (ExpressionNode)elseExpression.duplicate(map);
    }

}
