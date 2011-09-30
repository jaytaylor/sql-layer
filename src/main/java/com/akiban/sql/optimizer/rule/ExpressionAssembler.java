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

package com.akiban.sql.optimizer.rule;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionRegistry;
import com.akiban.server.types.extract.Extractors;
import com.akiban.sql.optimizer.plan.*;

import static com.akiban.server.expression.std.Expressions.*;
import com.akiban.qp.operator.Operator;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.UnsupportedSQLException;

import java.util.ArrayList;
import java.util.List;

/** Turn {@link ExpressionNode} into {@link Expression}. */
public class ExpressionAssembler
{
    private ExpressionRegistry expressionRegistry;

    public ExpressionAssembler(RulesContext rulesContext) {
        this.expressionRegistry = ((SchemaRulesContext)
                                  rulesContext).getExpressionRegistry();
    }

    public static interface ColumnExpressionToIndex {
        /** Return the field position of the given column in the target row. */
        public int getIndex(ColumnExpression column);
    }
    
    public Expression assembleExpression(ExpressionNode node,
                                         ColumnExpressionToIndex fieldOffsets) {
        if (node instanceof ConstantExpression)
            return literal(((ConstantExpression)node).getValue());
        else if (node instanceof ColumnExpression)
            return field(((ColumnExpression)node).getColumn(), fieldOffsets.getIndex((ColumnExpression)node));
        else if (node instanceof ParameterExpression)
            return variable(node.getAkType(), ((ParameterExpression)node).getPosition());
        else if (node instanceof BooleanOperationExpression)
            throw new UnsupportedSQLException("NIY", null);
        else if (node instanceof CastExpression)
            // TODO: Need actual cast.
            return assembleExpression(((CastExpression)node).getOperand(),
                                      fieldOffsets);
        else if (node instanceof ComparisonCondition) {
            ComparisonCondition cond = (ComparisonCondition)node;
            return compare(assembleExpression(cond.getLeft(), fieldOffsets),
                           cond.getOperation(),
                           assembleExpression(cond.getRight(), fieldOffsets));
        }
        else if (node instanceof FunctionExpression) {
            FunctionExpression funcNode = (FunctionExpression) node;
            List<Expression> children = new ArrayList<Expression>();
            for (ExpressionNode operand : funcNode.getOperands()) {
                children.add(assembleExpression(operand, fieldOffsets));
            }
            return expressionRegistry.composer(funcNode.getFunction()).compose(children);
        }
        else if (node instanceof IfElseExpression)
            throw new UnsupportedSQLException("NIY", node.getSQLsource());
        else if (node instanceof AggregateFunctionExpression)
            throw new UnsupportedSQLException("Aggregate used as regular function", 
                                              node.getSQLsource());
        else if (node instanceof SubqueryExpression)
            throw new AkibanInternalException("Should have called assembleSubqueryExpression");
        else
            throw new UnsupportedSQLException("Unknown expression", node.getSQLsource());
    }

    public Expression assembleSubqueryExpression(SubqueryExpression node,
                                                 ColumnExpressionToIndex fieldOffsets,
                                                 Operator subquery) {
        if (node instanceof SubqueryValueExpression)
            throw new UnsupportedSQLException("subquery as expression", 
                                              node.getSQLsource());
        else if (node instanceof ExistsCondition)
            throw new UnsupportedSQLException("EXISTS as expression", 
                                              node.getSQLsource());
        else if (node instanceof AnyCondition)
            throw new UnsupportedSQLException("ANY as expression", 
                                              node.getSQLsource());
        else
            throw new UnsupportedSQLException("Unknown subquery", node.getSQLsource());
    }

    

    public ConstantExpression evalNow(ExpressionNode node) {
        if (node instanceof ConstantExpression)
            return (ConstantExpression)node;
        Expression expr = assembleExpression(node, null);
        if (!expr.isConstant())
            throw new AkibanInternalException("required constant expression: " + expr);
        if (node instanceof ConditionExpression) {
            boolean value = Extractors.getBooleanExtractor().getBoolean(expr.evaluation().eval(), false);
            return new BooleanConstantExpression(value,
                                                 node.getSQLtype(), 
                                                 node.getSQLsource());
        }
        else {
            return new ConstantExpression(expr.evaluation().eval(),
                                          node.getSQLtype(), 
                                          node.getSQLsource());
        }
    }

}
