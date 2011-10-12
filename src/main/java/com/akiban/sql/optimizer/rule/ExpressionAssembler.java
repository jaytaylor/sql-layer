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
import com.akiban.server.expression.std.InExpression;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.UnsupportedSQLException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Turn {@link ExpressionNode} into {@link Expression}. */
public class ExpressionAssembler
{
    private ExpressionRegistry expressionRegistry;

    public ExpressionAssembler(RulesContext rulesContext) {
        this.expressionRegistry = ((SchemaRulesContext)
                                  rulesContext).getExpressionRegistry();
    }

    public interface ColumnExpressionToIndex {
        /** Return the field position of the given column in the target row. */
        public int getIndex(ColumnExpression column);

        /** Return the row type that the index goes with. */
        public RowType getRowType();
    }
    
    public interface ColumnExpressionContext {
        /** Get the current input row if any. */
        public ColumnExpressionToIndex getCurrentRow();

        /** Get list (deepest first) of rows from nested loops. */
        public List<ColumnExpressionToIndex> getBoundRows();

        /** Get the index offset to be used for the deepest nested loop.
         * Normally this is the number of parameters to the query.
         */
        public int getBindingsOffset();
    }

    public Expression assembleExpression(ExpressionNode node,
                                         ColumnExpressionContext columnContext) {
        if (node instanceof ConstantExpression)
            return literal(((ConstantExpression)node).getValue());
        else if (node instanceof ColumnExpression)
            return assembleColumnExpression((ColumnExpression)node, columnContext);
        else if (node instanceof ParameterExpression)
            return variable(node.getAkType(), ((ParameterExpression)node).getPosition());
        else if (node instanceof BooleanOperationExpression) {
            BooleanOperationExpression bexpr = (BooleanOperationExpression)node;
            return expressionRegistry
                .composer(bexpr.getOperation().getFunctionName())
                .compose(Arrays.asList(assembleExpression(bexpr.getLeft(), columnContext),
                                       assembleExpression(bexpr.getRight(), columnContext)));
        }
        else if (node instanceof CastExpression)
            // TODO: Need actual cast.
            return assembleExpression(((CastExpression)node).getOperand(),
                                      columnContext);
        else if (node instanceof ComparisonCondition) {
            ComparisonCondition cond = (ComparisonCondition)node;
            return compare(assembleExpression(cond.getLeft(), columnContext),
                           cond.getOperation(),
                           assembleExpression(cond.getRight(), columnContext));
        }
        else if (node instanceof FunctionExpression) {
            FunctionExpression funcNode = (FunctionExpression)node;
            return expressionRegistry
                .composer(funcNode.getFunction())
                .compose(assembleExpressions(funcNode.getOperands(), columnContext));
        }
        else if (node instanceof IfElseExpression) {
            IfElseExpression ifElse = (IfElseExpression)node;
            // TODO: Is this right?
            return expressionRegistry
                .composer("ifThenElse")
                .compose(Arrays.asList(assembleExpression(ifElse.getTestCondition(), 
                                                          columnContext),
                                       assembleExpression(ifElse.getThenExpression(), 
                                                          columnContext),
                                       assembleExpression(ifElse.getElseExpression(), 
                                                          columnContext)));
        }
        else if (node instanceof InListCondition) {
            InListCondition inList = (InListCondition)node;
            Expression lhs = assembleExpression(inList.getOperand(), columnContext);
            List<Expression> rhs = assembleExpressions(inList.getExpressions(),
                                                       columnContext);
            return new InExpression(lhs, rhs);
        }
        else if (node instanceof AggregateFunctionExpression)
            throw new UnsupportedSQLException("Aggregate used as regular function", 
                                              node.getSQLsource());
        else if (node instanceof SubqueryExpression)
            throw new AkibanInternalException("Should have called assembleSubqueryExpression");
        else
            throw new UnsupportedSQLException("Unknown expression", node.getSQLsource());
    }

    public Expression assembleColumnExpression(ColumnExpression column,
                                               ColumnExpressionContext columnContext) {
        ColumnExpressionToIndex currentRow = columnContext.getCurrentRow();
        if (currentRow != null) {
            int fieldIndex = currentRow.getIndex(column);
            if (fieldIndex >= 0)
                return field(currentRow.getRowType(), fieldIndex);
        }
        
        List<ColumnExpressionToIndex> boundRows = columnContext.getBoundRows();
        for (int rowIndex = boundRows.size() - 1; rowIndex >= 0; rowIndex--) {
            ColumnExpressionToIndex boundRow = boundRows.get(rowIndex);
            if (boundRow == null) continue;
            int fieldIndex = boundRow.getIndex(column);
            if (fieldIndex >= 0) {
                rowIndex += columnContext.getBindingsOffset();
                return boundField(boundRow.getRowType(), rowIndex, fieldIndex);
            }
        }
        throw new AkibanInternalException("Column not found " + column);
    }

    public Expression assembleSubqueryExpression(SubqueryExpression node,
                                                 Operator subquery,
                                                 RowType outerRowType,
                                                 RowType innerRowType,
                                                 int bindingPosition) {
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

    protected List<Expression> assembleExpressions(List<ExpressionNode> expressions,
                                                   ColumnExpressionContext columnContext) {
        List<Expression> result = new ArrayList<Expression>(expressions.size());
        for (ExpressionNode expr : expressions) {
            result.add(assembleExpression(expr, columnContext));
        }
        return result;
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
