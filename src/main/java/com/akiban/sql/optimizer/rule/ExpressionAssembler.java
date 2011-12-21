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
import com.akiban.server.expression.EnvironmentExpressionFactory;
import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.server.types.extract.Extractors;
import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.types.DataTypeDescriptor;
import static com.akiban.server.expression.std.Expressions.*;
import com.akiban.server.expression.std.InExpression;
import com.akiban.server.types.AkType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.expression.std.IntervalCastExpression;
import com.akiban.sql.pg.PostgresType;
import com.akiban.sql.types.TypeId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Turn {@link ExpressionNode} into {@link Expression}. */
public class ExpressionAssembler
{
    private FunctionsRegistry functionsRegistry;

    public ExpressionAssembler(RulesContext rulesContext) {
        this.functionsRegistry = ((SchemaRulesContext)
                                  rulesContext).getFunctionsRegistry();
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
        public int getExpressionBindingsOffset();

        /** Get the index offset to be used for the deepest nested loop.
         * These come after any expressions.
         */
        public int getLoopBindingsOffset();
    }

    public interface SubqueryOperatorAssembler {
        /** Assemble the given subquery expression. */
        public Expression assembleSubqueryExpression(SubqueryExpression subqueryExpression);
    }

    public Expression assembleExpression(ExpressionNode node,
                                         ColumnExpressionContext columnContext,
                                         SubqueryOperatorAssembler subqueryAssembler) {
        if (node instanceof ConstantExpression) {
            if (node.getAkType() == null)
                return literal(((ConstantExpression)node).getValue());
            else
                return literal(((ConstantExpression)node).getValue(),
                               node.getAkType());
        }
        else if (node instanceof ColumnExpression)
            return assembleColumnExpression((ColumnExpression)node, columnContext);
        else if (node instanceof ParameterExpression)
            return variable(node.getAkType(), ((ParameterExpression)node).getPosition());
        else if (node instanceof BooleanOperationExpression) {
            BooleanOperationExpression bexpr = (BooleanOperationExpression)node;
            return functionsRegistry
                .composer(bexpr.getOperation().getFunctionName())
                .compose(Arrays.asList(assembleExpression(bexpr.getLeft(), 
                                                          columnContext, 
                                                          subqueryAssembler),
                                       assembleExpression(bexpr.getRight(), 
                                                          columnContext,
                                                          subqueryAssembler)));
        }
        else if (node instanceof CastExpression)
            return assembleCastExpression((CastExpression)node,
                                          columnContext, subqueryAssembler);
        else if (node instanceof ComparisonCondition) {
            ComparisonCondition cond = (ComparisonCondition)node;
            return compare(assembleExpression(cond.getLeft(), 
                                              columnContext, subqueryAssembler),
                           cond.getOperation(),
                           assembleExpression(cond.getRight(), 
                                              columnContext, subqueryAssembler));
        }
        else if (node instanceof FunctionExpression) {
            FunctionExpression funcNode = (FunctionExpression)node;
            return functionsRegistry
                .composer(funcNode.getFunction())
                .compose(assembleExpressions(funcNode.getOperands(), 
                                             columnContext, subqueryAssembler));
        }
        else if (node instanceof IfElseExpression) {
            IfElseExpression ifElse = (IfElseExpression)node;
            return functionsRegistry
                .composer("if")
                .compose(Arrays.asList(assembleExpression(ifElse.getTestCondition(), 
                                                          columnContext, 
                                                          subqueryAssembler),
                                       assembleExpression(ifElse.getThenExpression(), 
                                                          columnContext,
                                                          subqueryAssembler),
                                       assembleExpression(ifElse.getElseExpression(), 
                                                          columnContext, 
                                                          subqueryAssembler)));
        }
        else if (node instanceof InListCondition) {
            InListCondition inList = (InListCondition)node;
            Expression lhs = assembleExpression(inList.getOperand(), 
                                                columnContext, subqueryAssembler);
            List<Expression> rhs = assembleExpressions(inList.getExpressions(),
                                                       columnContext, subqueryAssembler);
            return new InExpression(lhs, rhs);
        }
        else if (node instanceof SubqueryExpression)
            return subqueryAssembler.assembleSubqueryExpression((SubqueryExpression)node);
        else if (node instanceof AggregateFunctionExpression)
            throw new UnsupportedSQLException("Aggregate used as regular function", 
                                              node.getSQLsource());
        else if (node instanceof EnvironmentFunctionExpression) {
            EnvironmentFunctionExpression funcNode = (EnvironmentFunctionExpression)node;
            EnvironmentExpressionFactory factory = functionsRegistry.environment(funcNode.getFunction());
            return factory.get(columnContext.getExpressionBindingsOffset() + funcNode.getBindingPosition());
        }
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
                rowIndex += columnContext.getLoopBindingsOffset();
                return boundField(boundRow.getRowType(), rowIndex, fieldIndex);
            }
        }
        throw new AkibanInternalException("Column not found " + column);
    }

    protected Expression assembleCastExpression(CastExpression castExpression,
                                                ColumnExpressionContext columnContext,
                                                SubqueryOperatorAssembler subqueryAssembler) {
        ExpressionNode operand = castExpression.getOperand();
        Expression expr = assembleExpression(operand, columnContext, subqueryAssembler);
        AkType toType = castExpression.getAkType();
        if (toType == null) return expr;
        if (!toType.equals(operand.getAkType()))
        {
            // Do type conversion.         
            TypeId id = castExpression.getSQLtype().getTypeId(); 
            
            if (id.isIntervalTypeId())
                expr = new IntervalCastExpression(expr, IntervalCastExpression.ID_MAP.get(id));
            else 
                expr = new com.akiban.server.expression.std.CastExpression(toType, expr);
        }
        
        switch (toType) {
        case VARCHAR:
            {
                DataTypeDescriptor fromSQL = operand.getSQLtype();
                DataTypeDescriptor toSQL = castExpression.getSQLtype();
                if ((toSQL != null) &&
                    (toSQL.getMaximumWidth() > 0) &&
                    ((fromSQL == null) ||
                     (toSQL.getMaximumWidth() < fromSQL.getMaximumWidth())))
                    // Cast to shorter VARCHAR.
                    expr = new com.akiban.server.expression.std.TruncateStringExpression(toSQL.getMaximumWidth(), expr);
            }
            break;
        case DECIMAL:
            {
                DataTypeDescriptor fromSQL = operand.getSQLtype();
                DataTypeDescriptor toSQL = castExpression.getSQLtype();
                if ((toSQL != null) && !toSQL.equals(fromSQL))
                    // Cast to DECIMAL scale.
                    expr = new com.akiban.server.expression.std.ScaleDecimalExpression(toSQL.getPrecision(), toSQL.getScale(), expr);
            }
            break;
        }
        return expr;
    }

    protected List<Expression> assembleExpressions(List<ExpressionNode> expressions,
                                                   ColumnExpressionContext columnContext,
                                                   SubqueryOperatorAssembler subqueryAssembler) {
        List<Expression> result = new ArrayList<Expression>(expressions.size());
        for (ExpressionNode expr : expressions) {
            result.add(assembleExpression(expr, columnContext, subqueryAssembler));
        }
        return result;
    }

    public ConstantExpression evalNow(ExpressionNode node) {
        if (node instanceof ConstantExpression)
            return (ConstantExpression)node;
        Expression expr = assembleExpression(node, null, null);
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
