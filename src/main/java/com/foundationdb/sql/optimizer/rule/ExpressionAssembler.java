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

package com.foundationdb.sql.optimizer.rule;

import com.foundationdb.sql.optimizer.plan.AggregateFunctionExpression;
import com.foundationdb.sql.optimizer.plan.AggregateSource;
import com.foundationdb.sql.optimizer.plan.BooleanOperationExpression;
import com.foundationdb.sql.optimizer.plan.CastExpression;
import com.foundationdb.sql.optimizer.plan.ColumnExpression;
import com.foundationdb.sql.optimizer.plan.ComparisonCondition;
import com.foundationdb.sql.optimizer.plan.ConstantExpression;
import com.foundationdb.sql.optimizer.plan.ExpressionNode;
import com.foundationdb.sql.optimizer.plan.FunctionExpression;
import com.foundationdb.sql.optimizer.plan.IfElseExpression;
import com.foundationdb.sql.optimizer.plan.InListCondition;
import com.foundationdb.sql.optimizer.plan.ParameterExpression;
import com.foundationdb.sql.optimizer.plan.RoutineExpression;
import com.foundationdb.sql.optimizer.plan.SubqueryExpression;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.Explainable;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.explain.Type;
import com.foundationdb.server.types3.texpressions.Comparison;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

abstract class ExpressionAssembler<T extends Explainable> {

    public abstract ConstantExpression evalNow(PlanContext planContext, ExpressionNode node);
    final PlanContext planContext;
    final PlanExplainContext explainContext;

    protected ExpressionAssembler(PlanContext planContext) {
        this.planContext = planContext;
        if (planContext instanceof ExplainPlanContext)
            explainContext = ((ExplainPlanContext)planContext).getExplainContext();
        else
            explainContext = null;
    }

    protected abstract T assembleFunction(ExpressionNode functionNode,
                                          String functionName,
                                          List<ExpressionNode> argumentNodes,
                                          ColumnExpressionContext columnContext,
                                          SubqueryOperatorAssembler<T> subqueryAssembler);
    protected abstract  T assembleCastExpression(CastExpression castExpression,
                                                ColumnExpressionContext columnContext,
                                                SubqueryOperatorAssembler<T> subqueryAssembler);
    protected abstract T tryLiteral(ExpressionNode node);
    protected abstract T literal(ConstantExpression expression);
    protected abstract T variable(ParameterExpression expression);
    protected abstract T compare(T left, ComparisonCondition comparison, T right);
    protected abstract T collate(T left, Comparison comparison, T right, AkCollator collator);
    protected abstract AkCollator collator(ComparisonCondition cond, T left, T right);
    protected abstract T in(T lhs, List<T> rhs, InListCondition inList);
    protected abstract T assembleFieldExpression(RowType rowType, int fieldIndex);
    protected abstract T assembleBoundFieldExpression(RowType rowType, int rowIndex, int fieldIndex);
    protected abstract T assembleRoutine(ExpressionNode routineNode, 
                                         Routine routine,
                                         List<ExpressionNode> operandNodes,
                                         ColumnExpressionContext columnContext,
                                         SubqueryOperatorAssembler<T> subqueryAssembler);

    protected abstract Logger logger();

    public T assembleExpression(ExpressionNode node,
                         ColumnExpressionContext columnContext,
                         SubqueryOperatorAssembler<T> subqueryAssembler)  {
        T possiblyLiteral = tryLiteral(node);
        if (possiblyLiteral != null)
            return possiblyLiteral;
        if (node instanceof ConstantExpression)
            return literal(((ConstantExpression)node));
        else if (node instanceof ColumnExpression)
            return assembleColumnExpression((ColumnExpression)node, columnContext);
        else if (node instanceof ParameterExpression)
            return variable((ParameterExpression)node);
        else if (node instanceof BooleanOperationExpression) {
            BooleanOperationExpression bexpr = (BooleanOperationExpression)node;
            return assembleFunction(bexpr, bexpr.getOperation().getFunctionName(),
                    Arrays.<ExpressionNode>asList(bexpr.getLeft(), bexpr.getRight()),
                    columnContext, subqueryAssembler);
        }
        else if (node instanceof CastExpression)
            return assembleCastExpression((CastExpression)node,
                    columnContext, subqueryAssembler);
        else if (node instanceof ComparisonCondition) {
            ComparisonCondition cond = (ComparisonCondition)node;
            T left = assembleExpression(cond.getLeft(), columnContext, subqueryAssembler);
            T right = assembleExpression(cond.getRight(), columnContext, subqueryAssembler);
            // never use a collator if we have a KeyComparable
            AkCollator collator = (cond.getKeyComparable() == null) ? collator(cond, left, right) : null;
            if (collator != null)
                return collate(left, cond.getOperation(), right, collator);
            else
                return compare(left, cond, right);
        }
        else if (node instanceof FunctionExpression) {
            FunctionExpression funcNode = (FunctionExpression)node;
            return assembleFunction(funcNode, funcNode.getFunction(),
                    funcNode.getOperands(),
                    columnContext, subqueryAssembler);
        }
        else if (node instanceof IfElseExpression) {
            IfElseExpression ifElse = (IfElseExpression)node;
            return assembleFunction(ifElse, "if",
                    Arrays.asList(ifElse.getTestCondition(),
                            ifElse.getThenExpression(),
                            ifElse.getElseExpression()),
                    columnContext, subqueryAssembler);
        }
        else if (node instanceof InListCondition) {
            InListCondition inList = (InListCondition)node;
            T lhs = assembleExpression(inList.getOperand(),
                    columnContext, subqueryAssembler);
            List<T> rhs = assembleExpressions(inList.getExpressions(),
                    columnContext, subqueryAssembler);
            return in(lhs, rhs, inList);
        }
        else if (node instanceof RoutineExpression) {
            RoutineExpression routineNode = (RoutineExpression)node;
            return assembleRoutine(routineNode, routineNode.getRoutine(),
                                   routineNode.getOperands(),
                                   columnContext, subqueryAssembler);
        }
        else if (node instanceof SubqueryExpression)
            return subqueryAssembler.assembleSubqueryExpression((SubqueryExpression)node);
        else if (node instanceof AggregateFunctionExpression)
            throw new UnsupportedSQLException("Aggregate used as regular function",
                    node.getSQLsource());
        else
            throw new UnsupportedSQLException("Unknown expression", node.getSQLsource());
    }

    protected List<T> assembleExpressions(List<ExpressionNode> expressions,
                                                   ColumnExpressionContext columnContext,
                                                   SubqueryOperatorAssembler<T> subqueryAssembler) {
        List<T> result = new ArrayList<>(expressions.size());
        for (ExpressionNode expr : expressions) {
            result.add(assembleExpression(expr, columnContext, subqueryAssembler));
        }
        return result;
    }

    private T assembleColumnExpression(ColumnExpression column,
                                       ColumnExpressionContext columnContext) {
        ColumnExpressionToIndex currentRow = columnContext.getCurrentRow();
        if (currentRow != null) {
            int fieldIndex = currentRow.getIndex(column);
            if (fieldIndex >= 0)
            {
                T expression = assembleFieldExpression(currentRow.getRowType(), fieldIndex);
                if (explainContext != null)
                    explainColumnExpression(expression, column);
                return expression;
            }
        }

        List<ColumnExpressionToIndex> boundRows = columnContext.getBoundRows();
        for (int rowIndex = boundRows.size() - 1; rowIndex >= 0; rowIndex--) {
            ColumnExpressionToIndex boundRow = boundRows.get(rowIndex);
            if (boundRow == null) continue;
            int fieldIndex = boundRow.getIndex(column);
            if (fieldIndex >= 0) {
                rowIndex += columnContext.getLoopBindingsOffset();
                T expression = assembleBoundFieldExpression(boundRow.getRowType(), rowIndex, fieldIndex);
                if (explainContext != null)
                    explainColumnExpression(expression, column);
                return expression;
            }
        }
        logger().debug("Did not find {} from {} in {}",
                new Object[]{column, column.getTable(), boundRows});
        throw new AkibanInternalException("Column not found " + column);
    }

    private void explainColumnExpression(T expression, ColumnExpression column) {
        CompoundExplainer explainer = new CompoundExplainer(Type.EXTRA_INFO);
        explainer.addAttribute(Label.POSITION, 
                               PrimitiveExplainer.getInstance(column.getPosition()));
        Column aisColumn = column.getColumn();
        if (aisColumn != null) {
            explainer.addAttribute(Label.TABLE_CORRELATION, 
                                   PrimitiveExplainer.getInstance(column.getTable().getName()));
            TableName tableName = aisColumn.getTable().getName();
            explainer.addAttribute(Label.TABLE_SCHEMA,
                                   PrimitiveExplainer.getInstance(tableName.getSchemaName()));
            explainer.addAttribute(Label.TABLE_NAME,
                                   PrimitiveExplainer.getInstance(tableName.getTableName()));
            explainer.addAttribute(Label.COLUMN_NAME,
                                   PrimitiveExplainer.getInstance(aisColumn.getName()));
        }
        explainContext.putExtraInfo(expression, explainer);
    }

    public abstract Operator assembleAggregates(Operator inputOperator, RowType rowType, int nkeys,
                                            AggregateSource aggregateSource);

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

    public interface SubqueryOperatorAssembler<T> {
        /** Assemble the given subquery expression. */
        public T assembleSubqueryExpression(SubqueryExpression subqueryExpression);
    }
}
