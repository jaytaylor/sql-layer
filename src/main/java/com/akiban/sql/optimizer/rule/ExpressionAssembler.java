/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.optimizer.rule;

import com.akiban.qp.rowtype.RowType;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.expression.std.Comparison;
import com.akiban.sql.optimizer.plan.AggregateFunctionExpression;
import com.akiban.sql.optimizer.plan.BooleanOperationExpression;
import com.akiban.sql.optimizer.plan.CastExpression;
import com.akiban.sql.optimizer.plan.ColumnExpression;
import com.akiban.sql.optimizer.plan.ComparisonCondition;
import com.akiban.sql.optimizer.plan.ConstantExpression;
import com.akiban.sql.optimizer.plan.ExpressionNode;
import com.akiban.sql.optimizer.plan.FunctionExpression;
import com.akiban.sql.optimizer.plan.IfElseExpression;
import com.akiban.sql.optimizer.plan.InListCondition;
import com.akiban.sql.optimizer.plan.ParameterExpression;
import com.akiban.sql.optimizer.plan.SubqueryExpression;

import java.util.Arrays;
import java.util.List;

abstract class  ExpressionAssembler<T> {

    protected abstract T assembleFunction(ExpressionNode functionNode,
                                          String functionName,
                                          List<ExpressionNode> argumentNodes,
                                          ColumnExpressionContext columnContext,
                                          SubqueryOperatorAssembler<T> subqueryAssembler);
    protected abstract T assembleColumnExpression(ColumnExpression column,
                                                  ColumnExpressionContext columnContext);
    protected abstract  T assembleCastExpression(CastExpression castExpression,
                                                ColumnExpressionContext columnContext,
                                                SubqueryOperatorAssembler<T> subqueryAssembler);
    protected abstract List<T> assembleExpressions(List<ExpressionNode> expressions,
                                                   ColumnExpressionContext columnContext,
                                                   SubqueryOperatorAssembler<T> subqueryAssembler);
    protected abstract T literal(ConstantExpression expression);
    protected abstract T variable(ParameterExpression expression);
    protected abstract T compare(T left, Comparison comparison, T right);
    protected abstract T collate(T left, Comparison comparison, T right, AkCollator collator);
    protected abstract AkCollator collator(ComparisonCondition cond, T left, T right);
    protected abstract T in(T lhs, List<T> rhs);

    public T assembleExpression(ExpressionNode node,
                         ColumnExpressionContext columnContext,
                         SubqueryOperatorAssembler<T> subqueryAssembler)  {
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
            AkCollator collator = collator(cond, left, right);
            if (collator != null)
                return collate(left, cond.getOperation(), right, collator);
            else
                return compare(left, cond.getOperation(), right);
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
            return in(lhs, rhs);
        }
        else if (node instanceof SubqueryExpression)
            return subqueryAssembler.assembleSubqueryExpression((SubqueryExpression)node);
        else if (node instanceof AggregateFunctionExpression)
            throw new UnsupportedSQLException("Aggregate used as regular function",
                    node.getSQLsource());
        else
            throw new UnsupportedSQLException("Unknown expression", node.getSQLsource());
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

    public interface SubqueryOperatorAssembler<T> {
        /** Assemble the given subquery expression. */
        public T assembleSubqueryExpression(SubqueryExpression subqueryExpression);
    }
}
