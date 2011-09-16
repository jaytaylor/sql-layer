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

import com.akiban.sql.optimizer.plan.*;

import com.akiban.qp.expression.Expression;

import java.util.*;

/** Evaluate as much as possible at generate time. */
public class FoldConstants extends BaseRule 
{
    @Override
    public PlanNode apply(PlanNode plan) {
        Folder folder = new Folder();
        while (folder.apply(plan));
        return plan;
    }

    static class Folder implements PlanVisitor, ExpressionRewriteVisitor {
        private Set<ColumnSource> eliminatedSources = new HashSet<ColumnSource>();
        private boolean changed;

        /** Return <code>true</code> if substantial enough changes were made that
         * need to be run again.
         */
        public boolean apply(PlanNode plan) {
            changed = false;
            plan.accept(this);
            return changed;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            if (n instanceof Filter)
                filterNode((Filter)n);
            else if (n instanceof SubquerySource)
                subquerySource((SubquerySource)n);
            else if (n instanceof AggregateSource)
                aggregateSource((AggregateSource)n);
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            return true;
        }

        @Override
        public boolean visitChildrenFirst(ExpressionNode expr) {
            // This assumes that there is no particular advantage to
            // figuring out the whole expression tree of functions and
            // constants and passing it to eval as a whole, rather than
            // doing it piece-by-piece.
            return true;
        }

        @Override
        public ExpressionNode visit(ExpressionNode expr) {
            if (expr instanceof ComparisonCondition)
                return comparisonCondition((ComparisonCondition)expr);
            else if (expr instanceof CastExpression)
                return castExpression((CastExpression)expr);
            else if (expr instanceof FunctionExpression)
                return functionExpression((FunctionExpression)expr);
            else if (expr instanceof IfElseExpression)
                return ifElseExpression((IfElseExpression)expr);
            else if (expr instanceof ColumnExpression)
                return columnExpression((ColumnExpression)expr);
            else if (expr instanceof SubqueryExpression)
                return subqueryExpression((SubqueryExpression)expr);
            else if (expr instanceof SubqueryCondition)
                return subqueryCondition((SubqueryCondition)expr);
            else
                return expr;
        }

        protected ExpressionNode comparisonCondition(ComparisonCondition cond) {
            Constantness lc = isConstant(cond.getLeft());
            Constantness rc = isConstant(cond.getRight());
            if ((lc != Constantness.VARIABLE) && (rc != Constantness.VARIABLE))
                return evalNow(cond);
            if ((lc == Constantness.NULL) || (rc == Constantness.NULL))
                return new BooleanConstantExpression(null, 
                                                     cond.getSQLtype(), 
                                                     cond.getSQLsource());
            return cond;
        }

        protected ExpressionNode castExpression(CastExpression cast) {
            Constantness c = isConstant(cast.getOperand());
            if (c != Constantness.VARIABLE)
                return evalNow(cast);
            return cast;
        }

        protected ExpressionNode functionExpression(FunctionExpression fun) {
            String fname = fun.getFunction();
            if ("isNullOp".equals(fname))
                return isNullExpression(fun);
            else if ("COALESCE".equals(fname))
                return coalesceExpression(fun);

            boolean allConstant = true, anyNull = false;
            for (ExpressionNode operand : fun.getOperands()) {
                switch (isConstant(operand)) {
                case NULL:
                    anyNull = true;
                    /* falls through */
                case VARIABLE:
                    allConstant = false;
                    break;
                }
            }
            if (allConstant && !isVolatile(fun))
                return evalNow(fun);
            // All the functions that treat NULL specially are caught before we get here.
            if (anyNull)
                return new BooleanConstantExpression(null, 
                                                     fun.getSQLtype(), 
                                                     fun.getSQLsource());
            return fun;
        }

        protected boolean isVolatile(FunctionExpression fun) {
            // TODO: Nice to get this from some functions repository
            // associated with their implementations.
            String fname = fun.getFunction();
            return ("currentDate".equals(fname) ||
                    "currentTime".equals(fname) ||
                    "currentTimestamp".equals(fname) ||
                    "RAND".equals(fname));
        }

        protected ExpressionNode isNullExpression(FunctionExpression fun) {
            ExpressionNode operand = fun.getOperands().get(0);
            if (isConstant(operand) != Constantness.VARIABLE)
                return evalNow(fun);
            // pkey IS NULL is FALSE, for instance.
            if ((operand.getSQLtype() != null) &&
                !operand.getSQLtype().isNullable())
                return new BooleanConstantExpression(Boolean.FALSE, 
                                                     fun.getSQLtype(), 
                                                     fun.getSQLsource());
            return fun;
        }

        protected ExpressionNode coalesceExpression(FunctionExpression fun) {
            // Don't need all the operands to make progress.
            List<ExpressionNode> operands = fun.getOperands();
            int i = 0;
            while (i < operands.size()) {
                ExpressionNode operand = operands.get(i);
                Constantness c = isConstant(operand);
                if (c == Constantness.NULL) {
                    operands.remove(i);
                    continue;
                }
                if (c == Constantness.CONSTANT) {
                    // If the first arg is a not-null constant, that's the answer.
                    if (i == 0) return operand;
                }
                i++;
            }
            if (operands.isEmpty())
                return new BooleanConstantExpression(null, 
                                                     fun.getSQLtype(), 
                                                     fun.getSQLsource());
            return fun;
        }

        protected ExpressionNode ifElseExpression(IfElseExpression cond) {
            Constantness c = isConstant(cond.getTestCondition());
            if (c == Constantness.VARIABLE)
                return cond;
            if (((ConstantExpression)cond.getTestCondition()).getValue() == Boolean.TRUE)
                return cond.getThenExpression();
            else
                return cond.getElseExpression();
        }

        protected ExpressionNode columnExpression(ColumnExpression col) {
            if (eliminatedSources.contains(col.getTable()))
                // TODO: Could do a new ColumnExpression with the
                // NullSource that replaced it, but then that'd have
                // to eval specially.
                return new ConstantExpression(null,
                                              col.getSQLtype(), col.getSQLsource());
            return col;
        }

        protected void filterNode(Filter filter) {
            boolean keep = checkConditions(filter.getConditions());
            if (keep && (filter.getInput() instanceof Joinable)) {
                Joinable input = (Joinable)filter.getInput();
                input = checkOuterJoins(input);
                if (input == null)
                    keep = false;
                else
                    filter.setInput(input);
            }
            if (!keep) {
                eliminateSources(filter.getInput());
                PlanWithInput inOutput = filter;
                PlanNode toReplace = null;
                while ((inOutput instanceof Filter) ||
                       (inOutput instanceof Sort)) {
                    // TODO: Also aggregate with GROUP BY. 
                    // Without GROUP BY, need special handling for COUNT to
                    // give 0 rather than NULL.
                    toReplace = inOutput;
                    inOutput = toReplace.getOutput();
                }
                inOutput.replaceInput(toReplace, new NullSource());
            }
        }

        protected Joinable checkOuterJoins(Joinable joinable) {
            if (eliminatedSources.contains(joinable))
                return null;
            if (joinable instanceof JoinNode) {
                JoinNode join = (JoinNode)joinable;
                Joinable left = checkOuterJoins(join.getLeft());
                Joinable right = checkOuterJoins(join.getRight());
                if (!checkConditions(join.getJoinConditions())) {
                    // Join cannot be satified.
                    switch (join.getJoinType()) {
                    case INNER_JOIN:
                        return null;
                    case LEFT_JOIN:
                        eliminateSources(right);
                        return left;
                    case RIGHT_JOIN:
                        eliminateSources(left);
                        return right;
                    }
                }
                if (left == null)
                    return right;
                if (right == null)
                    return left;
                join.setLeft(left);
                join.setRight(right);
            }
            return joinable;
        }

        protected void eliminateSources(PlanNode node) {
            if (node instanceof ColumnSource) {
                eliminateSource((ColumnSource)node);
            }
            if (node instanceof BasePlanWithInput) {
                eliminateSources(((BasePlanWithInput)node).getInput());
            }
            else if (node instanceof JoinNode) {
                JoinNode join = (JoinNode)node;
                eliminateSources(join.getLeft());
                eliminateSources(join.getRight());
            }
        }

        protected void eliminateSource(ColumnSource source) {
            eliminatedSources.add(source);
            // Need to find all the references to it, which means another pass.
            changed = true;
        }

        /** Returns <code>false</code> if it's impossible for these
         * conditions to be satisfied. */
        protected boolean checkConditions(List<ConditionExpression> conditions) {
            if (conditions == null) return true;
            int i = 0;
            while (i < conditions.size()) {
                ConditionExpression condition = conditions.get(i);
                Constantness c = isConstant(condition);
                if (c != Constantness.VARIABLE) {
                    if (((ConstantExpression)condition).getValue() == Boolean.TRUE)
                        conditions.remove(i);
                    else
                        return false;
                }
                i++;
            }
            return true;
        }

        protected ExpressionNode subqueryExpression(SubqueryExpression expr) {
            if (isNullSubquery(expr.getSubquery())) {
                return new ConstantExpression(null,
                                              expr.getSQLtype(), expr.getSQLsource());
            }
            return expr;
        }

        protected ExpressionNode subqueryCondition(SubqueryCondition cond) {
            if (isNullSubquery(cond.getSubquery())) {
                return new BooleanConstantExpression((cond.getKind() == SubqueryCondition.Kind.NOT_EXISTS),
                                                     cond.getSQLtype(), cond.getSQLsource());
            }
            return cond;
        }
    
        protected void subquerySource(SubquerySource source) {
            if (isNullSubquery(source.getSubquery())) {
                eliminateSource(source);
            }            
        }

        protected boolean isNullSubquery(Subquery subquery) {
            return (subquery.getInput() instanceof NullSource);
        }

        protected void aggregateSource(AggregateSource aggr) {
            // TODO: SUM(NULL) is NULL, COUNT(NULL) is 0,
            // COUNT(not nullable) is COUNT(*).
        }

        protected static enum Constantness { VARIABLE, CONSTANT, NULL }

        protected Constantness isConstant(ExpressionNode expr) {
            if (expr.isConstant())
                return ((((ConstantExpression)expr).getValue() == null) ? 
                        Constantness.NULL :
                        Constantness.CONSTANT);
            else 
                return Constantness.VARIABLE;
        }

        protected ExpressionNode evalNow(ExpressionNode node) {
            try {
                Expression expr = node.generateExpression(null);
                Object value = expr.evaluate(null, null);
                if (node instanceof ConditionExpression)
                    return new BooleanConstantExpression((Boolean)value, 
                                                         node.getSQLtype(), 
                                                         node.getSQLsource());
                else
                    return new ConstantExpression(value, 
                                                  node.getSQLtype(), 
                                                  node.getSQLsource());
            }
            catch (Exception ex) {
            }
            return node;
        }
    }
}
