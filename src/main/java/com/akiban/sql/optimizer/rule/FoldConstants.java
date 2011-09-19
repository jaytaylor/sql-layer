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

/** Evaluate as much as possible at generate time. 
 * As with any compiler, false constants used in conditions can lead
 * to dead code, that is, join sources that don't need to bother
 * outputting any data. And these empty data sets can in turn affect
 * subqueries and aggregation.
 */
public class FoldConstants extends BaseRule 
{
    @Override
    public PlanNode apply(PlanNode plan) {
        Folder folder = new Folder();
        while (folder.foldConstants(plan));
        folder.finishAggregates(plan);
        return plan;
    }

    static class Folder implements PlanVisitor, ExpressionRewriteVisitor {
        private Set<ColumnSource> eliminatedSources = new HashSet<ColumnSource>();
        private Set<AggregateSource> changedAggregates = null;
        private enum State { FOLDING, AGGREGATES };
        private State state;
        private boolean changed;

        /** Return <code>true</code> if substantial enough changes were made that
         * need to be run again.
         */
        public boolean foldConstants(PlanNode plan) {
            state = State.FOLDING;
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
            if (state == State.FOLDING) {
                if (n instanceof Filter)
                    filterNode((Filter)n);
                else if (n instanceof SubquerySource)
                    subquerySource((SubquerySource)n);
                else if (n instanceof AggregateSource)
                    aggregateSource((AggregateSource)n);
            }
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
            if (state == State.FOLDING) {
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
            }
            else if (state == State.AGGREGATES) {
                if (expr instanceof ColumnExpression)
                    return columnExpression((ColumnExpression)expr);
            }
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
            ColumnSource source = col.getTable();
            if (source instanceof AggregateSource) {
                AggregateSource asource = (AggregateSource)source;
                int apos = col.getPosition() - asource.getGroupBy().size();
                if (apos >= 0) {
                    List<AggregateFunctionExpression> afuns = asource.getAggregates();
                    AggregateFunctionExpression afun = afuns.get(apos);
                    if (state == State.FOLDING) {
                        boolean ok = eliminatedSources.contains(asource);
                        if (!ok) {
                            if (isAggregateOfNull(afun)) {
                                ok = true;
                                changedAggregates = new HashSet<AggregateSource>();
                                changedAggregates.add(asource);
                            }
                        }
                        if (ok) {
                            // This is an aggregate of a NULL value or with no inputs.
                            // That can be NULL or 0 for COUNT.
                            Object value = null;
                            if (isAggregateZero(afun))
                                value = Integer.valueOf(0);
                            return new ConstantExpression(value,
                                                          col.getSQLtype(), 
                                                          col.getSQLsource());
                        }
                    }
                    else if (state == State.AGGREGATES) {
                        if (changedAggregates.contains(asource) &&
                            !eliminatedSources.contains(source)) {
                            // Adjust position for any null functions
                            // which are about to get
                            // removed. References to these positions
                            // were replaced by constants some time
                            // ago.
                            int delta = 0;
                            for (int i = 0; i < apos; i++) {
                                if (isAggregateOfNull(afuns.get(i))) {
                                    delta++;
                                }
                            }
                            if (delta > 0)
                                col.setPosition(col.getPosition() - delta);
                        }
                    }
                }
            }
            if (state == State.FOLDING) {
                if (eliminatedSources.contains(source))
                    // TODO: Could do a new ColumnExpression with the
                    // NullSource that replaced it, but then that'd have
                    // to eval specially.
                    return new ConstantExpression(null,
                                                  col.getSQLtype(), col.getSQLsource());
            }
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
                PlanNode toReplace = filter;
                PlanWithInput inOutput = toReplace.getOutput();
                if (inOutput instanceof Sort) {
                    toReplace = inOutput;
                    inOutput = toReplace.getOutput();
                }
                boolean emptyRow = false;
                if (inOutput instanceof AggregateSource) {
                    toReplace = inOutput;
                    inOutput = toReplace.getOutput();
                    eliminatedSources.add((ColumnSource)toReplace);
                    // No GROUP BY outputs an answer for no inputs.
                    emptyRow = ((AggregateSource)toReplace).getGroupBy().isEmpty();
                }
                PlanNode replacement;
                if (!emptyRow)
                    replacement = new NullSource();
                else {
                    replacement = new ExpressionsSource(Collections.singletonList(Collections.<ExpressionNode>emptyList()));
                }
                inOutput.replaceInput(toReplace, replacement);
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
            PlanNode node = subquery;
            while ((node instanceof Subquery) ||
                   (node instanceof ResultSet))
                node = ((BasePlanWithInput)node).getInput();
            return (node instanceof NullSource);
        }

        public void finishAggregates(PlanNode plan) {
            if (changedAggregates == null) return;
            state = State.AGGREGATES;
            plan.accept(this);
            // Now that all the indexes are fixed, we can finally
            // remove the precomputed aggregate results.
            for (AggregateSource asource : changedAggregates) {
                List<AggregateFunctionExpression> afuns = asource.getAggregates();
                int i = 0;
                while (i < afuns.size()) {
                    AggregateFunctionExpression afun = afuns.get(i);
                    if (isAggregateOfNull(afun)) {
                        afuns.remove(i);
                        continue;
                    }
                    i++;
                }
            }
        }
        
        protected void aggregateSource(AggregateSource aggr) {
            // TODO: Check nullity of outer join result. Should be
            // added even if not on column.
            for (AggregateFunctionExpression afun : aggr.getAggregates()) {
                if ((afun.getOperand() != null) &&
                    !afun.isDistinct() &&
                    "COUNT".equals(afun.getFunction()) &&
                    ((isConstant(afun.getOperand()) == Constantness.CONSTANT) ||
                     ((afun.getOperand().getSQLtype() != null) &&
                      !afun.getOperand().getSQLtype().isNullable()))) {
                    // COUNT(constant or NOT NULL) -> COUNT(*).
                    afun.setOperand(null);
                }
            }
        }

        protected boolean isAggregateOfNull(AggregateFunctionExpression afun) {
            return ((afun.getOperand() != null) &&
                    (isConstant(afun.getOperand()) == Constantness.NULL));
        }

        protected boolean isAggregateZero(AggregateFunctionExpression afun) {
            return ("COUNT".equals(afun.getFunction()));
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
