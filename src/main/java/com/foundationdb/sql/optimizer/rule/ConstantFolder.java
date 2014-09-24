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

import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.sql.optimizer.plan.*;
import com.foundationdb.sql.optimizer.plan.ExpressionsSource.DistinctState;
import com.foundationdb.sql.optimizer.rule.TypeResolver.ResolvingVisitor;

import com.foundationdb.server.types.texpressions.Comparison;

import com.foundationdb.ais.model.Routine;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.types.service.TCastResolver;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.common.types.TypesTranslator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Evaluate as much as possible at generate time. 
 * As with any compiler, false constants used in conditions can lead
 * to dead code, that is, join sources that don't need to bother
 * outputting any data. And these empty data sets can in turn affect
 * subqueries and aggregation.
 */
public class ConstantFolder extends BaseRule 
{
    private static final Logger logger = LoggerFactory.getLogger(ConstantFolder.class);

    public ConstantFolder() {
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext planContext) {
        Folder folder = new Folder(planContext);
        while (folder.foldConstants());
        folder.finishAggregates();
    }

    public static class Folder implements PlanVisitor, ExpressionRewriteVisitor {
        protected final PlanContext planContext;
        protected final ExpressionAssembler expressionAssembler;
        private Set<ColumnSource> eliminatedSources = new HashSet<>();
        private Set<AggregateSource> changedAggregates = null;
        private enum State { FOLDING, AGGREGATES, FOLDING_PIECEMEAL };
        private State state;
        private boolean changed;
        private Map<ConditionExpression,Boolean> topLevelConditions = 
            new IdentityHashMap<>();
        private ExpressionRewriteVisitor resolvingVisitor;

        public Folder(PlanContext planContext) {
            this.planContext = planContext;
            this.expressionAssembler = new ExpressionAssembler(planContext);
        }
        
        public void initResolvingVisitor(ExpressionRewriteVisitor resolvingVisitor) {
            this.resolvingVisitor = resolvingVisitor;
        }

        public ExpressionNode foldConstants(ExpressionNode fromNode) {
            do {
                state = State.FOLDING_PIECEMEAL;
                changed = false;
                topLevelConditions.clear();
                fromNode = fromNode.accept(this);
            } while (changed);
            return fromNode;
        }

        /** Return <code>true</code> if substantial enough changes were made that
         * need to be run again.
         */
        public boolean foldConstants() {
            state = State.FOLDING;
            changed = false;
            topLevelConditions.clear();
            planContext.accept(this);
            return changed;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            if (state == State.FOLDING) {
                if (n instanceof Select)
                    selectNode((Select)n);
                else if (n instanceof SubquerySource)
                    subquerySource((SubquerySource)n);
                else if (n instanceof AggregateSource)
                    aggregateSource((AggregateSource)n);
            }
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            if (n instanceof Select) {
                for (ConditionExpression condition : ((Select)n).getConditions()) {
                    topLevelConditions.put(condition, Boolean.TRUE);
                }
            }
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
            if ((state == State.FOLDING) || (state == State.FOLDING_PIECEMEAL)) {
                if (expr instanceof ComparisonCondition)
                    return comparisonCondition((ComparisonCondition)expr);
                else if (expr instanceof CastExpression)
                    return castExpression((CastExpression)expr);
                else if (expr instanceof FunctionExpression) {
                    if (expr instanceof LogicalFunctionCondition)
                        return logicalFunctionCondition((LogicalFunctionCondition)expr);
                    else
                        return functionExpression((FunctionExpression)expr);
                }
                else if (expr instanceof IfElseExpression)
                    return ifElseExpression((IfElseExpression)expr);
                else if (expr instanceof ColumnExpression)
                    return columnExpression((ColumnExpression)expr);
                else if (expr instanceof SubqueryValueExpression)
                    return subqueryValueExpression((SubqueryValueExpression)expr);
                else if (expr instanceof ExistsCondition)
                    return existsCondition((ExistsCondition)expr);
                else if (expr instanceof AnyCondition)
                    return anyCondition((AnyCondition)expr);
                else if (expr instanceof RoutineExpression)
                    return routineExpression((RoutineExpression)expr);
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
            if ((lc == Constantness.NULL) || (rc == Constantness.NULL))
                return newBooleanConstant(null, cond);
            if ((lc != Constantness.VARIABLE) && (rc != Constantness.VARIABLE))
                return evalNow(cond);
            if (isIdempotentEquality(cond)) {
                if ((cond.getLeft().getSQLtype() != null) &&
                    !cond.getLeft().getSQLtype().isNullable()) {
                    return newBooleanConstant(Boolean.TRUE, cond);
                }
            }
            return cond;
        }
        
        protected boolean isIdempotentEquality(ComparisonCondition cond) {
            if (cond.getOperation() != Comparison.EQ)
                return false;
            ExpressionNode left = cond.getLeft();
            if (!left.equals(cond.getRight()))
                return false;
            if ((left instanceof FunctionExpression) &&
                !isIdempotent((FunctionExpression)left))
                return false;
            return true;
        }

        protected ExpressionNode castExpression(CastExpression cast) {
            Constantness c = isConstant(cast.getOperand());
            if (c != Constantness.VARIABLE)
                return evalNow(cast);
            return cast;
        }

        protected ExpressionNode functionExpression(FunctionExpression fun) {
            String fname = fun.getFunction();
            if ("isNull".equals(fname) || 
                "isUnknown".equals(fname))
                return isNullExpression(fun);
            else if ("isTrue".equals(fname))
                return isTrueExpression(fun);
            else if ("isFalse".equals(fname))
                return isFalseExpression(fun);
            else if ("COALESCE".equals(fname) || "ifnull".equals(fname))
                return coalesceExpression(fun);
            else if ("if".equals(fname))
                return ifFunction(fun);

            return genericFunctionExpression(fun);
        }

        protected ConditionExpression logicalFunctionCondition(LogicalFunctionCondition lfun) {
            String fname = lfun.getFunction();
            if ("and".equals(fname)) {
                ConditionExpression left = lfun.getLeft();
                ConditionExpression right = lfun.getRight();
                if (isConstant(left) == Constantness.CONSTANT) {
                    boolean lv = checkConstantBoolean(left);
                    if (lv)
                        return right; // TRUE AND X -> X
                    else
                        return left; // FALSE AND X -> FALSE
                }
                if (isConstant(right) == Constantness.CONSTANT) {
                    boolean rv = checkConstantBoolean(right);
                    if (rv)
                        return left; // X AND TRUE -> X
                    else
                        return right; // X AND FALSE -> FALSE
                }
            }
            else if ("or".equals(fname)) {
                ConditionExpression left = lfun.getLeft();
                ConditionExpression right = lfun.getRight();
                if (isConstant(left) == Constantness.CONSTANT) {
                    boolean lv = checkConstantBoolean(left);
                    if (lv)
                        return left; // TRUE OR X -> TRUE
                    else
                        return right; // FALSE OR X -> X
                }
                if (isConstant(right) == Constantness.CONSTANT) {
                    boolean rv = checkConstantBoolean(right);
                    if (rv)
                        return right; // X OR TRUE -> TRUE
                    else
                        return left; // X OR FALSE -> X
                }
            }
            else if ("not".equals(fname)) {
                ConditionExpression cond = lfun.getOperand();
                Constantness c = isConstant(cond);
                if (c == Constantness.NULL)
                    return newBooleanConstant(null, lfun);
                if (c == Constantness.CONSTANT)
                    return newBooleanConstant(!checkConstantBoolean((ConstantExpression)cond), lfun);
            }
            return lfun;
        }

        protected boolean isIdempotent(FunctionExpression fun) {
            // TODO: Nice to get this from some functions repository
            // associated with their implementations.
            // Why do we even need this? Why can't we just know if the function is const?
            String fname = fun.getFunction();
            return !("currentDate".equals(fname) ||
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
                return newBooleanConstant(Boolean.FALSE, fun);
            return fun;
        }

        protected ExpressionNode isTrueExpression(FunctionExpression fun) {
            ExpressionNode operand = fun.getOperands().get(0);
            if (isConstant(operand) != Constantness.VARIABLE)
                return evalNow(fun);
            else if (isFalseOrUnknown((ConditionExpression)operand))
                return newBooleanConstant(Boolean.FALSE, fun);
            else
                return fun;
        }

        protected ExpressionNode isFalseExpression(FunctionExpression fun) {
            ExpressionNode operand = fun.getOperands().get(0);
            if (isConstant(operand) != Constantness.VARIABLE)
                return evalNow(fun);
            else if (isTrueOrUnknown((ConditionExpression)operand))
                return newBooleanConstant(Boolean.FALSE, fun);
            else
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
                return newBooleanConstant(null, fun);
            return fun;
        }

        protected ExpressionNode ifElseExpression(IfElseExpression cond) {
            boolean keep = checkConditions(cond.getTestConditions());
            if (!keep)
                return cond.getElseExpression();
            else if (cond.getTestConditions().isEmpty())
                return cond.getThenExpression();
            else
                return cond;
        }

        protected ExpressionNode ifFunction(FunctionExpression fun) {
            List<ExpressionNode> operands = fun.getOperands();
            Constantness c = isConstant(operands.get(0));
            if (c != Constantness.VARIABLE) {
              if (checkConstantBoolean(operands.get(0)))
                return operands.get(1);
              else
                return operands.get(2);
            }
            return fun;
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
                                changedAggregates = new HashSet<>();
                                changedAggregates.add(asource);
                            }
                        }
                        if (ok) {
                            // This is an aggregate of a NULL value or with no inputs.
                            // That can be NULL or 0 for COUNT.
                            Object value = null;
                            if (isAggregateZero(afun))
                                value = Long.valueOf(0);
                            return newConstant(value, col);
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
                    return newConstant(null, col);
            }
            return col;
        }

        protected ExpressionNode routineExpression(RoutineExpression expr) {
            Routine routine = expr.getRoutine();
            boolean allConstant = true, anyNull = false;
            for (ExpressionNode operand : expr.getOperands()) {
                switch (isConstant(operand)) {
                case NULL:
                    anyNull = true;
                    /* falls through */
                case VARIABLE:
                    allConstant = false;
                    break;
                }
            }
            if (allConstant && routine.isDeterministic())
                return evalNow(expr);
            if (anyNull && !routine.isCalledOnNullInput()) {
                return newBooleanConstant(null, expr);
            }
            return expr;
        }

        protected void selectNode(Select select) {
            boolean keep = checkConditions(select.getConditions());
            if (keep && (select.getInput() instanceof Joinable)) {
                Joinable input = (Joinable)select.getInput();
                input = checkOuterJoins(input);
                if (input == null)
                    keep = false;
                else
                    select.setInput(input);
            }
            if (!keep) {
                eliminateSources(select.getInput());
                PlanNode toReplace = select;
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
                    // set iTinstance types? No. 
                    replacement = new ExpressionsSource(Collections.singletonList(Collections.<ExpressionNode>emptyList()));
                    ResolvingVisitor visitor = (ResolvingVisitor) TypeResolver.getResolver(planContext);
                    if (visitor != null) visitor.visitLeave(replacement);
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
                    case INNER:
                        return null;
                    case LEFT:
                        eliminateSources(right);
                        return left;
                    case RIGHT:
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
         * conditions to be satisfied.
         * Only valid when conditions are being tested, not when used
         * as a value.
         */
        protected boolean checkConditions(ConditionList conditions) {
            if (conditions == null) return true;
            int i = 0;
            while (i < conditions.size()) {
                ConditionExpression condition = conditions.get(i);
                Constantness c = isConstant(condition);
                if (c != Constantness.VARIABLE) {
                    if (checkConstantBoolean(condition))
                        conditions.remove(i);
                    else
                        return false;
                }
                else if (isFalseOrUnknown(condition))
                    return false;
                else if (condition instanceof LogicalFunctionCondition) {
                    // A complex boolean may have been reduced to a top-level AND.
                    LogicalFunctionCondition lcond = (LogicalFunctionCondition)condition;
                    if ("and".equals(lcond.getFunction())) {
                        conditions.set(i, lcond.getLeft());
                        conditions.add(i+1, lcond.getRight());
                        continue; // Might be AND(AND(...
                    }
                }
                i++;
            }
            return true;
        }

        /** Returns <code>true</code> if the given expression will always evaluate
         * to either <i>true</i> or <i>unknown</i>.
         */
        protected boolean isTrueOrUnknown(ConditionExpression expr) {
            if (expr instanceof ConstantExpression) {
                Boolean value = getBooleanObject((ConstantExpression)expr);
                return ((value == null) ||
                        (value == Boolean.TRUE));
            }
            else if (expr instanceof LogicalFunctionCondition) {
                LogicalFunctionCondition lfun = (LogicalFunctionCondition)expr;
                String fname = lfun.getFunction();
                if ("and".equals(fname)) {
                    return (isTrueOrUnknown(lfun.getLeft()) &&
                            isTrueOrUnknown(lfun.getRight()));
                }
                else if ("or".equals(fname)) {
                    return (isTrueOrUnknown(lfun.getLeft()) ||
                            isTrueOrUnknown(lfun.getRight()));
                }
                else if ("not".equals(fname)) {
                    return isFalseOrUnknown(lfun.getOperand());
                }
            }
            else if (expr instanceof ComparisonCondition) {
                return isIdempotentEquality((ComparisonCondition)expr);
            }
            return false;
        }

        /** Returns <code>true</code> if the given expression will always evaluate
         * to either <i>false</i> or <i>unknown</i>.
         */
        protected boolean isFalseOrUnknown(ConditionExpression expr) {
            if (expr instanceof ConstantExpression) {
                Boolean value = getBooleanObject((ConstantExpression)expr);
                return ((value == null) ||
                        (value == Boolean.FALSE));
            }
            else if (expr instanceof LogicalFunctionCondition) {
                LogicalFunctionCondition lfun = (LogicalFunctionCondition)expr;
                String fname = lfun.getFunction();
                if ("and".equals(fname)) {
                    return (isFalseOrUnknown(lfun.getLeft()) ||
                            isFalseOrUnknown(lfun.getRight()));
                }
                else if ("or".equals(fname)) {
                    return (isFalseOrUnknown(lfun.getLeft()) &&
                            isFalseOrUnknown(lfun.getRight()));
                }
                else if ("not".equals(fname)) {
                    return isTrueOrUnknown(lfun.getOperand());
                }
            }
            else if (expr instanceof AnyCondition) {
                ExpressionNode inner = getSubqueryColumn(((AnyCondition)expr).getSubquery());
                if (inner instanceof ConditionExpression)
                    // Will be false if empty and whatever this is otherwise.
                    // E.g., NULL IN (SELECT ...)
                    return isFalseOrUnknown((ConditionExpression)inner);
            }
            return false;
        }

        protected boolean isTopLevelCondition(ConditionExpression cond) {
            return (topLevelConditions.get(cond) == Boolean.TRUE);
        }

        protected ExpressionNode subqueryValueExpression(SubqueryValueExpression expr) {
            SubqueryEmptiness empty = isEmptySubquery(expr.getSubquery());
            if (empty == SubqueryEmptiness.EMPTY) {
                return newConstant(null, expr);
            }
            ExpressionNode inner = getSubqueryColumn(expr.getSubquery());
            if (inner != null) {
                Constantness ic = isConstant(inner);
                if (ic == Constantness.NULL) {
                    // If it's empty, it's NULL. 
                    // If it selects something, that projects NULL.
                    // NULL either way.
                    return newConstant(null, expr);
                }
                else if ((ic == Constantness.CONSTANT) &&
                         (empty == SubqueryEmptiness.NON_EMPTY)) {
                    // If the inner is a constant and it's driven by a
                    // VALUES, know the value it must generate.  It
                    // would be possible to run the whole subquery if
                    // a variable expression only depended on fields
                    // from the VALUES, but that's getting to be a lot
                    // of setup.
                    return inner;
                }
            }
            return expr;
        }

        protected ExpressionNode existsCondition(ExistsCondition cond) {
            if (isEmptySubquery(cond.getSubquery()) == SubqueryEmptiness.EMPTY) {
                // Empty EXISTS is false.
                return newBooleanConstant(Boolean.FALSE, cond);
            }
            return cond;
        }
    
        protected ExpressionNode anyCondition(AnyCondition cond) {
            SubqueryEmptiness empty = isEmptySubquery(cond.getSubquery());
            if (empty == SubqueryEmptiness.EMPTY) {
                // Empty ANY is false.
                return newBooleanConstant(Boolean.FALSE, cond);
            }
            ExpressionNode inner = getSubqueryColumn(cond.getSubquery());
            if ((inner != null) &&
                (isConstant(inner) == Constantness.CONSTANT) &&
                ((empty == SubqueryEmptiness.NON_EMPTY) ||
                 (!checkConstantBoolean(inner)))) {
                // Constant false: if it's empty, it's false. If it
                // selects something, that projects false. False
                // either way.
                // Constant true: if it's known non-empty, that's what
                // is returned.
                return inner;
            }

            InCondition in = InCondition.of(cond, planContext);
            if (in != null) {
                in.dedup(isTopLevelCondition(cond), (state == State.FOLDING));
                switch (in.compareConstants(this)) {
                case COMPARE_NULL:
                    return newBooleanConstant(null, cond);
                case ROW_EQUALS:
                    return newBooleanConstant(Boolean.TRUE, cond);
                }
                if (in.isEmpty())
                    return newBooleanConstant(Boolean.FALSE, cond);
                if (in.isSingleton()) {
                    return in.buildCondition(in.getSingleton(), this);
                }
            }
            return cond;
        }
    
        protected void subquerySource(SubquerySource source) {
            if (isEmptySubquery(source.getSubquery()) == SubqueryEmptiness.EMPTY) {
                eliminateSource(source);
            }            
        }

        static enum SubqueryEmptiness {
            UNKNOWN, EMPTY, NON_EMPTY
        }

        protected SubqueryEmptiness isEmptySubquery(Subquery subquery) {
            PlanNode node = subquery.getQuery();
            if (node instanceof ResultSet)
                node = ((ResultSet)node).getInput();
            if (node instanceof Project)
                node = ((Project)node).getInput();
            if ((node instanceof Select) &&
                ((Select)node).getConditions().isEmpty())
                node = ((BasePlanWithInput)node).getInput();
            if (node instanceof NullSource)
                return SubqueryEmptiness.EMPTY;
            if (node instanceof ExpressionsSource) {
                int nrows = ((ExpressionsSource)node).getExpressions().size();
                if (nrows == 0)
                    return SubqueryEmptiness.EMPTY;
                else
                    return SubqueryEmptiness.NON_EMPTY;
            }
            else
                return SubqueryEmptiness.UNKNOWN;
        }

        // If the inside of this subquery returns a single column (in
        // an obvious to work out way), get it.
        protected ExpressionNode getSubqueryColumn(Subquery subquery) {
            PlanNode node = subquery.getQuery();
            if (node instanceof ResultSet)
                node = ((ResultSet)node).getInput();
            if (node instanceof Project) {
                List<ExpressionNode> cols = ((Project)node).getFields();
                if (cols.size() == 1)
                    return cols.get(0);
            }
            return null;
        }

        public void finishAggregates() {
            if (changedAggregates == null) return;
            state = State.AGGREGATES;
            planContext.accept(this);
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

        protected ExpressionNode evalNow(ExpressionNode node) {
            try {
                return expressionAssembler.evalNow(planContext, node);
            }
            catch (Exception ex) {
                logger.debug("Error evaluating as constant", ex);
            }
            return node;
        }

        protected boolean checkConstantBoolean(ExpressionNode node) {
            Boolean actual = getBooleanObject((ConstantExpression)node);
            return Boolean.TRUE.equals(actual);
        }
        
        protected ConditionExpression newBooleanConstant(Boolean value, ExpressionNode source) {
            return (ConditionExpression)
                newExpression(new BooleanConstantExpression(value));
        }

        protected ExpressionNode newExpression(ExpressionNode expr) {
            if (resolvingVisitor != null)
                return resolvingVisitor.visit(expr);
            else
                return expr;
        }

        protected ExpressionNode newConstant(Object value, ExpressionNode source) {
            return (value == null)
                    ? newExpression(ConstantExpression.typedNull(
                        source.getSQLtype(),
                        source.getSQLsource(),
                        source.getType()))
                    : newExpression(new ConstantExpression(value, 
                        source.getSQLtype(),
                        source.getSQLsource(),
                        source.getType()));
        }

        protected ExpressionNode genericFunctionExpression(FunctionExpression fun) {
            TPreptimeValue preptimeValue = fun.getPreptimeValue();
            return (preptimeValue.value() == null)
                    ? fun
                    : new ConstantExpression(preptimeValue);
        }

        protected Boolean getBooleanObject(ConstantExpression expression) {
            ValueSource value = expression.getPreptimeValue().value();
            return value == null || value.isNull()
                    ? null
                    : value.getBoolean();
        }

        protected Constantness isConstant(ExpressionNode expr) {
            TPreptimeValue tpv = expr.getPreptimeValue();
            if (tpv == null) {
                return Constantness.NULL;
            }
            ValueSource value = tpv.value();
            if (tpv.type() == null) {
                assert value == null || value.isNull() : value;
                assert !(expr instanceof ParameterExpression) : value;
                return Constantness.NULL;
            }
            if (value == null)
                return Constantness.VARIABLE;
            return value.isNull()
                    ? Constantness.NULL
                    : Constantness.CONSTANT;
        }

    }

    // Recognize and improve IN conditions with a list (or VALUES).
    protected static class InCondition implements Comparator<List<ExpressionNode>> {
        private AnyCondition any;
        private ExpressionsSource expressions;
        private List<ComparisonCondition> comparisons;
        private Project project;
        private final TypesRegistryService typesRegistry;
        private final TypesTranslator typesTranslator;
        private final QueryContext qc;
        
        private InCondition(AnyCondition any,
                            ExpressionsSource expressions,
                            List<ComparisonCondition> comparisons,
                            Project project,
                            PlanContext planContext)
        {
            this.any = any;
            this.expressions = expressions;
            this.comparisons = comparisons;
            this.project = project;
            SchemaRulesContext rulesContext = (SchemaRulesContext)planContext.getRulesContext();
            typesRegistry = rulesContext.getTypesRegistry();
            typesTranslator = rulesContext.getTypesTranslator();
            qc = planContext.getQueryContext();
        }

        public boolean isEmpty() {
            return expressions.getExpressions().isEmpty();
        }

        public boolean isSingleton() {
            return (expressions.getExpressions().size() == 1);
        }
        
        public List<ExpressionNode> getSingleton() {
            return expressions.getExpressions().get(0);
        }

        public List<ComparisonCondition> getConditions() {
            return comparisons;
        }

        // Recognize the form of IN we support improving.
        public static InCondition of(AnyCondition any, PlanContext planContext) {
            Subquery subquery = any.getSubquery();
            PlanNode input = subquery.getInput();
            if (!(input instanceof Project))
                return null;
            Project project = (Project)input;
            input = project.getInput();
            if (!(input instanceof ExpressionsSource))
                return null;
            ExpressionsSource expressions = (ExpressionsSource)input;
            if (project.getFields().size() != 1)
                return null;
            ExpressionNode cond = project.getFields().get(0);
            if (!(cond instanceof ConditionExpression))
                return null;
            List<ComparisonCondition> comps = new ArrayList<>();
            if (!getAnyConditions(comps, (ConditionExpression)cond, expressions))
                return null;
            List<List<ExpressionNode>> rows = expressions.getExpressions();
            if (!(rows.isEmpty() ||
                  (rows.get(0).size() == comps.size())))
                return null;
            return new InCondition(any, expressions, comps, project, planContext);
        }

        private static boolean getAnyConditions(List<ComparisonCondition> comps,
                                                ConditionExpression cond,
                                                ExpressionsSource expressions) {
            if (cond instanceof ComparisonCondition) {
                ComparisonCondition comp = (ComparisonCondition)cond;
                if (!(comp.getRight().isColumn() &&
                      (comp.getOperation() == Comparison.EQ) &&
                      (((ColumnExpression)comp.getRight()).getTable() == expressions) &&
                      (((ColumnExpression)comp.getRight()).getPosition() == comps.size())))
                    return false;
                comps.add((ComparisonCondition)cond);
                return true;
            }
            else if (cond instanceof LogicalFunctionCondition) {
                LogicalFunctionCondition lcond = (LogicalFunctionCondition)cond;
                return (getAnyConditions(comps, lcond.getLeft(), expressions) &&
                        getAnyConditions(comps, lcond.getRight(), expressions));
            }
            else {
                return false;
            }
        }

        public void dedup(boolean topLevel, boolean setDistinct) {
            if (expressions.getDistinctState() != null)
                return;

            List<List<ExpressionNode>> rows = expressions.getExpressions();
            List<List<ExpressionNode>> constants = new ArrayList<>();
            List<List<ExpressionNode>> parameters = null;
            List<List<ExpressionNode>> others = null;
            boolean anyNull = false;
            for (List<ExpressionNode> row : rows) {
                boolean allConstant = true, allConstOrParam = true, hasNull = false;
                for (ExpressionNode col : row) {
                    if (col instanceof ConstantExpression) {
                        ConstantExpression constant = (ConstantExpression)col;
                        if (constant.getValue() == null) {
                            anyNull = hasNull = true;
                        }
                    }
                    else {
                        allConstant = false;
                        if (!isParam(col)) {
                            allConstOrParam = false;
                        }
                    }
                }
                if (hasNull && topLevel) {
                    // A top-level condition does not need to worry about the
                    // difference between false and unknown and so can discard
                    // NULL elements.
                    continue;
                }
                if (allConstant) {
                    constants.add(row);
                }
                else if (allConstOrParam) {
                    if (parameters == null)
                        parameters = new ArrayList<>();
                    parameters.add(row);
                }
                else {
                    if (others == null)
                        others = new ArrayList<>();
                    others.add(row);
                }
            }
            if (constants.size() > 1)
                Collections.sort(constants, this);
            rows.clear();
            List<ExpressionNode> lastRow = null;
            for (List<ExpressionNode> row : constants) {
                if ((lastRow == null) ||
                    (compare(lastRow, row) != 0)) {
                    rows.add(row);
                    lastRow = row;
                }
            }
            if (parameters != null)
                rows.addAll(parameters);
            if (others != null)
                rows.addAll(others);

            if (setDistinct) {
                DistinctState distinct;
                if (others != null)
                    distinct = DistinctState.HAS_EXPRESSSIONS;
                else if (parameters != null)
                    distinct = DistinctState.HAS_PARAMETERS;
                else if (anyNull)
                    distinct = DistinctState.DISTINCT_WITH_NULL;
                else
                    distinct = DistinctState.DISTINCT;
                expressions.setDistinctState(distinct);
            }
        }

        private static boolean isParam(ExpressionNode expr) {
            // CAST(? AS type) is okay, too.
            // (Actually, any function with only one param as arguments would be.)
            while (expr instanceof CastExpression)
                expr = ((CastExpression)expr).getOperand();
            return (expr instanceof ParameterExpression);
        }

        public enum CompareConstants { NORMAL, COMPARE_NULL, ROW_EQUALS };

        /**
         * 
         * For types3 ONLY!
         * 
         * Compare ValueSources of two constant expression node
         * @param leftNode
         * @param rightNode
         * @param registry
         * @param qc
         * @return  true if the two ExpressionNodes' ValueSource are equal.
         *          false otherwise
         */
        public static boolean comparePrepValues(ExpressionNode leftNode,
                                         ExpressionNode rightNode,
                                         TypesRegistryService registry,
                                         TypesTranslator typesTranslator,
                                         QueryContext qc)
        {
            // if either is not constant, preptime values aren't available
            if (!leftNode.isConstant() || !rightNode.isConstant())
                return false;
            
            ValueSource leftSource = leftNode.getPreptimeValue().value();
            ValueSource rightSource = rightNode.getPreptimeValue().value();

            TInstance lTIns = leftSource.getType();
            TInstance rTIns = rightSource.getType();
            
            if (TClass.comparisonNeedsCasting(lTIns, rTIns))
            {
                boolean nullable = leftSource.isNull() || rightSource.isNull();
                TCastResolver casts = registry.getCastsResolver();
                TInstance common = TypeResolver.commonInstance(casts, lTIns, rTIns);
                if (common == null)
                    common = typesTranslator.typeForString();
                
                Value leftCasted = new Value(common);
                Value rightCasted = new Value(common);

                TExecutionContext execContext = new TExecutionContext(Arrays.asList(lTIns, rTIns), common, qc);
                casts.cast(lTIns, common).evaluate(execContext, leftSource, leftCasted);
                casts.cast(rTIns, common).evaluate(execContext, rightSource, rightCasted);
                
                return TClass.compare(leftCasted.getType(), leftCasted,
                                      rightCasted.getType(),rightCasted)
                       == 0;
            }
            else
                return TClass.compare(lTIns, leftSource, rTIns, rightSource) == 0;
        }

        // If some of the values in the LHS row and RHS rows are constants, 
        // can compare them now, and either eliminate a LHS value that always matches
        // or a row that never matches or find a row that always matches, which is
        // the answer.
        public CompareConstants compareConstants(Folder folder) {
            List<List<ExpressionNode>> rows = expressions.getExpressions();
            BitSet matching = new BitSet(rows.size());
            matching.set(0, rows.size());
            boolean removedRow = false, removedComparison = false;
            int i = 0;
            while (i < comparisons.size()) {
                ComparisonCondition cond = comparisons.get(i);
                ExpressionNode left = cond.getLeft();
                switch (folder.isConstant(left)) {
                case NULL:
                    return CompareConstants.COMPARE_NULL;
                case CONSTANT:
                    {
                        boolean verticalMatch = true;
                        for (int j = 0; j < rows.size(); j++) {
                            List<ExpressionNode> row = rows.get(j);
                            if (row == null) continue;
                            ExpressionNode right = row.get(i);
                            if (folder.isConstant(right) == Folder.Constantness.CONSTANT) {
                                if (!comparePrepValues(left, right, typesRegistry, typesTranslator, qc)) {
                                    // Definitely not equal, can remove row.
                                    rows.set(j, null);
                                    removedRow = true;
                                    matching.clear(j);
                                }

                                continue; // Definitely equal.
                            }
                            // Neither this row nor this column known compare equal.
                            verticalMatch = false;
                            matching.clear(j);
                        }
                        if (verticalMatch) {
                            // Column was all constants: every row
                            // matched or was eliminated; don't need this comparison
                            // any more,
                            // This cannot introduce any duplicates, because all the
                            // rows that remain have the same value in the column
                            // being removed and thus would have already been
                            // duplicates.
                            comparisons.remove(i);
                            for (int j = 0; j < rows.size(); j++) {
                                List<ExpressionNode> row = rows.get(j);
                                if (row == null) continue;
                                row.remove(i);
                            }
                            removedComparison = true;
                            continue;
                        }
                    }
                    break;
                default:
                    matching.clear();
                }
                i++;
            }
            if (!matching.isEmpty())
                return CompareConstants.ROW_EQUALS;
            if (removedRow) {
                int j = 0;
                while (j < rows.size()) {
                    if (rows.get(j) == null)
                        rows.remove(j);
                    else
                        j++;
                }
            }
            if (removedComparison)
                project.getFields().set(0, buildCondition(null, folder));
            return CompareConstants.NORMAL;
        }

        public ConditionExpression buildCondition(List<ExpressionNode> values,
                                                  Folder folder) {
            ConditionExpression result = null;
            for (int i = 0; i < comparisons.size(); i++) {
                ComparisonCondition comp = comparisons.get(i);
                if (values != null)
                    comp.setRight(values.get(i));
                if (result == null)
                    result = comp;
                else {
                    List<ConditionExpression> operands = new ArrayList<>(2);
                        
                    operands.add(result);
                    operands.add(comp);
                    result = (ConditionExpression)
                        folder.newExpression(new LogicalFunctionCondition("and", operands,
                                                                          comp.getSQLtype(), null, comp.getType()));
                }
            }
            if (result == null)
                result = folder.newBooleanConstant(Boolean.TRUE, any);
            return result;
        }

        @Override
        @SuppressWarnings("unchecked")
        public int compare(List<ExpressionNode> r1, List<ExpressionNode> r2) {
            for (int i = 0; i < r1.size(); i++) {
                Comparable o1 = asComparable(r1.get(i));
                Comparable o2 = asComparable(r2.get(i));
                if (o1 == null) {
                    if (o2 != null) {
                        return +1;
                    }
                }
                else if (o2 == null) {
                    return -1;
                }
                else {
                    int c = o1.compareTo(o2);
                    if (c != 0) return c;
                }
            }
            return 0;
        }

        private Comparable asComparable(ExpressionNode elem) {
            // TODO Needed because a TKeyComparison may cause the expressions to not be cast to the same type.
            // This will only work as long as all TKeyComparisons are between integer types.
            Object value = ((ConstantExpression)elem).getValue();
            assert value == null || value instanceof Comparable : "value not a comparable: " + value.getClass();
            Comparable result = (Comparable)value;
            if (result instanceof Byte || result instanceof Short || result instanceof Integer || result instanceof Long)
                result = ((Number)result).longValue();
            return result;
        }

    }

}
