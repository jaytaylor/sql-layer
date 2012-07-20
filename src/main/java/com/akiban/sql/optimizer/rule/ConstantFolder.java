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

import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.ExpressionsSource.DistinctState;

import com.akiban.server.expression.std.Comparison;

import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;

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
    
    private final boolean usePValues;
    
    public ConstantFolder() {
        this(Types3Switch.ON);
    }

    public ConstantFolder(boolean usePValues) {
        this.usePValues = usePValues;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext planContext) {
        Folder folder = usePValues ? new NewFolder(planContext) : new OldFolder(planContext);
        while (folder.foldConstants());
        folder.finishAggregates();
    }

    private static abstract class Folder implements PlanVisitor, ExpressionRewriteVisitor {
        private final PlanContext planContext;
        protected final ExpressionAssembler<?> expressionAssembler;
        private Set<ColumnSource> eliminatedSources = new HashSet<ColumnSource>();
        private Set<AggregateSource> changedAggregates = null;
        private enum State { FOLDING, AGGREGATES };
        private State state;
        private boolean changed;
        private Map<ConditionExpression,Boolean> topLevelConditions = 
            new IdentityHashMap<ConditionExpression,Boolean>();

        protected Folder(PlanContext planContext, ExpressionAssembler<?> expressionAssembler) {
            this.planContext = planContext;
            this.expressionAssembler = expressionAssembler;
        }
        
        public ExpressionNode foldConstants(ExpressionNode fromNode) {
            do {
                state = State.FOLDING;
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
            if (state == State.FOLDING) {
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
            if (isIdempotentEquality(cond)) {
                if ((cond.getLeft().getSQLtype() != null) &&
                    !cond.getLeft().getSQLtype().isNullable()) {
                    return new BooleanConstantExpression(Boolean.TRUE, 
                                                         cond.getSQLtype(), 
                                                         cond.getSQLsource());
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
                    boolean lv = getBoolean((ConstantExpression) left);
                    if (lv)
                        return right; // TRUE AND X -> X
                    else
                        return left; // FALSE AND X -> FALSE
                }
                if (isConstant(right) == Constantness.CONSTANT) {
                    boolean rv = getBoolean((ConstantExpression) right);
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
                    boolean lv = getBoolean((ConstantExpression) left);
                    if (lv)
                        return left; // TRUE OR X -> TRUE
                    else
                        return right; // FALSE OR X -> X
                }
                if (isConstant(right) == Constantness.CONSTANT) {
                    boolean rv = (Boolean)((ConstantExpression)right).getValue();
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
                    return new BooleanConstantExpression(null, 
                                                         lfun.getSQLtype(), 
                                                         lfun.getSQLsource());
                if (c == Constantness.CONSTANT)
                    return new BooleanConstantExpression(!getBoolean((ConstantExpression)cond),
                                                         lfun.getSQLtype(), 
                                                         lfun.getSQLsource());
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
                return new BooleanConstantExpression(Boolean.FALSE, 
                                                     fun.getSQLtype(), 
                                                     fun.getSQLsource());
            return fun;
        }

        protected ExpressionNode isTrueExpression(FunctionExpression fun) {
            ExpressionNode operand = fun.getOperands().get(0);
            if (isConstant(operand) != Constantness.VARIABLE)
                return evalNow(fun);
            else if (isFalseOrUnknown((ConditionExpression)operand))
                return new BooleanConstantExpression(Boolean.FALSE, 
                                                     fun.getSQLtype(), 
                                                     fun.getSQLsource());
            else
                return fun;
        }

        protected ExpressionNode isFalseExpression(FunctionExpression fun) {
            ExpressionNode operand = fun.getOperands().get(0);
            if (isConstant(operand) != Constantness.VARIABLE)
                return evalNow(fun);
            else if (isTrueOrUnknown((ConditionExpression)operand))
                return new BooleanConstantExpression(Boolean.FALSE, 
                                                     fun.getSQLtype(), 
                                                     fun.getSQLsource());
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
                return new BooleanConstantExpression(null, 
                                                     fun.getSQLtype(), 
                                                     fun.getSQLsource());
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
              if (getBoolean((ConstantExpression)operands.get(0)))
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
                                                          col.getAkType(),
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
                                                  col.getSQLtype(), AkType.NULL, col.getSQLsource());
            }
            return col;
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
                    if (getBoolean((ConstantExpression)condition))
                        conditions.remove(i);
                    else
                        return false;
                }
                else if (isFalseOrUnknown(condition))
                    return false;
                i++;
            }
            return true;
        }

        /** Returns <code>true</code> if the given expression will always evaluate
         * to either <i>true</i> or <i>unknown</i>.
         */
        protected boolean isTrueOrUnknown(ConditionExpression expr) {
            if (expr instanceof ConstantExpression) {
                Boolean value = getBoolean((ConstantExpression)expr);
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
                Boolean value = getBoolean((ConstantExpression)expr);
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
                return new ConstantExpression(null,
                                              expr.getSQLtype(), AkType.NULL, expr.getSQLsource());
            }
            ExpressionNode inner = getSubqueryColumn(expr.getSubquery());
            if (inner != null) {
                Constantness ic = isConstant(inner);
                if (ic == Constantness.NULL) {
                    // If it's empty, it's NULL. 
                    // If it selects something, that projects NULL.
                    // NULL either way.
                    return new ConstantExpression(NullValueSource.only(),
                                                  expr.getSQLtype(), 
                                                  expr.getSQLsource());
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
                return new BooleanConstantExpression(Boolean.FALSE,
                                                     cond.getSQLtype(), cond.getSQLsource());
            }
            return cond;
        }
    
        protected ExpressionNode anyCondition(AnyCondition cond) {
            SubqueryEmptiness empty = isEmptySubquery(cond.getSubquery());
            if (empty == SubqueryEmptiness.EMPTY) {
                // Empty ANY is false.
                return new BooleanConstantExpression(Boolean.FALSE,
                                                     cond.getSQLtype(), cond.getSQLsource());
            }
            ExpressionNode inner = getSubqueryColumn(cond.getSubquery());
            if ((inner != null) &&
                (isConstant(inner) == Constantness.CONSTANT) &&
                ((empty == SubqueryEmptiness.NON_EMPTY) ||
                 (!getBoolean((ConstantExpression)inner)))) {
                // Constant false: if it's empty, it's false. If it
                // selects something, that projects false. False
                // either way.
                // Constant true: if it's known non-empty, that's what
                // is returned.
                return inner;
            }
            InCondition in = InCondition.of(cond);
            if (in != null) {
                in.dedup(isTopLevelCondition(cond));
                if (in.isEmpty())
                    return new BooleanConstantExpression(Boolean.FALSE,
                                                         cond.getSQLtype(), 
                                                         cond.getSQLsource());
                else if (in.isSingleton()) {
                    ComparisonCondition comp = in.getCondition();
                    comp.setRight(in.getSingleton());
                    return comp;
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
            }
            return node;
        }
        
        protected abstract ExpressionNode genericFunctionExpression(FunctionExpression fun);
        protected abstract Boolean getBoolean(ConstantExpression expression);
        protected abstract Constantness isConstant(ExpressionNode expr);
    }

    private static final class OldFolder extends Folder {
        private OldExpressionAssembler oldExpressionAssembler;
        
        public OldFolder(PlanContext planContext) {
            this(planContext, new OldExpressionAssembler(planContext.getRulesContext()));
        }
        
        private OldFolder(PlanContext planContext, OldExpressionAssembler expressionAssembler) {
            super(planContext, expressionAssembler);
            this.oldExpressionAssembler = expressionAssembler;
        }

        @Override
        protected ExpressionNode genericFunctionExpression(FunctionExpression fun) {
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
            if (allConstant && isIdempotent(fun))
                return evalNow(fun);

            if (anyNull)
                switch(oldExpressionAssembler.getFunctionRegistry().composer(fun.getFunction()).getNullTreating())
                {
                case REMOVE:       return removeNull(fun);
                case RETURN_NULL: return new BooleanConstantExpression(null,
                        fun.getSQLtype(),
                        fun.getSQLsource());
                }

            return fun;
        }

        @Override
        protected Boolean getBoolean(ConstantExpression expression) {
            assert expression != null; // this way, a NPE in the next line unambiguously means getValue() returned null
            return (Boolean) expression.getValue();
        }
        
        @Override
        protected Constantness isConstant(ExpressionNode expr) {
            if (expr.isConstant())
                return ((((ConstantExpression)expr).getValue() == null) ?
                        Constantness.NULL :
                        Constantness.CONSTANT);
            else
                return Constantness.VARIABLE;
        }

        private ExpressionNode removeNull(FunctionExpression fun)
        {
            List<ExpressionNode> operands = fun.getOperands();

            if (operands.isEmpty())
                return new BooleanConstantExpression(null,
                        fun.getSQLtype(),
                        fun.getSQLsource());
            int i = 0;
            while (i < operands.size())
            {
                ExpressionNode operand = operands.get(i);
                Constantness c = isConstant(operand);
                if (c == Constantness.NULL)
                {
                    operands.remove(i);
                    continue;
                }
                i++;
            }
            return fun;
        }
    }

    public static final class NewFolder extends Folder {
        public NewFolder(PlanContext planContext) {
            super(planContext, new NewExpressionAssembler(planContext.getRulesContext()));
        }

        @Override
        protected ExpressionNode genericFunctionExpression(FunctionExpression fun) {
            TPreptimeValue preptimeValue = fun.getPreptimeValue();
            return (preptimeValue.value() == null)
                    ? fun
                    : new ConstantExpression(preptimeValue);
        }

        @Override
        protected Boolean getBoolean(ConstantExpression expression) {
            PValueSource value = expression.getPreptimeValue().value();
            return value.isNull()
                    ? null
                    : value.getBoolean();
        }

        @Override
        protected Constantness isConstant(ExpressionNode expr) {
            PValueSource value = expr.getPreptimeValue().value();
            if (value == null)
                return Constantness.VARIABLE;
            return value.isNull()
                    ? Constantness.NULL
                    : Constantness.CONSTANT;
        }
    }

    // Recognize and improve IN conditions with a list (or VALUES).
    // This currently only recognizes the one operand LHS version, but
    // is easily extended to the row constructor form once the parser
    // supports that.
    protected static class InCondition implements Comparator<List<ExpressionNode>> {
        private ExpressionsSource expressions;
        private ComparisonCondition comparison;

        private InCondition(ExpressionsSource expressions,
                            ComparisonCondition comparison) {
            this.expressions = expressions;
            this.comparison = comparison;
        }

        public boolean isEmpty() {
            return expressions.getExpressions().isEmpty();
        }

        public boolean isSingleton() {
            return (expressions.getExpressions().size() == 1);
        }
        
        public ExpressionNode getSingleton() {
            return expressions.getExpressions().get(0).get(0);
        }

        public ComparisonCondition getCondition() {
            return comparison;
        }

        // Recognize the form of IN we support improving.
        public static InCondition of(AnyCondition any) {
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
            if (!(cond instanceof ComparisonCondition))
                return null;
            ComparisonCondition comp = (ComparisonCondition)cond;
            if (!(comp.getRight().isColumn() &&
                  (comp.getOperation() == Comparison.EQ) &&
                  (((ColumnExpression)comp.getRight()).getTable() == expressions)))
                return null;
            List<List<ExpressionNode>> rows = expressions.getExpressions();
            if (!(rows.isEmpty() ||
                  (rows.get(0).size() == 1)))
                return null;
            return new InCondition(expressions, comp);
        }

        public void dedup(boolean topLevel) {
            if (expressions.getDistinctState() != null)
                return;

            List<List<ExpressionNode>> rows = expressions.getExpressions();
            List<List<ExpressionNode>> constants = new ArrayList<List<ExpressionNode>>();
            List<ExpressionNode> constantNull = null;
            List<List<ExpressionNode>> parameters = null;
            List<List<ExpressionNode>> others = null;
            for (List<ExpressionNode> row : rows) {
                ExpressionNode col = row.get(0);
                if (col instanceof ConstantExpression) {
                    ConstantExpression constant = (ConstantExpression)col;
                    if (constant.getValue() == null)
                        constantNull = row;
                    else
                        constants.add(row);
                }
                else if (col instanceof ParameterExpression) {
                    if (parameters == null)
                        parameters = new ArrayList<List<ExpressionNode>>();
                    parameters.add(row);
                }
                else {
                    if (others == null)
                        others = new ArrayList<List<ExpressionNode>>();
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
            // A top-level condition does not need to worry about the
            // difference between false and unknown and so can discard
            // NULL elements.
            if (topLevel)
                constantNull = null;
            if (constantNull != null)
                rows.add(constantNull);
            if (parameters != null)
                rows.addAll(parameters);
            if (others != null)
                rows.addAll(others);

            DistinctState distinct;
            if (others != null)
                distinct = DistinctState.HAS_EXPRESSSIONS;
            else if (parameters != null)
                distinct = DistinctState.HAS_PARAMETERS;
            else if (constantNull != null)
                distinct = DistinctState.DISTINCT_WITH_NULL;
            else
                distinct = DistinctState.DISTINCT;
            expressions.setDistinctState(distinct);
        }

        public int compare(List<ExpressionNode> r1, List<ExpressionNode> r2) {
            Comparable o1 = (Comparable)((ConstantExpression)r1.get(0)).getValue();
            Comparable o2 = (Comparable)((ConstantExpression)r2.get(0)).getValue();
            return o1.compareTo(o2);
        }

    }

}
