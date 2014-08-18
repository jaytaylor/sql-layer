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

import com.foundationdb.sql.optimizer.plan.*;

import com.foundationdb.sql.optimizer.plan.JoinNode.JoinType;
import com.foundationdb.sql.optimizer.plan.ExpressionsSource.DistinctState;

import com.foundationdb.server.error.AkibanInternalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Turn a top-level IN condition into a semi-join and allow it to be reversed. */
public class InConditionReverser extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(InConditionReverser.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext planContext) {
        List<TopLevelSubqueryCondition> conds = new ConditionFinder(planContext).find();
        Collections.reverse(conds); // Transform depth first.
        for (TopLevelSubqueryCondition cond : conds) {
            if (cond.subqueryCondition instanceof AnyCondition)
                convert(cond.select, cond.selectElement, 
                        (AnyCondition)cond.subqueryCondition, cond.isNegated());
            else if (cond.subqueryCondition instanceof ExistsCondition)
                convert(cond.select, cond.selectElement,
                        (ExistsCondition)cond.subqueryCondition, cond.isNegated());
        }
    }

    /** Convert an IN / ANY to a EXISTS-like semi-join.
     * The ANY condition becomes the join condition.
     * If possible, the RHS is a {@link ColumnSource} and the join is
     * reversible. This requires knowing that it is distinct or how to make it so.
     * @see IndexPicker#pickIndexesTableValues.
     */
    public void convert(Select select, ConditionExpression selectElement,
                        AnyCondition any, boolean negated) {
        PlanNode sinput = select.getInput();
        if (!(sinput instanceof Joinable))
            return;
        Joinable selectInput = (Joinable)sinput;
        Subquery subquery = any.getSubquery();
        PlanNode input = subquery.getInput();
        // TODO: DISTINCT does not matter inside an ANY. So
        // effectively this is a hint, enabling reversal in the
        // absence of CBO.
        boolean hasDistinct = false;
        if (input instanceof Distinct) {
            input = ((Distinct)input).getInput();
            hasDistinct = true;
        }
        if (!(input instanceof Project))
            return;
        Project project = (Project)input;
        input = project.getInput();
        List<ExpressionNode> projectFields = project.getFields();
        if (projectFields.size() != 1)
            return;
        ConditionList joinConditions = new ConditionList();
        addAnyConditions(joinConditions, (ConditionExpression)projectFields.get(0));
        if (!negated) {
            if (input instanceof ExpressionsSource) {
                ExpressionsSource expressionsSource = (ExpressionsSource)input;
                // If the source was VALUES, see if it's distinct. If so,
                // we can possibly reverse the join and benefit from an
                // index.
                JoinType semiType = JoinType.SEMI;
                DistinctState distinct = expressionsSource.getDistinctState();
                switch (distinct) {
                case DISTINCT:
                case DISTINCT_WITH_NULL:
                    semiType = JoinType.SEMI_INNER_ALREADY_DISTINCT;
                    break;
                case HAS_PARAMETERS:
                    semiType = JoinType.SEMI_INNER_IF_DISTINCT;
                    break;
                }
                convertToSemiJoin(select, selectElement, selectInput, expressionsSource,
                                  joinConditions, semiType);
                return;
            }
            if (convertToSubquerySource(select, selectElement, any, selectInput, input,
                                        project, projectFields, 
                                        joinConditions, hasDistinct))
                return;
        }
        ConditionList insideJoinConditions = null;
        if (input instanceof Select) {
            Select inselect = (Select)input;
            insideJoinConditions = inselect.getConditions();
            input = inselect.getInput();
        }
        if (input instanceof Joinable) {
            addJoinConditions:
            if (insideJoinConditions != null) {
                // Include inside WHERE in outer join.
                joinConditions.addAll(insideJoinConditions);
            }
            convertToSemiJoin(select, selectElement, selectInput, 
                              (Joinable)input, joinConditions, 
                              (negated) ? JoinType.ANTI : JoinType.SEMI);
            return;
        }
    }

    private void addAnyConditions(ConditionList joinConditions,
                                  ConditionExpression condition) {
        if (condition instanceof LogicalFunctionCondition) {
            LogicalFunctionCondition lcond = (LogicalFunctionCondition)condition;
            if ("and".equals(lcond.getFunction())) {
                addAnyConditions(joinConditions, lcond.getLeft());
                addAnyConditions(joinConditions, lcond.getRight());
                return;
            }
        }
        joinConditions.add(condition);
    }

    protected boolean convertToSubquerySource(Select select, 
                                              ConditionExpression selectElement, AnyCondition any, 
                                              Joinable selectInput, PlanNode input,
                                              Project project, List<ExpressionNode> projectFields,
                                              ConditionList joinConditions, 
                                              boolean hasDistinct) {
        SubqueryBoundTables sbt = new SubqueryBoundTables(input);
        for (ConditionExpression cond : joinConditions) {
            if (!(cond instanceof ComparisonCondition))
                return false;
            ComparisonCondition ccond = (ComparisonCondition)cond;
            if (!(sbt.freeOfTables(ccond.getLeft()) && 
                  sbt.onlyHasTables(ccond.getRight())))
                return false;
        }
        if (!sbt.onlyHasTables(input))
            return false;       // Also must be uncorrelated (beyond interim Project).
        // Clean split in table references.  Join with derived table
        // whose columns are the RHS of the ANY comparisons, which
        // then references that table instead. That way the table
        // works if put on the outer side of a nested loop join as
        // well. If it stays on the inside, the subquery will be
        // elided later.
        PlanNode subqueryInput = project;
        if (input instanceof Limit) {
            // Put subquery source with limit into more standard form.
            Limit limit = (Limit)input;
            input = limit.getInput();
            project.replaceInput(limit, input);
            limit.replaceInput(input, project);
            subqueryInput = limit;
        }
        EquivalenceFinder<ColumnExpression> emptyEquivs = any.getSubquery().getColumnEquivalencies();
        SubquerySource subquerySource = new SubquerySource(new Subquery(subqueryInput, emptyEquivs), "ANY");
        projectFields.clear();
        for (ConditionExpression cond : joinConditions) {
            ComparisonCondition ccond = (ComparisonCondition)cond;
            ExpressionNode cright = ccond.getRight();
            projectFields.add(cright);
            ccond.setRight(new ColumnExpression(subquerySource,
                                                projectFields.size() - 1,
                                                cright.getSQLtype(),
                                                cright.getSQLsource(),
                                                cright.getType()));
        }
        convertToSemiJoin(select, selectElement, selectInput, subquerySource,
                          joinConditions, 
                          hasDistinct ? JoinType.SEMI_INNER_IF_DISTINCT : JoinType.SEMI);
        return true;
    }

    protected void convertToSemiJoin(Select select, ConditionExpression selectElement,
                                     Joinable selectInput, Joinable semiInput,
                                     ConditionList joinConditions, 
                                     JoinType semiType) {
        JoinNode join = new JoinNode(selectInput, semiInput, semiType);
        select.getConditions().remove(selectElement);
        join.setJoinConditions(joinConditions);
        select.replaceInput(selectInput, join);
    }

    public static void beforeReverseSemiJoin(JoinNode join) {
        if (join.getJoinType() == JoinType.SEMI_INNER_IF_DISTINCT) {
            Joinable right = join.getRight();
            if (right instanceof ExpressionsSource) {
                ExpressionsSource values = (ExpressionsSource)right;
                values.setDistinctState(DistinctState.NEED_DISTINCT);
            }
            else if (right instanceof SubquerySource) {
                Subquery subquery = ((SubquerySource)right).getSubquery();
                PlanNode input = subquery.getInput();
                subquery.replaceInput(input, new Distinct(input));
            }
            else {
                throw new AkibanInternalException("Could not make distinct " + 
                                                  right);
            }
        }
        else {
            assert (join.getJoinType() == JoinType.SEMI_INNER_ALREADY_DISTINCT);
        }
        join.setJoinType(JoinType.INNER);
    }

    public static void didNotReverseSemiJoin(JoinNode join) {
        assert join.getJoinType().isSemi() : join.getJoinType();
        cleanUpSemiJoin(join, join.getRight());
        join.setJoinType(JoinType.SEMI);
    }

    public static void cleanUpSemiJoin(JoinNode join, Joinable right) {
        if (right instanceof SubquerySource) {
            // Undo part of what we did above. Specifically,
            // splice out the SubquerySource, Subquery, Project
            // and move any Select up into the join conditions.
            // Not semantically necessary, but putting more of the
            // conditions together helps with the changes of being
            // able to use a group index.
            PlanNode input = ((SubquerySource)right).getSubquery().getInput();
            if (!(input instanceof Project))
                return;
            Project project = (Project)input;
            input = project.getInput();
            Select select = null;
            if (input instanceof Select) {
                select = (Select)input;
                input = select.getInput();
            }
            if (!(input instanceof Joinable))
                return;
            if (join.hasJoinConditions()) {
                ConditionList conds = join.getJoinConditions();
                for (int i = 0; i < conds.size(); i++) {
                    ConditionExpression cond = conds.get(i);
                    if (!(cond instanceof ComparisonCondition))
                        return;
                    ComparisonCondition ccond = (ComparisonCondition)cond;
                    ccond.setRight(project.getFields().get(
                            ((ColumnExpression)ccond.getRight()).getPosition()));
                }
                if (select != null)
                    conds.addAll(select.getConditions());
            }
            else if (select != null) {
                join.setJoinConditions(select.getConditions());
            }
            join.replaceInput(right, input);
        }
    }

    public void convert(Select select, ConditionExpression selectElement,
                        ExistsCondition exists, boolean negated) {
        Subquery subquery = exists.getSubquery();
        PlanNode qinput = subquery.getInput();
        PlanNode sinput = select.getInput();
        PlanNode input = qinput;
        ConditionList conditions = null;
        if (input instanceof Select) {
            Select sinner = (Select)input;
            conditions = sinner.getConditions();
            input = sinner.getInput();
        }
        if (!((sinput instanceof Joinable) && (input instanceof Joinable)))
            return;
        JoinNode join;
        if (subquery.getOuterTables().isEmpty()) {
            // Uncorrelated subquery; can be done independently.
            subquery.replaceInput(qinput,
                                  negated ?
                                  new OnlyIfEmpty(qinput) :
                                  new Limit(qinput, 1));
            SubquerySource subquerySource = 
                new SubquerySource(subquery, negated ? "NOT EXISTS" : "EXISTS");
            join = new JoinNode((Joinable)sinput, subquerySource, JoinType.INNER);
        }
        else {
            join = new JoinNode((Joinable)sinput, (Joinable)input,
                                (negated) ? JoinType.ANTI : JoinType.SEMI);
            if (conditions != null)
                join.setJoinConditions(conditions);
        }
        select.getConditions().remove(selectElement);
        select.replaceInput(sinput, join);
    }
    
    static class TopLevelSubqueryCondition {
        Select select;
        ConditionExpression selectElement;
        SubqueryExpression subqueryCondition;

        public TopLevelSubqueryCondition(Select select, 
                                         ConditionExpression selectElement,
                                         SubqueryExpression subqueryCondition) {
            this.select = select;
            this.selectElement = selectElement;
            this.subqueryCondition = subqueryCondition;
        }

        public boolean isNegated() {
            return (selectElement != subqueryCondition);
        }
    }

    static class ConditionFinder extends SubqueryBoundTablesTracker {
        List<TopLevelSubqueryCondition> result = 
            new ArrayList<>();

        public ConditionFinder(PlanContext planContext) {
            super(planContext);
        }

        public List<TopLevelSubqueryCondition> find() {
            run();
            return result;
        }

        @Override
        public boolean visit(PlanNode n) {
            super.visit(n);
            if (n instanceof Select) {
                Select select = (Select)n;
                for (ConditionExpression cond : select.getConditions()) {
                    if (cond instanceof SubqueryExpression) {
                        result.add(new TopLevelSubqueryCondition(select, cond, (SubqueryExpression)cond));
                    }
                    else if (cond instanceof LogicalFunctionCondition) {
                        LogicalFunctionCondition lcond = (LogicalFunctionCondition)cond;
                        if (lcond.getFunction().equals("not") &&
                            (lcond.getOperands().get(0) instanceof SubqueryExpression)) {
                            result.add(new TopLevelSubqueryCondition(select, cond, (SubqueryExpression)lcond.getOperands().get(0)));
                            }
                    }
                }
            }
            return true;
        }
    }

    static class SubqueryBoundTables  implements PlanVisitor, ExpressionVisitor {
        private Set<ColumnSource> insideTables = new HashSet<>();

        private static enum State { TABLES, FREE, ONLY };
        private State state;
        private boolean found;

        public SubqueryBoundTables(PlanNode n) {
            state = State.TABLES;
            found = false;
            n.accept(this);
        }

        public boolean freeOfTables(ExpressionNode n) {
            state = State.FREE;
            found = false;
            n.accept(this);
            return !found;
        }

        public boolean onlyHasTables(ExpressionNode n) {
            state = State.ONLY;
            found = false;
            n.accept(this);
            return !found;
        }

        public boolean onlyHasTables(PlanNode n) {
            state = State.ONLY;
            found = false;
            n.accept(this);
            return !found;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            return !found;
        }

        @Override
        public boolean visit(PlanNode n) {
            if ((state == State.TABLES) &&
                (n instanceof ColumnSource))
                insideTables.add((ColumnSource)n);
            return !found;
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(ExpressionNode n) {
            return !found;
        }

        @Override
        public boolean visit(ExpressionNode n) {
            if (n instanceof ColumnExpression) {
                ColumnSource table = ((ColumnExpression)n).getTable();
                switch (state) {
                case FREE:
                    if (insideTables.contains(table))
                        found = true;
                    break;
                case ONLY:
                    if (!insideTables.contains(table))
                        found = true;
                    break;
                }
            }
            return !found;
        }
    }

}
