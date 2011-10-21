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

import com.akiban.sql.optimizer.plan.JoinNode.JoinType;
import com.akiban.sql.optimizer.plan.JoinNode.JoinReverseHook;
import com.akiban.sql.optimizer.plan.ExpressionsSource.DistinctState;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.UnsupportedSQLException;

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
        List<TopLevelSubqueryCondition> conds = 
            new ConditionFinder().find(planContext.getPlan());
        for (TopLevelSubqueryCondition cond : conds) {
            if (cond.condition instanceof AnyCondition)
                convert(cond.select, (AnyCondition)cond.condition);
            else if (cond.condition instanceof ExistsCondition)
                convert(cond.select, (ExistsCondition)cond.condition);
        }
    }

    /** Convert an IN / ANY to a EXISTS-like semi-join.
     * The ANY condition becomes the join condition.
     * If possible, the RHS is a {@link ColumnSource} and the join is
     * reversible.
     * @see IndexPicker#pickIndexesTableValues.
     */
    public void convert(Select select, AnyCondition any) {
        PlanNode sinput = select.getInput();
        if (!(sinput instanceof Joinable))
            return;
        Subquery subquery = any.getSubquery();
        PlanNode input = subquery.getInput();
        if (!(input instanceof Project))
            return;
        Project project = (Project)input;
        input = project.getInput();
        // TODO: DISTINCT does not matter inside an ANY. So
        // effectively this is a hint, enabling reversal in the
        // absence of CBO. Could splice it out when kept on the
        // inside.
        boolean hasDistinct = false;
        if (input instanceof ResultSet)
            input = ((ResultSet)input).getInput();
        if (input instanceof Distinct) {
            input = ((Distinct)input).getInput();
            hasDistinct = true;
        }
        List<ExpressionNode> projectFields = project.getFields();
        ConditionExpression cond = (ConditionExpression)projectFields.get(0);
        make_column_source:
        if (!((input instanceof Joinable) && (input instanceof ColumnSource))) {
            SubqueryBoundTables sbt = new SubqueryBoundTables(input);
            // TODO: This will need extending to support row constructor IN.
            // Probably will be an AND or maybe something with a ConditionList.
            if (cond instanceof ComparisonCondition) {
                ComparisonCondition ccond = (ComparisonCondition)cond;
                ExpressionNode cleft = ccond.getLeft();
                ExpressionNode cright = ccond.getRight();
                if (sbt.freeOfTables(cleft) && sbt.onlyHasTables(cright)) {
                    // Clean split in table references.  Join with
                    // derived table whose columns are the RHS of the
                    // ANY comparisons, which then references that
                    // table instead. That way the table works if put
                    // on the outer side of a nested loop join as
                    // well.
                    projectFields.clear();
                    projectFields.add(cright);
                    SubquerySource ssource = new SubquerySource(new Subquery(project),
                                                                "ANY");
                    ccond.setRight(new ColumnExpression(ssource,
                                                        projectFields.size() - 1,
                                                        cright.getSQLtype(),
                                                        cright.getAkType(),
                                                        cright.getSQLsource()));
                    input = ssource;
                    break make_column_source;
                }
            }
            if (!(input instanceof Joinable))
                return;
        }
        JoinNode join = new JoinNode((Joinable)sinput, (Joinable)input, JoinType.SEMI);
        ConditionList conds = new ConditionList(1);
        conds.add(cond);
        join.setJoinConditions(conds);
        select.getConditions().remove(any);
        select.replaceInput(sinput, join);
        if (input instanceof ExpressionsSource) {
            // If the source was VALUES, see if it's distinct. If so,
            // we can possibly reverse the join and benefit from an
            // index.
            DistinctState distinct = ((ExpressionsSource)input).getDistinctState();
            switch (distinct) {
            case DISTINCT:
            case DISTINCT_WITH_NULL:
                join.setReverseHook(new ReverseHook(true, true));
                break;
            case HAS_PARAMETERS:
                join.setReverseHook(new ReverseHook(true, false));
                break;
            }
        }
        else {
            join.setReverseHook(new ReverseHook(hasDistinct, hasDistinct));
        }
    }
    
    static class ReverseHook implements JoinReverseHook {
        private boolean reversible, distinct;

        public ReverseHook(boolean reversible, boolean distinct) {
            this.reversible = reversible;
            this.distinct = distinct;
        }

        public boolean canReverse(JoinNode join) {
            return reversible;
        }

        public void beforeReverse(JoinNode join) {
            if (!distinct) {
                Joinable right = join.getRight();
                if (right instanceof ExpressionsSource) {
                    ExpressionsSource values = (ExpressionsSource)right;
                    values.setDistinctState(DistinctState.NEED_DISTINCT);
                }
                else {
                    throw new AkibanInternalException("Could not make distinct " + 
                                                      right);
                }
            }
            join.setJoinType(JoinType.INNER);
        }

        public void didNotReverse(JoinNode join) {
            Joinable right = join.getRight();
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
                ConditionList conds = join.getJoinConditions();
                ConditionExpression cond = conds.get(0);
                if (!(cond instanceof ComparisonCondition))
                    return;
                ComparisonCondition ccond = (ComparisonCondition)cond;
                ccond.setRight(project.getFields().get(0));
                if (select != null)
                    conds.addAll(select.getConditions());
                join.replaceInput(right, input);
            }
        }
    }

    public void convert(Select select, ExistsCondition exists) {
        Subquery subquery = exists.getSubquery();
        PlanNode input = subquery.getInput();
        PlanNode sinput = select.getInput();
        ConditionList conditions = null;
        if (input instanceof Select) {
            Select sinner = (Select)input;
            conditions = sinner.getConditions();
            input = sinner.getInput();
        }
        if (!((sinput instanceof Joinable) && (input instanceof Joinable)))
            return;
        JoinNode join = new JoinNode((Joinable)sinput, (Joinable)input, JoinType.SEMI);
        if (conditions != null)
            join.setJoinConditions(conditions);
        select.getConditions().remove(exists);
        select.replaceInput(sinput, join);
    }
    
    static class TopLevelSubqueryCondition {
        Select select;
        SubqueryExpression condition;

        public TopLevelSubqueryCondition(Select select, SubqueryExpression condition) {
            this.select = select;
            this.condition = condition;
        }
    }

    static class ConditionFinder implements PlanVisitor, ExpressionVisitor {
        List<TopLevelSubqueryCondition> result = 
            new ArrayList<TopLevelSubqueryCondition>();

        public List<TopLevelSubqueryCondition> find(PlanNode root) {
            root.accept(this);
            return result;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            if (n instanceof Select) {
                Select select = (Select)n;
                for (ConditionExpression cond : select.getConditions()) {
                    if (cond instanceof SubqueryExpression) {
                        result.add(new TopLevelSubqueryCondition(select, (SubqueryExpression)cond));
                    }
                }
            }
            return true;
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(ExpressionNode n) {
            return true;
        }

        @Override
        public boolean visit(ExpressionNode n) {
            return true;
        }
    }

    static class SubqueryBoundTables  implements PlanVisitor, ExpressionVisitor {
        private Set<ColumnSource> insideTables = new HashSet<ColumnSource>();

        private static enum State { TABLES, FREE, ONLY };
        private State state;

        public SubqueryBoundTables(PlanNode n) {
            state = State.TABLES;
            n.accept(this);
        }

        public boolean freeOfTables(ExpressionNode n) {
            state = State.FREE;
            return n.accept(this);
        }

        public boolean onlyHasTables(ExpressionNode n) {
            state = State.ONLY;
            return n.accept(this);
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            if ((state == State.TABLES) &&
                (n instanceof ColumnSource))
                insideTables.add((ColumnSource)n);
            return true;
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(ExpressionNode n) {
            return true;
        }

        @Override
        public boolean visit(ExpressionNode n) {
            if (n instanceof ColumnExpression) {
                ColumnSource table = ((ColumnExpression)n).getTable();
                switch (state) {
                case FREE:
                    if (insideTables.contains(table))
                        return false;
                    break;
                case ONLY:
                    if (!insideTables.contains(table))
                        return false;
                    break;
                }
            }
            return true;
        }
    }

}
