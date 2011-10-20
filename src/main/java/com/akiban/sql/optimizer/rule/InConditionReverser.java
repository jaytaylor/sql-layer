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

    public void convert(Select select, AnyCondition any) {
        Subquery subquery = any.getSubquery();
        PlanNode input = subquery.getInput();
        if (!(input instanceof Project))
            return;
        Project project = (Project)input;
        input = project.getInput();
        // The ANY condition becomes the join condition for an
        // EXISTS-like semi-join with its source.
        ConditionExpression cond = (ConditionExpression)project.getFields().get(0);
        PlanNode sinput = select.getInput();
        if (!((sinput instanceof Joinable) && (input instanceof Joinable)))
            return;
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
                join.setReverseHook(new ReverseHook(true));
                break;
            case HAS_PARAMETERS:
                join.setReverseHook(new ReverseHook(false));
                break;
            }
        }
    }
    
    static class ReverseHook implements JoinReverseHook {
        private boolean distinct;

        public ReverseHook(boolean distinct) {
            this.distinct = distinct;
        }

        public boolean canReverse(JoinNode join) {
            return true;
        }

        public void beforeReverse(JoinNode join) {
            if (!distinct) {
                ExpressionsSource values = (ExpressionsSource)join.getRight();
                values.setDistinctState(DistinctState.NEED_DISTINCT);
            }
            join.setJoinType(JoinType.INNER);
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

}
