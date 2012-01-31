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

import com.akiban.server.error.UnsupportedSQLException;

import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.JoinNode.JoinType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Turn outer joins where the optional side of the join has a WHERE
 * condition on it (other that those that permit NULLs like IS NULL)
 * into inner joins.
 */
public class OuterJoinPromoter extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(OuterJoinPromoter.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    static class WhereFinder implements PlanVisitor, ExpressionVisitor {
        List<Select> result = new ArrayList<Select>();

        public List<Select> find(PlanNode root) {
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
                Select f = (Select)n;
                if (f.getInput() instanceof Joinable) {
                    result.add(f);
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

    @Override
    public void apply(PlanContext plan) {
        List<Select> wheres = new WhereFinder().find(plan.getPlan());
        for (Select select : wheres) {
            doJoins(select);
        }
    }

    static class RequiredSources implements ExpressionVisitor {
        private Set<ColumnSource> required = new HashSet<ColumnSource>();
        private Deque<ExpressionNode> stack = new ArrayDeque<ExpressionNode>();
        
        public RequiredSources() {
        }

        public Set<ColumnSource> getRequired() {
            return required;
        }

        public void intersect(RequiredSources other) {
            required.retainAll(other.required);
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            stack.push(n);
            return visit(n);
        }

        @Override
        public boolean visitLeave(ExpressionNode n) {
            stack.pop();
            return true;
        }

        @Override
        public boolean visit(ExpressionNode n) {
            if (n instanceof ColumnExpression) {
                if (!insideDangerousFunction())
                    required.add(((ColumnExpression)n).getTable());
            }
            return true;
        }

        protected boolean insideDangerousFunction() {
            for (ExpressionNode inside : stack) {
                if (inside instanceof FunctionExpression) {
                    String name = ((FunctionExpression)inside).getFunction();
                    if (name.equals("COALESCE"))
                        return true;
                }
            }
            return false;
        }

        // Given a condition, collect all the column sources it implies aren't all null.
        protected void gatherRequired(ConditionExpression condition) {
            if (condition instanceof FunctionCondition) {
                FunctionCondition fcond = (FunctionCondition)condition;
                String fname = fcond.getFunction();
                if (fname.equals("and")) {
                    for (ExpressionNode operand : fcond.getOperands()) {
                        gatherRequired((ConditionExpression)operand);
                    }
                    return;
                }
                else if (fname.equals("or")) {
                    // Same table must be mentioned down every branch of the OR.
                    RequiredSources intersection = null;
                    for (ExpressionNode operand : fcond.getOperands()) {
                        RequiredSources opreq = new RequiredSources();
                        opreq.gatherRequired((ConditionExpression)operand);
                        if (intersection == null)
                            intersection = opreq;
                        else
                            intersection.intersect(opreq);
                    }
                    required.addAll(intersection.getRequired());
                    return;
                }
                else if (fname.equals("not") ||
                         fname.equals("isNull")) {
                    // These are too complicated to understand.
                    return;
                }
            }
            // Conditions, functions such as LIKE, etc.
            condition.accept(this);
        }
    }

    protected void doJoins(Select select) {
        RequiredSources required = new RequiredSources();
        for (ConditionExpression condition : select.getConditions()) {
            required.gatherRequired(condition);
        }
        Set<ColumnSource> sources = required.getRequired();
        if (sources.isEmpty()) return;
        promoteOuterJoins((Joinable)select.getInput(), sources);
    }

    protected boolean promoteOuterJoins(Joinable joinable,
                                        Collection<ColumnSource> required) {
        if (required.contains(joinable))
            return true;
        if (joinable instanceof JoinNode) {
            JoinNode join = (JoinNode)joinable;
            boolean lp = promoteOuterJoins(join.getLeft(), required);
            boolean rp = promoteOuterJoins(join.getRight(), required);
            boolean promoted = false;
            switch (join.getJoinType()) {
            case LEFT:
                promoted = rp;
                break;
            case RIGHT:
                promoted = lp;
                break;
            }
            if (promoted) {
                join.setJoinType(JoinType.INNER);
                promotedOuterJoin(join);
            }
            return lp || rp;
        }
        return false;
    }

    // Walk back down recomputing the required/optional flag.
    protected void promotedOuterJoin(Joinable joinable) {
        if (joinable instanceof JoinNode) {
            JoinNode join = (JoinNode)joinable;
            if (join.getJoinType() == JoinType.INNER) {
                promotedOuterJoin(join.getLeft());
                promotedOuterJoin(join.getRight());
            }
        }
        else if (joinable instanceof TableSource) {
            ((TableSource)joinable).setRequired(true);
        }
    }
    
}
