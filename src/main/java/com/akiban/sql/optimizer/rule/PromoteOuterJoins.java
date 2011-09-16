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

import com.akiban.qp.physicaloperator.API.JoinType;

import java.util.*;

/** Turn outer joins where the optional side of the join has a WHERE
 * condition on it (other that those that permit NULLs like IS NULL)
 * into inner joins.
 */
public class PromoteOuterJoins extends BaseRule
{
    static class WhereFinder implements PlanVisitor, ExpressionVisitor {
        List<Filter> result = new ArrayList<Filter>();

        public List<Filter> find(PlanNode root) {
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
            if (n instanceof Filter) {
                Filter f = (Filter)n;
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
    public PlanNode apply(PlanNode plan) {
        List<Filter> wheres = new WhereFinder().find(plan);
        for (Filter filter : wheres) {
            doJoins(filter);
        }
        return plan;
    }

    static class RequiredSources implements ExpressionVisitor {
        private Set<ColumnSource> required = new HashSet<ColumnSource>();
        private Stack<ExpressionNode> stack = new Stack<ExpressionNode>();
        
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
                         fname.equals("isNullOp")) {
                    // These are too complicated to understand.
                    return;
                }
            }
            // Conditions, functions such as LIKE, etc.
            condition.accept(this);
        }
    }

    protected void doJoins(Filter filter) {
        RequiredSources required = new RequiredSources();
        for (ConditionExpression condition : filter.getConditions()) {
            required.gatherRequired(condition);
        }
        Set<ColumnSource> sources = required.getRequired();
        if (sources.isEmpty()) return;
        promoteOuterJoins((Joinable)filter.getInput(), sources);
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
            case LEFT_JOIN:
                promoted = rp;
                break;
            case RIGHT_JOIN:
                promoted = lp;
                break;
            }
            if (promoted) {
                join.setJoinType(JoinType.INNER_JOIN);
                promotedOuterJoin(join);
            }
            return lp || rp;
        }
        return false;
    }

    protected void promotedOuterJoin(Joinable joinable) {
        if (joinable instanceof JoinNode) {
            JoinNode join = (JoinNode)joinable;
            promotedOuterJoin(join.getLeft());
            promotedOuterJoin(join.getRight());
        }
        // TODO: Mark not optional once we have that state.
    }
    
}
