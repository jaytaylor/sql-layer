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
        List<Select> result = new ArrayList<>();

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
        private Set<ColumnSource> required = new HashSet<>();
        private Deque<ExpressionNode> stack = new ArrayDeque<>();
        
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
                else if (inside instanceof IfElseExpression) {
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
            else if (condition instanceof AnyCondition) {
                Subquery subquery = ((AnyCondition)condition).getSubquery();
                if (subquery.getInput() instanceof Project) {
                    Project project = (Project)subquery.getInput();
                    gatherRequired((ConditionExpression)project.getFields().get(0));
                }
                return;
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
        Joinable joinable = (Joinable)select.getInput();
        gatherInnerJoinConditions(joinable, required);
        if (required.getRequired().isEmpty()) return;
        promoteOuterJoins(joinable, required);
    }

    // Predicates expressed as join conditions on an INNER join
    // reachable from the top are also promoting.
    protected void gatherInnerJoinConditions(Joinable joinable,
                                             RequiredSources required) {
        if (joinable instanceof JoinNode) {
            JoinNode join = (JoinNode)joinable;
            switch (join.getJoinType()) {
            case INNER:
                if (join.getJoinConditions() != null) {
                    for (ConditionExpression condition : join.getJoinConditions()) {
                        required.gatherRequired(condition);
                    }
                }
                gatherInnerJoinConditions(join.getLeft(), required);
                gatherInnerJoinConditions(join.getRight(), required);
                break;
            case LEFT:
            case SEMI:
                gatherInnerJoinConditions(join.getLeft(), required);
                break;
            case RIGHT:
                gatherInnerJoinConditions(join.getRight(), required);
                break;
            }
        }
    }

    protected boolean promoteOuterJoins(Joinable joinable,
                                        RequiredSources required) {
        if (required.getRequired().contains(joinable))
            return true;
        if (joinable instanceof JoinNode) {
            JoinNode join = (JoinNode)joinable;
            while (true) {
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
                    int sizeBefore = required.getRequired().size();
                    for (ConditionExpression condition : join.getJoinConditions()) {
                        required.gatherRequired(condition);
                    }
                    if (sizeBefore < required.getRequired().size())
                        continue;
                }
                return lp || rp;
            }
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
