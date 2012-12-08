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
        if (required.getRequired().isEmpty()) return;
        promoteOuterJoins((Joinable)select.getInput(), required);
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
