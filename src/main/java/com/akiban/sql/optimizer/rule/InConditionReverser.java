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
import com.akiban.sql.optimizer.plan.ExpressionsSource.DistinctState;

import com.akiban.server.error.AkibanInternalException;

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
        if (input instanceof Select) {
            Select inselect = (Select)input;
            joinConditions.addAll(inselect.getConditions());
            input = inselect.getInput();
        }
        if (input instanceof Joinable) {
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
        // Clean split in table references.  Join with derived table
        // whose columns are the RHS of the ANY comparisons, which
        // then references that table instead. That way the table
        // works if put on the outer side of a nested loop join as
        // well. If it stays on the inside, the subquery will be
        // elided later.
        EquivalenceFinder<ColumnExpression> emptyEquivs = any.getSubquery().getColumnEquivalencies();
        SubquerySource subquerySource = new SubquerySource(new Subquery(project, emptyEquivs), "ANY");
        projectFields.clear();
        for (ConditionExpression cond : joinConditions) {
            ComparisonCondition ccond = (ComparisonCondition)cond;
            ExpressionNode cright = ccond.getRight();
            projectFields.add(cright);
            ccond.setRight(new ColumnExpression(subquerySource,
                                                projectFields.size() - 1,
                                                cright.getSQLtype(),
                                                cright.getAkType(),
                                                cright.getSQLsource()));
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
        join.setJoinConditions(joinConditions);
        select.getConditions().remove(selectElement);
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
                ConditionExpression cond = conds.get(0);
                if (!(cond instanceof ComparisonCondition))
                    return;
                ComparisonCondition ccond = (ComparisonCondition)cond;
                ccond.setRight(project.getFields().get(0));
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
        JoinNode join = new JoinNode((Joinable)sinput, (Joinable)input,
                                     (negated) ? JoinType.ANTI : JoinType.SEMI);
        if (conditions != null)
            join.setJoinConditions(conditions);
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
