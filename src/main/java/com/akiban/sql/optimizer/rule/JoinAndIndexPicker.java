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

import com.akiban.sql.optimizer.rule.cost.CostEstimator;
import com.akiban.sql.optimizer.rule.join_enum.*;
import com.akiban.sql.optimizer.rule.join_enum.DPhyp.JoinOperator;

import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;
import com.akiban.sql.optimizer.plan.JoinNode.JoinType;

import com.akiban.server.expression.std.Comparison;

import com.akiban.server.error.AkibanInternalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Pick joins and indexes. 
 * This the the core of actual query optimization.
 */
public class JoinAndIndexPicker extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(JoinAndIndexPicker.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext planContext) {
        BaseQuery query = (BaseQuery)planContext.getPlan();
        List<Picker> pickers = 
            new JoinsFinder((SchemaRulesContext)planContext.getRulesContext())
            .find(query);
        for (Picker picker : pickers) {
            picker.apply();
        }
    }

    static class Picker {
        Map<SubquerySource,Picker> subpickers;
        SchemaRulesContext rulesContext;
        CostEstimator costEstimator;
        Joinable joinable;
        BaseQuery query;
        QueryIndexGoal queryGoal;

        public Picker(Joinable joinable, BaseQuery query,
                      SchemaRulesContext rulesContext,
                      Map<SubquerySource,Picker> subpickers) {
            this.subpickers = subpickers;
            this.rulesContext = rulesContext;
            this.costEstimator = rulesContext.getCostEstimator();
            this.joinable = joinable;
            this.query = query;
        }

        public void apply() {
            queryGoal = determineQueryIndexGoal(joinable);
            if (joinable instanceof TableGroupJoinTree) {
                // Single group.
                pickIndex((TableGroupJoinTree)joinable);
            }
            else if (joinable instanceof JoinNode) {
                // General joins.
                pickJoinsAndIndexes((JoinNode)joinable);
            }
            else if (joinable instanceof SubquerySource) {
                // Single subquery // view. Just do its insides.
                subpicker((SubquerySource)joinable).apply();
            }
            // TODO: Any other degenerate cases?
        }

        protected QueryIndexGoal determineQueryIndexGoal(PlanNode input) {
            ConditionList whereConditions = null;
            Sort ordering = null;
            AggregateSource grouping = null;
            Project projectDistinct = null;
            Limit limit = null;
            input = input.getOutput();
            if (input instanceof Select) {
                ConditionList conds = ((Select)input).getConditions();
                if (!conds.isEmpty()) {
                    whereConditions = conds;
                }
            }
            input = input.getOutput();
            if (input instanceof Sort) {
                ordering = (Sort)input;
                input = input.getOutput();
                if (input instanceof Project)
                    input = input.getOutput();
            }
            else if (input instanceof AggregateSource) {
                grouping = (AggregateSource)input;
                if (!grouping.hasGroupBy())
                    grouping = null;
                input = input.getOutput();
                if (input instanceof Select)
                    input = input.getOutput();
                if (input instanceof Sort) {
                    // Needs to be possible to satisfy both.
                    ordering = (Sort)input;
                    if (grouping != null) {
                        List<ExpressionNode> groupBy = grouping.getGroupBy();
                        for (OrderByExpression orderBy : ordering.getOrderBy()) {
                            ExpressionNode orderByExpr = orderBy.getExpression();
                            if (!((orderByExpr.isColumn() &&
                                   (((ColumnExpression)orderByExpr).getTable() == grouping)) ||
                                  groupBy.contains(orderByExpr))) {
                                ordering = null;
                                break;
                            }
                        }
                    }
                    if (ordering != null) // No limit if sort lost.
                        input = input.getOutput();
                }
                if (input instanceof Project)
                    input = input.getOutput();
            }
            else if (input instanceof Project) {
                Project project = (Project)input;
                input = project.getOutput();
                if (input instanceof Distinct) {
                    projectDistinct = project;
                    input = input.getOutput();
                }
                else if (input instanceof Sort) {
                    ordering = (Sort)input;
                    input = input.getOutput();
                    if (input instanceof Distinct)
                        input = input.getOutput(); // Not projectDistinct (already marked as explicitly sorted).
                }
            }
            if (input instanceof Limit)
                limit = (Limit)input;
            return new QueryIndexGoal(query, costEstimator, whereConditions, 
                                      grouping, ordering, projectDistinct, limit);
        }

        // Only a single group of tables. Don't need to run general
        // join algorithm and can shortcut some of the setup for this
        // group.
        protected void pickIndex(TableGroupJoinTree tables) {
            GroupIndexGoal groupGoal = new GroupIndexGoal(queryGoal, tables);
            List<JoinOperator> empty = Collections.emptyList(); // No more joins / bound tables.
            groupGoal.updateRequiredColumns(empty, empty);
            BaseScan scan = groupGoal.pickBestScan();
            groupGoal.install(scan, null, true, false);
        }

        // General joins: run enumerator.
        protected void pickJoinsAndIndexes(JoinNode joins) {
            Plan rootPlan = new JoinEnumerator(this).run(joins, queryGoal.getWhereConditions()).bestPlan(Collections.<JoinOperator>emptyList());
            installPlan(rootPlan, false);
        }

        // Put the chosen plan in place.
        public void installPlan(Plan rootPlan, boolean copy) {
            joinable.getOutput().replaceInput(joinable, rootPlan.install(copy));
        }

        // Get the handler for the given subquery so that it can be done in context.
        public Picker subpicker(SubquerySource subquery) {
            Picker subpicker = subpickers.get(subquery);
            assert (subpicker != null);
            return subpicker;
        }

        // Subquery but as part of a larger plan tree. Return best
        // plan to be installed with it.
        public Plan subqueryPlan(Set<ColumnSource> subqueryBoundTables,
                                 Collection<JoinOperator> subqueryJoins,
                                 Collection<JoinOperator> subqueryOutsideJoins) {
            if (queryGoal == null)
                queryGoal = determineQueryIndexGoal(joinable);
            if (joinable instanceof TableGroupJoinTree) {
                TableGroupJoinTree tables = (TableGroupJoinTree)joinable;
                GroupIndexGoal groupGoal = new GroupIndexGoal(queryGoal, tables);
                // In this block because we were not a JoinNode, query has no joins itself
                List<JoinOperator> queryJoins = Collections.emptyList();
                List<ConditionList> conditionSources = groupGoal.updateContext(subqueryBoundTables, queryJoins, subqueryJoins, subqueryOutsideJoins, true, null);
                BaseScan scan = groupGoal.pickBestScan();
                CostEstimate costEstimate = scan.getCostEstimate();
                return new GroupPlan(groupGoal, JoinableBitSet.of(0), scan, costEstimate, conditionSources, true, null);
            }
            if (joinable instanceof JoinNode) {
                return new JoinEnumerator(this, subqueryBoundTables, subqueryJoins, subqueryOutsideJoins).run((JoinNode)joinable, queryGoal.getWhereConditions()).bestPlan(Collections.<JoinOperator>emptyList());
            }
            if (joinable instanceof SubquerySource) {
                return subpicker((SubquerySource)joinable).subqueryPlan(subqueryBoundTables, subqueryJoins, subqueryOutsideJoins);
            }
            throw new AkibanInternalException("Unknown join element: " + joinable);
        }

    }

    static abstract class Plan implements Comparable<Plan> {
        CostEstimate costEstimate;

        protected Plan(CostEstimate costEstimate) {
            this.costEstimate = costEstimate;
        }

        public int compareTo(Plan other) {
            return costEstimate.compareTo(other.costEstimate);
        }

        public abstract Joinable install(boolean copy);

        public void addDistinct() {
            throw new UnsupportedOperationException();
        }

        public boolean semiJoinEquivalent() {
            return false;
        }

        public boolean containsColumn(ColumnExpression column) {
            return false;
        }

        public double joinSelectivity() {
            return 1.0;
        }

        public void redoCostWithLimit(long limit) {
        }
    }

    static abstract class PlanClass {
        JoinEnumerator enumerator;        
        long bitset;

        protected PlanClass(JoinEnumerator enumerator, long bitset) {
            this.enumerator = enumerator;
            this.bitset = bitset;
        }

        public abstract Plan bestPlan(Collection<JoinOperator> outsideJoins);

        public Plan bestNestedPlan(PlanClass outerPlan, Collection<JoinOperator> joins, Collection<JoinOperator> outsideJoins) {
            return bestPlan(outsideJoins); // By default, side doesn't matter.
        }
    }
    
    static class GroupPlan extends Plan {
        GroupIndexGoal groupGoal;
        long outerTables;
        BaseScan scan;
        List<ConditionList> conditionSources;
        boolean sortAllowed;
        ConditionList extraConditions;

        public GroupPlan(GroupIndexGoal groupGoal,
                         long outerTables, BaseScan scan, 
                         CostEstimate costEstimate,
                         List<ConditionList> conditionSources,
                         boolean sortAllowed,
                         ConditionList extraConditions) {
            super(costEstimate);
            this.groupGoal = groupGoal;
            this.outerTables = outerTables;
            this.scan = scan;
            this.conditionSources = conditionSources;
            this.sortAllowed = sortAllowed;
            this.extraConditions = extraConditions;
        }

        @Override
        public String toString() {
            return scan.toString();
        }

        @Override
        public Joinable install(boolean copy) {
            if (extraConditions != null) {
                // Move to WHERE clause or join condition so that any
                // that survive indexing are preserved.
                assert ((conditionSources.size() > 1) &&
                        (conditionSources.indexOf(extraConditions) > 0));
                conditionSources.get(0).addAll(extraConditions);
            }
            return groupGoal.install(scan, conditionSources, sortAllowed, copy);
        }

        public boolean orderedForDistinct(Distinct distinct) {
            if (!((distinct.getInput() instanceof Project) &&
                  (scan instanceof IndexScan)))
                return false;
            return groupGoal.orderedForDistinct((Project)distinct.getInput(),
                                                (IndexScan)scan);
        }

        @Override
        public boolean semiJoinEquivalent() {
            return groupGoal.semiJoinEquivalent(scan);
        }

        @Override
        public boolean containsColumn(ColumnExpression column) {
            ColumnSource table = column.getTable();
            if (!(table instanceof TableSource)) return false;
            return groupGoal.getTables().containsTable((TableSource)table);
        }

        @Override
        public double joinSelectivity() {
            if (scan instanceof IndexScan)
                return groupGoal.estimateSelectivity((IndexScan)scan);
            else
                return super.joinSelectivity();
        }

        @Override
        public void redoCostWithLimit(long limit) {
            if (scan instanceof IndexScan) {
                costEstimate = groupGoal.estimateCost((IndexScan)scan, limit);
            }
        }
    }

    static class GroupPlanClass extends PlanClass {
        GroupIndexGoal groupGoal;
        Collection<GroupPlan> bestPlans = new ArrayList<GroupPlan>();

        public GroupPlanClass(JoinEnumerator enumerator, long bitset, 
                              GroupIndexGoal groupGoal) {
            super(enumerator, bitset);
            this.groupGoal = groupGoal;
        }

        protected GroupPlanClass(GroupPlanClass other) {
            super(other.enumerator, other.bitset);
            this.groupGoal = other.groupGoal;
        }

        @Override
        public Plan bestPlan(Collection<JoinOperator> outsideJoins) {
            return bestPlan(JoinableBitSet.empty(), Collections.<JoinOperator>emptyList(), outsideJoins);
        }

        @Override
        public Plan bestNestedPlan(PlanClass outerPlan, Collection<JoinOperator> joins, Collection<JoinOperator> outsideJoins) {
            return bestPlan(outerPlan.bitset, joins, outsideJoins);
        }

        protected ConditionList getExtraConditions() {
            return null;
        }

        protected GroupPlan bestPlan(long outerTables, Collection<JoinOperator> joins, Collection<JoinOperator> outsideJoins) {
            for (GroupPlan groupPlan : bestPlans) {
                if (groupPlan.outerTables == outerTables) {
                    return groupPlan;
                }
            }
            boolean sortAllowed = joins.isEmpty();
            List<ConditionList> conditionSources = groupGoal.updateContext(enumerator.boundTables(outerTables), joins, joins, outsideJoins, sortAllowed, getExtraConditions());
            BaseScan scan = groupGoal.pickBestScan();
            CostEstimate costEstimate = scan.getCostEstimate();
            GroupPlan groupPlan = new GroupPlan(groupGoal, outerTables, scan, costEstimate, conditionSources, sortAllowed, getExtraConditions());
            bestPlans.add(groupPlan);
            return groupPlan;
        }
    }

    static class SubqueryPlan extends Plan {
        SubquerySource subquery;
        Picker picker;
        long outerTables;
        Plan rootPlan;

        public SubqueryPlan(SubquerySource subquery, Picker picker,
                            long outerTables, Plan rootPlan, 
                            CostEstimate costEstimate) {
            super(costEstimate);
            this.subquery = subquery;
            this.picker = picker;
            this.outerTables = outerTables;
            this.rootPlan = rootPlan;
        }

        @Override
        public String toString() {
            return rootPlan.toString();
        }


        @Override
        public Joinable install(boolean copy) {
            picker.installPlan(rootPlan, copy);
            return subquery;
        }        

        @Override
        public void addDistinct() {
            Subquery output = subquery.getSubquery();
            PlanNode input = output.getInput();
            Distinct distinct = new Distinct(input);
            output.replaceInput(input, distinct);
            if ((rootPlan instanceof GroupPlan) &&
                ((GroupPlan)rootPlan).orderedForDistinct(distinct)) {
                distinct.setImplementation(Distinct.Implementation.PRESORTED);
            }
        }
    }

    static class SubqueryPlanClass extends PlanClass {
        SubquerySource subquery;
        Picker picker;
        Collection<SubqueryPlan> bestPlans = new ArrayList<SubqueryPlan>();

        public SubqueryPlanClass(JoinEnumerator enumerator, long bitset, 
                                 SubquerySource subquery, Picker picker) {
            super(enumerator, bitset);
            this.subquery = subquery;
            this.picker = picker;
        }

        @Override
        public Plan bestPlan(Collection<JoinOperator> outsideJoins) {
            return bestPlan(JoinableBitSet.empty(), Collections.<JoinOperator>emptyList(), outsideJoins);
        }

        @Override
        public Plan bestNestedPlan(PlanClass outerPlan, Collection<JoinOperator> joins, Collection<JoinOperator> outsideJoins) {
            return bestPlan(outerPlan.bitset, joins, outsideJoins);
        }

        protected SubqueryPlan bestPlan(long outerTables, Collection<JoinOperator> joins, Collection<JoinOperator> outsideJoins) {
            for (SubqueryPlan subqueryPlan : bestPlans) {
                if (subqueryPlan.outerTables == outerTables) {
                    return subqueryPlan;
                }
            }
            Plan rootPlan = picker.subqueryPlan(enumerator.boundTables(outerTables), joins, outsideJoins);
            CostEstimate costEstimate = rootPlan.costEstimate;
            SubqueryPlan subqueryPlan = new SubqueryPlan(subquery, picker,
                                                         outerTables, rootPlan, 
                                                         costEstimate);
            bestPlans.add(subqueryPlan);
            return subqueryPlan;
        }
    }

    static class ValuesPlan extends Plan {
        ExpressionsSource values;
        
        public ValuesPlan(ExpressionsSource values, CostEstimate costEstimate) {
            super(costEstimate);
            this.values = values;
        }

        @Override
        public String toString() {
            return values.getName();
        }


        @Override
        public Joinable install(boolean copy) {
            return values;
        }

        @Override
        public void addDistinct() {
            values.setDistinctState(ExpressionsSource.DistinctState.NEED_DISTINCT);
        }
    }

    static class ValuesPlanClass extends PlanClass {
        ValuesPlan plan, nestedPlan;

        public ValuesPlanClass(JoinEnumerator enumerator, long bitset, 
                               ExpressionsSource values, Picker picker) {
            super(enumerator, bitset);
            CostEstimator costEstimator = picker.costEstimator;
            this.plan = new ValuesPlan(values, costEstimator.costValues(values, false));
            // Nested also needs to check the join condition with Select.
            this.nestedPlan = new ValuesPlan(values, costEstimator.costValues(values, true));
        }

        @Override
        public Plan bestPlan(Collection<JoinOperator> outsideJoins) {
            return plan;
        }

        @Override
        public Plan bestNestedPlan(PlanClass outerPlan, Collection<JoinOperator> joins, Collection<JoinOperator> outsideJoins) {
            return nestedPlan;
        }
    }

    static class JoinPlan extends Plan {
        Plan left, right;
        JoinType joinType;
        JoinNode.Implementation joinImplementation;
        Collection<JoinOperator> joins;
        boolean needDistinct;
        
        public JoinPlan(Plan left, Plan right, 
                        JoinType joinType, JoinNode.Implementation joinImplementation,
                        Collection<JoinOperator> joins, CostEstimate costEstimate) {
            super(costEstimate);
            this.left = left;
            this.right = right;
            switch (joinType) {
            case SEMI_INNER_ALREADY_DISTINCT:
            case SEMI_INNER_IF_DISTINCT:
                this.joinType = JoinType.SEMI;
                break;
            case INNER_NEED_DISTINCT:
                this.joinType = JoinType.INNER;
                needDistinct = true;
                break;
            default:
                this.joinType = joinType;
            }
            this.joinImplementation = joinImplementation;
            this.joins = joins;
        }

        @Override
        public String toString() {
            return "(" + left + ") " +
                joinType + "/" + joinImplementation +
                " (" + right + ")";
        }

        @Override
        public Joinable install(boolean copy) {
            if (needDistinct)
                left.addDistinct();
            Joinable leftJoinable = left.install(copy);
            Joinable rightJoinable = right.install(copy);
            ConditionList joinConditions = mergeJoinConditions(joins);
            JoinNode join = new JoinNode(leftJoinable, rightJoinable, joinType);
            join.setJoinConditions(joinConditions);
            join.setImplementation(joinImplementation);
            if (joinType == JoinType.SEMI)
                InConditionReverser.cleanUpSemiJoin(join, rightJoinable);
            return join;
        }

        protected ConditionList mergeJoinConditions(Collection<JoinOperator> joins) {
            ConditionList joinConditions = null;
            boolean newJoinConditions = false;
            for (JoinOperator joinOp : joins) {
                if ((joinOp.getJoinConditions() != null) &&
                    !joinOp.getJoinConditions().isEmpty()) {
                    if (joinConditions == null) {
                        joinConditions = joinOp.getJoinConditions();
                    }
                    else { 
                        if (!newJoinConditions) {
                            joinConditions = new ConditionList(joinConditions);
                            newJoinConditions = true;
                        }
                        joinConditions.addAll(joinOp.getJoinConditions());
                    }
                }
            }
            return joinConditions;
        }

        public void redoCostWithLimit(long limit) {
            left.redoCostWithLimit(limit);
            costEstimate = left.costEstimate.nest(right.costEstimate);
        }
    }

    static class HashJoinPlan extends JoinPlan {
        Plan loader;
        BaseHashTable hashTable;
        List<ExpressionNode> hashColumns, matchColumns;
        
        public HashJoinPlan(Plan loader, Plan input, Plan check,
                            JoinType joinType, JoinNode.Implementation joinImplementation,
                            Collection<JoinOperator> joins, CostEstimate costEstimate,
                            BaseHashTable hashTable, List<ExpressionNode> hashColumns, List<ExpressionNode> matchColumns) {
            super(input, check, joinType, joinImplementation, joins, costEstimate);
            this.loader = loader;
            this.hashTable = hashTable;
            this.hashColumns = hashColumns;
            this.matchColumns = matchColumns;
        }

        @Override
        public Joinable install(boolean copy) {
            if (needDistinct)
                left.addDistinct();
            Joinable loaderJoinable = loader.install(true);
            Joinable inputJoinable = left.install(copy);
            Joinable checkJoinable = right.install(copy);
            ConditionList joinConditions = mergeJoinConditions(joins);
            HashJoinNode join = new HashJoinNode(loaderJoinable, inputJoinable, checkJoinable, joinType, hashTable, hashColumns, matchColumns);
            join.setJoinConditions(joinConditions);
            join.setImplementation(joinImplementation);
            if (joinType == JoinType.SEMI)
                InConditionReverser.cleanUpSemiJoin(join, checkJoinable);
            return join;
        }
    }

    static class JoinPlanClass extends PlanClass {
        JoinPlan bestPlan;      // TODO: Later have separate sorted, etc.

        public JoinPlanClass(JoinEnumerator enumerator, long bitset) {
            super(enumerator, bitset);
        }

        @Override
        public Plan bestPlan(Collection<JoinOperator> outsideJoins) {
            return bestPlan;
        }

        public void consider(JoinPlan joinPlan) {
            if (bestPlan == null) {
                logger.debug("Selecting {}, {}", joinPlan, joinPlan.costEstimate);
                bestPlan = joinPlan;
            }
            else if (bestPlan.compareTo(joinPlan) > 0) {
                logger.debug("Preferring {}, {}", joinPlan, joinPlan.costEstimate);
                bestPlan = joinPlan;
            }
            else {
                logger.debug("Rejecting {}, {}", joinPlan, joinPlan.costEstimate);
            }
        }
    }

    static class JoinEnumerator extends DPhyp<PlanClass> {
        private Picker picker;
        private Set<ColumnSource> subqueryBoundTables;
        private Collection<JoinOperator> subqueryJoins, subqueryOutsideJoins;

        public JoinEnumerator(Picker picker) {
            this.picker = picker;
        }

        public JoinEnumerator(Picker picker, Set<ColumnSource> subqueryBoundTables, Collection<JoinOperator> subqueryJoins, Collection<JoinOperator> subqueryOutsideJoins) {
            this.picker = picker;
            this.subqueryBoundTables = subqueryBoundTables;
            this.subqueryJoins = subqueryJoins;
            this.subqueryOutsideJoins = subqueryOutsideJoins;
        }

        @Override
        public PlanClass evaluateTable(long s, Joinable joinable) {
            // Seed with the right plan class to hold state / alternatives.
            if (joinable instanceof TableGroupJoinTree) {
                GroupIndexGoal groupGoal = new GroupIndexGoal(picker.queryGoal, 
                                                              (TableGroupJoinTree)joinable);
                return new GroupPlanClass(this, s, groupGoal);
            }
            if (joinable instanceof SubquerySource) {
                SubquerySource subquery = (SubquerySource)joinable;
                Picker subpicker = picker.subpicker(subquery);
                return new SubqueryPlanClass(this, s, subquery, subpicker);
            }
            if (joinable instanceof ExpressionsSource) {
                return new ValuesPlanClass(this, s, (ExpressionsSource)joinable, picker);
            }
            throw new AkibanInternalException("Unknown join element: " + joinable);
        }

        @Override
        public PlanClass evaluateJoin(long leftBitset, PlanClass left, 
                                      long rightBitset, PlanClass right, 
                                      long bitset, PlanClass existing,
                                      JoinType joinType, Collection<JoinOperator> joins, Collection<JoinOperator> outsideJoins) {
            JoinPlanClass planClass = (JoinPlanClass)existing;
            if (planClass == null)
                planClass = new JoinPlanClass(this, bitset);
            joins = new ArrayList<JoinOperator>(joins);
            Collection<JoinOperator> condJoins = joins; // Joins with conditions for indexing.
            if (subqueryJoins != null) {
                // "Push down" joins into the subquery. Since these
                // are joins to the dervived table, they still need to
                // be recognized to match an indexable column.
                condJoins = new ArrayList<JoinOperator>(joins);
                condJoins.addAll(subqueryJoins);
            }
            if (subqueryOutsideJoins != null) {
                outsideJoins.addAll(subqueryOutsideJoins);
            }
            outsideJoins.addAll(joins); // Total set for outer; inner must subtract.
            // TODO: Divvy up sorting. Consider group joins. Consider merge joins.
            Plan leftPlan = left.bestPlan(outsideJoins);
            Plan rightPlan = right.bestNestedPlan(left, condJoins, outsideJoins);
            CostEstimate costEstimate = leftPlan.costEstimate.nest(rightPlan.costEstimate);
            JoinPlan joinPlan = new JoinPlan(leftPlan, rightPlan,
                                             joinType, JoinNode.Implementation.NESTED_LOOPS,
                                             joins, costEstimate);
            if (joinType.isSemi() || rightPlan.semiJoinEquivalent()) {
                Plan loaderPlan = right.bestPlan(outsideJoins);
                JoinPlan hashPlan = buildBloomFilterSemiJoin(loaderPlan, joinPlan);
                if (hashPlan != null)
                    planClass.consider(hashPlan);
            }
            planClass.consider(joinPlan);
            return planClass;
        }

        /** Get the tables that correspond to the given bitset, plus
         * any that are bound outside the subquery, either
         * syntactically or via joins to it.
         */
        public Set<ColumnSource> boundTables(long tables) {
            if (JoinableBitSet.isEmpty(tables) &&
                (subqueryBoundTables == null))
                return picker.queryGoal.getQuery().getOuterTables();
            Set<ColumnSource> boundTables = new HashSet<ColumnSource>();
            boundTables.addAll(picker.queryGoal.getQuery().getOuterTables());
            if (subqueryBoundTables != null)
                boundTables.addAll(subqueryBoundTables);
            if (!JoinableBitSet.isEmpty(tables)) {
                for (int i = 0; i < 64; i++) {
                    if (JoinableBitSet.overlaps(tables, JoinableBitSet.of(i))) {
                        Joinable table = getTable(i);
                        if (table instanceof TableGroupJoinTree) {
                            for (TableGroupJoinTree.TableGroupJoinNode gtable : (TableGroupJoinTree)table) {
                                boundTables.add(gtable.getTable());
                            }
                        }
                        else {
                            boundTables.add((ColumnSource)table);
                        }
                    }
                }
            }
            return boundTables;
        }

        double BLOOM_FILTER_MAX_SELECTIVITY_DEFAULT = 0.05;

        public JoinPlan buildBloomFilterSemiJoin(Plan loaderPlan, JoinPlan joinPlan) {
            Plan inputPlan = joinPlan.left;
            Plan checkPlan = joinPlan.right;
            Collection<JoinOperator> joins = joinPlan.joins;
            if (checkPlan.costEstimate.getRowCount() > 1)
                return null;    // Join not selective.
            double maxSelectivity;
            String prop = picker.rulesContext.getProperty("bloomFilterMaxSelectivity");
            if (prop != null)
                maxSelectivity = Double.valueOf(prop);
            else
                maxSelectivity = BLOOM_FILTER_MAX_SELECTIVITY_DEFAULT;
            if (maxSelectivity <= 0.0) return null; // Feature turned off.
            List<ExpressionNode> hashColumns = new ArrayList<ExpressionNode>();
            List<ExpressionNode> matchColumns = new ArrayList<ExpressionNode>();
            for (JoinOperator join : joins) {
                if (join.getJoinConditions() != null) {
                    for (ConditionExpression cond : join.getJoinConditions()) {
                        if (!(cond instanceof ComparisonCondition)) return null;
                        ComparisonCondition ccond = (ComparisonCondition)cond;
                        if (ccond.getOperation() != Comparison.EQ) return null;
                        ExpressionNode left = ccond.getLeft();
                        ExpressionNode right = ccond.getRight();
                        // TODO: Could allow somewhat more
                        // complicated, provided still just use one
                        // side's tables.
                        if (!((left instanceof ColumnExpression) &&
                              (right instanceof ColumnExpression)))
                            return null;
                        if (inputPlan.containsColumn((ColumnExpression)left) && 
                            checkPlan.containsColumn((ColumnExpression)right)) {
                            matchColumns.add(left);
                            hashColumns.add(right);
                        }
                        else if (inputPlan.containsColumn((ColumnExpression)right) && 
                                 checkPlan.containsColumn((ColumnExpression)left)) {
                            matchColumns.add(right);
                            hashColumns.add(left);
                        }
                        else {
                            return null;
                        }
                    }
                }
            }
            double selectivity = checkPlan.joinSelectivity();
            if (selectivity > maxSelectivity)
                return null;
            long limit = picker.queryGoal.getLimit();
            if (joinPlan.costEstimate.getRowCount() == limit) {
                // If there was a limit, it was applied too liberally
                // for the very non-selective join.
                // TODO: This should have always been the cost of the
                // join plan, but that would be too disruptive, so
                // only do it when need to make the comparison with
                // the Bloom filter accurate.
                limit = Math.round(limit / selectivity);
                joinPlan.redoCostWithLimit(limit);
            }
            BloomFilter bloomFilter = new BloomFilter(loaderPlan.costEstimate.getRowCount(), 1);
            CostEstimate costEstimate = picker.costEstimator
                .costBloomFilter(loaderPlan.costEstimate, inputPlan.costEstimate, checkPlan.costEstimate, selectivity);
            return new HashJoinPlan(loaderPlan, inputPlan, checkPlan,
                                    JoinType.SEMI, JoinNode.Implementation.BLOOM_FILTER,
                                    joins, costEstimate, bloomFilter, hashColumns, matchColumns);
        }
    }
    
    // Purpose is twofold: 
    // Find top-level joins and note what query they come from; 
    // Annotate subqueries with their outer table references.
    // Top-level queries and those used in expressions are returned directly.
    // Derived tables are deferred, since they need to be planned in
    // the context of various join orders to allow for join predicated
    // to be pushed "inside." So they are stored in a Map accessible
    // to other Pickers.
    static class JoinsFinder implements PlanVisitor, ExpressionVisitor {
        List<Picker> result;
        Map<SubquerySource,Picker> subpickers;
        BaseQuery rootQuery;
        Deque<SubqueryState> subqueries = new ArrayDeque<SubqueryState>();
        SchemaRulesContext rulesContext;

        public JoinsFinder(SchemaRulesContext rulesContext) {
            this.rulesContext = rulesContext;
        }

        public List<Picker> find(BaseQuery query) {
            result = new ArrayList<Picker>();
            subpickers = new HashMap<SubquerySource,Picker>();
            rootQuery = query;
            query.accept(this);
            result.removeAll(subpickers.values()); // Do these in context.
            return result;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            if (n instanceof Subquery) {
                subqueries.push(new SubqueryState((Subquery)n));
                return true;
            }
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            if (n instanceof Subquery) {
                SubqueryState s = subqueries.pop();
                Set<ColumnSource> outerTables = s.getTablesReferencedButNotDefined();
                s.subquery.setOuterTables(outerTables);
                if (!subqueries.isEmpty())
                    subqueries.peek().tablesReferenced.addAll(outerTables);
            }
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            if (!subqueries.isEmpty() &&
                (n instanceof ColumnSource)) {
                boolean added = subqueries.peek().tablesDefined.add((ColumnSource)n);
                assert added : "Table defined more than once";
            }
            if ((n instanceof Joinable) && !(n instanceof TableSource)) {
                Joinable j = (Joinable)n;
                while (j.getOutput() instanceof Joinable)
                    j = (Joinable)j.getOutput();
                BaseQuery query = rootQuery;
                SubquerySource subquerySource = null;
                if (!subqueries.isEmpty()) {
                    query = subqueries.peek().subquery;
                    if (query.getOutput() instanceof SubquerySource)
                        subquerySource = (SubquerySource)query.getOutput();
                }
                for (Picker picker : result) {
                    if (picker.joinable == j)
                        // Already have another set of joins to same root join.
                        return true;
                }
                Picker picker = new Picker(j, query, rulesContext, subpickers);
                result.add(picker);
                if (subquerySource != null)
                    subpickers.put(subquerySource, picker);
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
            if (!subqueries.isEmpty() &&
                (n instanceof ColumnExpression)) {
                subqueries.peek().tablesReferenced.add(((ColumnExpression)n).getTable());
            }
            return true;
        }
    }

    static class SubqueryState {
        Subquery subquery;
        Set<ColumnSource> tablesReferenced = new HashSet<ColumnSource>();
        Set<ColumnSource> tablesDefined = new HashSet<ColumnSource>();

        public SubqueryState(Subquery subquery) {
            this.subquery = subquery;
        }

        public Set<ColumnSource> getTablesReferencedButNotDefined() {
            tablesReferenced.removeAll(tablesDefined);
            return tablesReferenced;
        }
    }

}
