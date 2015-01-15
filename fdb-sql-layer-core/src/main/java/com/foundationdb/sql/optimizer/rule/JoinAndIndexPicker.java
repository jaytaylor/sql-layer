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

import com.foundationdb.server.types.TKeyComparable;
import com.foundationdb.sql.optimizer.rule.cost.CostEstimator;
import com.foundationdb.sql.optimizer.rule.join_enum.*;
import com.foundationdb.sql.optimizer.rule.join_enum.DPhyp.ExpressionTables;
import com.foundationdb.sql.optimizer.rule.join_enum.DPhyp.JoinOperator;

import com.foundationdb.sql.optimizer.plan.*;
import com.foundationdb.sql.optimizer.plan.Sort.OrderByExpression;
import com.foundationdb.sql.optimizer.plan.JoinNode.JoinType;

import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.texpressions.Comparison;
import com.foundationdb.sql.types.CharacterTypeAttributes;

import com.foundationdb.server.error.AkibanInternalException;

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
        List<Picker> pickers = new JoinsFinder(planContext).find();
        for (Picker picker : pickers) {
            picker.apply();
        }
    }

    static class Picker {
        Map<SubquerySource,Picker> subpickers;
        PlanContext planContext;
        SchemaRulesContext rulesContext;
        Joinable joinable;
        BaseQuery query;
        QueryIndexGoal queryGoal;
        ConditionList originalSubqueryWhereConditions;
        boolean enableFKJoins = true;

        public Picker(Joinable joinable, BaseQuery query,
                      PlanContext planContext,
                      Map<SubquerySource,Picker> subpickers) {
            this.subpickers = subpickers;
            this.planContext = planContext;
            this.rulesContext = (SchemaRulesContext)planContext.getRulesContext();
            this.joinable = joinable;
            this.query = query;
        }

        public CostEstimator getCostEstimator() {
            return rulesContext.getCostEstimator();
        }
        
        public PlanContext getPlanContext() { 
            return planContext;
        }
        
        public Joinable rootJoin() { 
            return joinable;
        }
        
        public void apply() {
            queryGoal = determineQueryIndexGoal(joinable);
            if (joinable instanceof TableGroupJoinTree) {
                // Single group.
                pickIndex((TableGroupJoinTree)joinable);
            }
            else if (joinable instanceof JoinNode) {
                // General joins.
                pickJoinsAndIndexes ((JoinNode)joinable);
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
                whereConditions = ((Select)input).getConditions();
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
            return new QueryIndexGoal(query, rulesContext, whereConditions, 
                                      grouping, ordering, projectDistinct, limit);
        }

        // Only a single group of tables. Don't need to run general
        // join algorithm and can shortcut some of the setup for this
        // group.
        protected void pickIndex(TableGroupJoinTree tables) {
            GroupIndexGoal groupGoal = new GroupIndexGoal(queryGoal, tables, planContext);
            List<JoinOperator> empty = Collections.emptyList(); // No more joins / bound tables.
            groupGoal.updateRequiredColumns(empty, empty);
            BaseScan scan = groupGoal.pickBestScan();
            groupGoal.install(scan, null, true, false);
            query.setCostEstimate(scan.getCostEstimate());
        }

        protected void pickJoinsAndIndexes (JoinNode joins) {
            Plan rootPlan = pickRootJoinPlan(joins, true);
            installPlan (rootPlan, false);
        }
        
        protected Plan pickRootJoinPlan (JoinNode joins, boolean sortAllowed) {
            JoinEnumerator processor = new JoinEnumerator (this);
            List<Joinable> tables = new ArrayList<>();
            JoinEnumerator.addTables(joins, tables);
            
            int threshold = Integer.parseInt(rulesContext.getProperty("fk_join_threshold", "8"));
            int tableCount = tables.size();
            // Do the full JoinEnumeration processing if 
            // The number of tables in the set is smaller than our configuration threshold OR
            // The top join in the query isn't a FK join.
            if (tableCount <= threshold || ((JoinNode)joinable).getFKJoin() == null) {
                return processor.run(joins, queryGoal.getWhereConditions()).
                        bestPlan(Collections.<JoinOperator>emptyList(), sortAllowed);
            } else {
                processor.init(joins, null);
            }
            

            // The code that follows has been extracted and simplified from
            // the processor.init() and processor.run() methods to handle the
            // one join case 
            List<JoinOperator> operators = new ArrayList<>();
            
            JoinOperator op = new JoinOperator(joins);
            operators.add(op); 
            long leftTable = JoinableBitSet.empty();
            long rightTable = JoinableBitSet.empty();
            
            if (joins.getLeft() instanceof TableGroupJoinTree) {
                leftTable = processor.getTableBit(joins.getLeft());
            }
            if (joins.getRight() instanceof TableGroupJoinTree) {
                rightTable = processor.getTableBit(joins.getRight());
            }
            
            ExpressionTables visitor = new ExpressionTables(processor.getTableBitSets());

            //protected void addWhereConditions(ConditionList whereConditions, ExpressionTables visitor) {            
            Iterator<ConditionExpression> iter = queryGoal.getWhereConditions().iterator();
            while (iter.hasNext()) {
                ConditionExpression condition = iter.next();
                if (condition instanceof ComparisonCondition) {
                    ComparisonCondition comp = (ComparisonCondition)condition;
                    long columnTables = columnReferenceTable(comp.getLeft(), processor.getTableBitSets());
                    if (!JoinableBitSet.isEmpty(columnTables)) {
                        long rhs = visitor.getTables(comp.getRight());
                        if (visitor.wasNullTolerant()) continue;
                        if (!JoinableBitSet.isEmpty(rhs) &&
                                !JoinableBitSet.overlaps(columnTables, rhs) &&
                                joins.getFKJoin().getConditions().contains(comp)) {
                            operators.add(new JoinOperator(comp, columnTables, rhs));
                            iter.remove();
                            continue;
                        }
                    }
                    columnTables = columnReferenceTable(comp.getRight(), processor.getTableBitSets());
                    if (!JoinableBitSet.isEmpty(columnTables)) {
                        long lhs = visitor.getTables(comp.getLeft());
                        if (visitor.wasNullTolerant()) continue;
                        if (!JoinableBitSet.isEmpty(lhs) &&
                                !JoinableBitSet.overlaps(columnTables, lhs) &&
                                joins.getFKJoin().getConditions().contains(comp)) {
                            operators.add(new JoinOperator(comp, columnTables, lhs));
                            iter.remove();
                            continue;
                        }
                    }
                } 
            }
            
            List<JoinOperator> outsideJoins = new ArrayList<>();
            outsideJoins.addAll(operators); // Total set for outer; inner must subtract.
            
            PlanClass leftClass = null;
            Plan leftPlan = null;
            Plan rightPlan = null;
            
            if (joins.getLeft() instanceof TableGroupJoinTree) {
                leftClass = processor.evaluateTable(leftTable, joins.getLeft());
                leftPlan = leftClass.bestPlan(outsideJoins, sortAllowed);
            } else if (joins.getLeft() instanceof JoinNode) {
                if (((JoinNode)joins.getLeft()).getFKJoin() != null) {
                    leftClass = new JoinPlanClass(processor, processor.rootJoinLeftTables());
                    leftPlan = pickRootJoinPlan((JoinNode) joins.getLeft(), sortAllowed);
                } else {
                    JoinEnumerator innerProcessor = new JoinEnumerator (this);
                    leftClass = innerProcessor.run(joins.getLeft(), queryGoal.getWhereConditions());
                    leftPlan = leftClass.bestPlan(Collections.<JoinOperator>emptyList(), sortAllowed);
                }
            }

            if (joins.getRight() instanceof TableGroupJoinTree) {
                 rightPlan = processor.evaluateTable(rightTable, joins.getRight()).
                         bestNestedPlan(leftClass, operators, outsideJoins);
            } else if (joins.getRight() instanceof JoinNode) {
                if (((JoinNode)joins.getRight()).getFKJoin() != null) {
                    rightPlan = pickRootJoinPlan((JoinNode)joins.getRight(), false);
                } else {
                    JoinEnumerator innerProcessor = new JoinEnumerator (this);
                    rightPlan = innerProcessor.run(joins.getRight(), queryGoal.getWhereConditions()).
                            bestPlan(Collections.<JoinOperator>emptyList(), false);
                }
            }

            JoinType joinType = joins.getJoinType();
            CostEstimate costEstimate = leftPlan.costEstimate.nest(rightPlan.costEstimate);
            JoinPlan joinPlan = new JoinPlan(leftPlan, rightPlan,
                                             joinType, JoinNode.Implementation.NESTED_LOOPS,
                                             outsideJoins, costEstimate);
            return joinPlan;
        }

        /** Is this a single column in a known table? */
        protected static long columnReferenceTable(ExpressionNode node, Map<Joinable, Long>tableBitSets) {
            if (node instanceof ColumnExpression) {
                Long bitset = tableBitSets.get(                      
                        ((ColumnExpression)node).getTable()
                        
                        );
                if (bitset != null) {
                    return bitset;
                }
            }
            return JoinableBitSet.empty();
        }

        // Put the chosen plan in place.
        public void installPlan(Plan rootPlan, boolean copy) {
            joinable.getOutput().replaceInput(joinable, 
                                              moveInSemiJoins(rootPlan.install(copy, true)));
            query.setCostEstimate(rootPlan.costEstimate);
        }

        // If any semi-joins to VALUES are left over at the top, they
        // can be put into the Select and then possibly moved earlier.
        protected Joinable moveInSemiJoins(Plan.JoinableWithConditionsToRemove joinedWithConditions) {
            Joinable joined = joinedWithConditions.getJoinable();
            if (queryGoal.getWhereConditions() != null) {

                while (joined instanceof JoinNode) {
                    JoinNode join = (JoinNode)joined;
                    if (join.getJoinType() != JoinType.SEMI) 
                        break;
                    Joinable right = join.getRight();
                    if (!(right instanceof ExpressionsSource))
                        break;
                    // Cf. GroupIndexGoal.semiJoinToInList.
                    ExpressionsSource values = (ExpressionsSource)right;
                    ComparisonCondition ccond = null;
                    boolean found = false;
                    if ((join.getJoinConditions() != null) &&
                        (join.getJoinConditions().size() == 1)) {
                        ConditionExpression joinCondition = join.getJoinConditions().get(0);
                        if (joinCondition instanceof ComparisonCondition) {
                            ccond = (ComparisonCondition)joinCondition;
                            if ((ccond.getOperation() == Comparison.EQ) &&
                                (ccond.getRight() instanceof ColumnExpression)) {
                                ColumnExpression rcol = (ColumnExpression)ccond.getRight();
                                if ((rcol.getTable() == values) &&
                                    (rcol.getPosition() == 0)) {
                                    found = true;
                                }
                            }
                        }
                    }
                    if (!found) break;
                    // Replace semi-join with In Select condition.
                    queryGoal.getWhereConditions()
                        .add(GroupIndexGoal.semiJoinToInList(values, ccond, rulesContext));
                    joined = join.getLeft();
                }
            }
            return joined;
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
            if (queryGoal == null) {
                queryGoal = determineQueryIndexGoal(joinable);
                if (queryGoal.getWhereConditions() != null)
                    originalSubqueryWhereConditions = new ConditionList(queryGoal.getWhereConditions());
            }
            if (joinable instanceof TableGroupJoinTree) {
                TableGroupJoinTree tables = (TableGroupJoinTree)joinable;
                GroupIndexGoal groupGoal = new GroupIndexGoal(queryGoal, tables, planContext);
                // In this block because we were not a JoinNode, query has no joins itself
                List<JoinOperator> queryJoins = Collections.emptyList();
                List<ConditionList> conditionSources = groupGoal.updateContext(subqueryBoundTables, queryJoins, subqueryJoins,
                        subqueryOutsideJoins, subqueryJoins, true, null);
                BaseScan scan = groupGoal.pickBestScan();
                CostEstimate costEstimate = scan.getCostEstimate();
                return new GroupPlan(groupGoal, JoinableBitSet.of(0), scan, costEstimate, conditionSources, true, null);
            }
            if (joinable instanceof JoinNode) {
                if ((originalSubqueryWhereConditions != null) &&
                    (originalSubqueryWhereConditions.size() != queryGoal.getWhereConditions().size())) {
                    // Restore possible join conditions for new enumerator.
                    queryGoal.getWhereConditions().clear();
                    queryGoal.getWhereConditions().addAll(originalSubqueryWhereConditions);
                }
                return new JoinEnumerator(this, subqueryBoundTables, subqueryJoins, subqueryOutsideJoins).
                        run((JoinNode) joinable, queryGoal.getWhereConditions()).
                        bestPlan(Collections.<JoinOperator>emptyList(), enableFKJoins);
            }
            if (joinable instanceof SubquerySource) {
                SubquerySource subquerySource = (SubquerySource) joinable;
                Plan plan = subpicker(subquerySource).subqueryPlan(subqueryBoundTables, subqueryJoins, subqueryOutsideJoins);
                return new SubqueryPlan(subquerySource, subpicker(subquerySource), JoinableBitSet.of(0), plan, plan.costEstimate);
            }
            if (joinable instanceof CreateAs) {
                return null;
            }
            if (joinable instanceof ExpressionsSource) {
                CostEstimator costEstimator = this.getCostEstimator();
                return new ValuesPlan((ExpressionsSource)joinable, costEstimator.costValues((ExpressionsSource)joinable, false));
            }
            throw new AkibanInternalException("Unknown join element: " + joinable);
        }

    }

    public static abstract class Plan implements Comparable<Plan> {
        CostEstimate costEstimate;

        protected Plan(CostEstimate costEstimate) {
            this.costEstimate = costEstimate;
        }

        public int compareTo(Plan other) {
            return costEstimate.compareTo(other.costEstimate);
        }

        public abstract JoinableWithConditionsToRemove install(boolean copy, boolean sortAllowed);

        public void addDistinct() {
            throw new UnsupportedOperationException();
        }

        public boolean semiJoinEquivalent() {
            return false;
        }

        public boolean containsColumn(ColumnExpression column) {
            return false;
        }

        public Collection<? extends ColumnSource> columnTables() {
            return Collections.emptyList();
        }

        public double joinSelectivity() {
            return 1.0;
        }

        public void redoCostWithLimit(long limit) {
        }

        public abstract Collection<? extends ConditionExpression> getConditions();

        public static class JoinableWithConditionsToRemove {
            public Joinable joinable;
            public List<? extends ConditionExpression> conditions;

            public JoinableWithConditionsToRemove(Joinable joinable, List<? extends ConditionExpression> conditions) {
                this.joinable = joinable;
                this.conditions = conditions;
            }

            public Joinable getJoinable() {
                return joinable;
            }

            public List<? extends ConditionExpression> getConditions() {
                return conditions;
            }
        }
    }

    static abstract class PlanClass {
        JoinEnumerator enumerator;        
        long bitset;

        protected PlanClass(JoinEnumerator enumerator, long bitset) {
            this.enumerator = enumerator;
            this.bitset = bitset;
        }

        public abstract Plan bestPlan(Collection<JoinOperator> outsideJoins, boolean sortAllowed);

        public abstract Plan bestNestedPlan(PlanClass outerPlan,
                                            Collection<JoinOperator> joins, Collection<JoinOperator> outsideJoins);

        public abstract Plan bestPlan(Collection<JoinOperator> condJoins, Collection<JoinOperator> outsideJoins, boolean sortAllowed);
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
        public JoinableWithConditionsToRemove install(boolean copy, boolean sortAllowed) {
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
            return groupGoal.orderedForDistinct((Project) distinct.getInput(), (IndexScan) scan);
        }

        @Override
        public boolean semiJoinEquivalent() {
            return groupGoal.semiJoinEquivalent(scan);
        }

        @Override
        public boolean containsColumn(ColumnExpression column) {
            ColumnSource table = column.getTable();
            if (!(table instanceof TableSource)) return false;
            return groupGoal.getTables().containsTable((TableSource) table);
        }

        @Override
        public Collection<? extends ColumnSource> columnTables() {
            return groupGoal.getTableColumnSources();
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

        @Override
        public Collection<? extends ConditionExpression> getConditions() {
            return scan.getConditions();
        }
    }

    static class GroupPlanClass extends PlanClass {
        GroupIndexGoal groupGoal;
        Collection<GroupPlan> bestPlans = new ArrayList<>();

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
        public String toString() {
            return groupGoal.toString();
        }

        @Override
        public Plan bestPlan(Collection<JoinOperator> outsideJoins, boolean sortAllowed) {
            return bestPlan(JoinableBitSet.empty(), Collections.<JoinOperator>emptyList(), outsideJoins, sortAllowed);
        }

        @Override
        public Plan bestNestedPlan(PlanClass outerPlan, Collection<JoinOperator> joins, Collection<JoinOperator> outsideJoins) {
            return bestPlan(outerPlan.bitset, joins, outsideJoins, false);
        }

        @Override
        public Plan bestPlan(Collection<JoinOperator> condJoins, Collection<JoinOperator> outsideJoins, boolean sortAllowed) {
            return bestPlan(JoinableBitSet.empty(), condJoins, outsideJoins, sortAllowed);
        }

        protected ConditionList getExtraConditions() {
            return null;
        }

        protected GroupPlan bestPlan(long outerTables,
                                     Collection<JoinOperator> joins, Collection<JoinOperator> outsideJoins,
                                     boolean sortAllowed) {
            return bestPlan(outerTables, joins, joins, outsideJoins, sortAllowed);
        }

        protected GroupPlan bestPlan(long outerTables, Collection<JoinOperator> queryJoins,
                                     Collection<JoinOperator> joins,
                                     Collection<JoinOperator> outsideJoins,
                                     boolean sortAllowed) {
            for (GroupPlan groupPlan : bestPlans) {
                if (groupPlan.outerTables == outerTables && groupPlan.sortAllowed == sortAllowed) {
                    return groupPlan;
                }
            }
            Collection<JoinOperator> requiredJoins = joins;
            if (JoinableBitSet.isEmpty(outerTables)) {
                // this is an outer plan
                requiredJoins = new ArrayList<>();
                joinsForOuterPlan(joins, bitset, requiredJoins);
            }
            List<ConditionList> conditionSources = groupGoal.updateContext(
                    enumerator.boundTables(outerTables), queryJoins, joins, outsideJoins, requiredJoins,
                    sortAllowed, getExtraConditions());
            BaseScan scan = groupGoal.pickBestScan();
            CostEstimate costEstimate = scan.getCostEstimate();
            GroupPlan groupPlan = new GroupPlan(groupGoal, outerTables, scan, costEstimate, conditionSources, sortAllowed, getExtraConditions());
            bestPlans.add(groupPlan);
            return groupPlan;
        }

        private void joinsForOuterPlan(Collection<JoinOperator> condJoins, long bitset,
                                       Collection<JoinOperator> joinsForLeft) {
            for (JoinOperator join : condJoins) {
                if (JoinableBitSet.isSubset(join.getTables(), bitset)) {
                    joinsForLeft.add(join);
                }
            }
        }
    }

    static class GroupWithInPlanClass extends GroupPlanClass {
        ConditionList inConditions;

        public GroupWithInPlanClass(GroupPlanClass left,
                                    ValuesPlanClass right,
                                    Collection<JoinOperator> joins) {
            super(left);
            InListCondition incond = left.groupGoal.semiJoinToInList(right.plan.values, joins);
            if (incond != null) {
                inConditions = new ConditionList(1);
                inConditions.add(incond);
            }
        }

        public GroupWithInPlanClass(GroupWithInPlanClass left,
                                    ValuesPlanClass right,
                                    Collection<JoinOperator> joins) {
            super(left);
            InListCondition incond = left.groupGoal.semiJoinToInList(right.plan.values, joins);
            if (incond != null) {
                inConditions = new ConditionList(left.inConditions);
                inConditions.add(incond);
            }
        }

        @Override
        protected ConditionList getExtraConditions() {
            return inConditions;
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
        public JoinableWithConditionsToRemove install(boolean copy, boolean sortAllowed) {
            picker.installPlan(this.rootPlan, copy);
            return new JoinableWithConditionsToRemove(subquery, new ConditionList());
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

        @Override
        public Collection<? extends ConditionExpression> getConditions() {
            return null;
        }
    }

    static class SubqueryPlanClass extends PlanClass {
        SubquerySource subquery;
        Picker picker;
        Collection<SubqueryPlan> bestPlans = new ArrayList<>();

        public SubqueryPlanClass(JoinEnumerator enumerator, long bitset, 
                                 SubquerySource subquery, Picker picker) {
            super(enumerator, bitset);
            this.subquery = subquery;
            this.picker = picker;
        }

        @Override
        public String toString() {
            return subquery.toString();
        }

        @Override
        public Plan bestPlan(Collection<JoinOperator> outsideJoins, boolean sortAllowed) {
            return bestPlan(JoinableBitSet.empty(), Collections.<JoinOperator>emptyList(), outsideJoins);
        }

        @Override
        public Plan bestNestedPlan(PlanClass outerPlan, Collection<JoinOperator> joins, Collection<JoinOperator> outsideJoins) {
            return bestPlan(outerPlan.bitset, joins, outsideJoins);
        }

        @Override
        public Plan bestPlan(Collection<JoinOperator> condJoins, Collection<JoinOperator> outsideJoins, boolean sortAllowed) {
            return bestPlan(JoinableBitSet.empty(), condJoins, outsideJoins);
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
        public JoinableWithConditionsToRemove install(boolean copy, boolean sortAllowed) {
            return new JoinableWithConditionsToRemove(values, new ConditionList());
        }

        @Override
        public void addDistinct() {
            values.setDistinctState(ExpressionsSource.DistinctState.NEED_DISTINCT);
        }

        @Override
        public Collection<? extends ConditionExpression> getConditions() {
            return null;
        }
    }

    static class ValuesPlanClass extends PlanClass {
        ValuesPlan plan, nestedPlan;

        public ValuesPlanClass(JoinEnumerator enumerator, long bitset, 
                               ExpressionsSource values, Picker picker) {
            super(enumerator, bitset);
            CostEstimator costEstimator = picker.getCostEstimator();
            this.plan = new ValuesPlan(values, costEstimator.costValues(values, false));
            // Nested also needs to check the join condition with Select.
            this.nestedPlan = new ValuesPlan(values, costEstimator.costValues(values, true));
        }

        @Override
        public String toString() {
            return plan.toString();
        }

        @Override
        public Plan bestPlan(Collection<JoinOperator> outsideJoins, boolean sortAllowed) {
            return sortAllowed ? plan : nestedPlan;
        }

        @Override
        public Plan bestNestedPlan(PlanClass outerPlan, Collection<JoinOperator> joins, Collection<JoinOperator> outsideJoins) {
            return nestedPlan;
        }

        @Override
        public Plan bestPlan(Collection<JoinOperator> condJoins, Collection<JoinOperator> outsideJoins, boolean sortAllowed) {
            return sortAllowed ? plan : nestedPlan;
        }
    }

    static class CreateAsPlanClass extends PlanClass {
        CreateAsPlan plan, nestedPlan;

        public CreateAsPlanClass(JoinEnumerator enumerator, long bitset,
                               CreateAs values, Picker picker) {
            super(enumerator, bitset);
            CostEstimator costEstimator = picker.getCostEstimator();
            this.plan = new CreateAsPlan(values, costEstimator.costBoundRow());
        }

        @Override
        public String toString() {
            return plan.toString();
        }

        @Override
        public Plan bestPlan(Collection<JoinOperator> outsideJoins, boolean sortAllowed) {
            return sortAllowed ? plan : nestedPlan;
        }

        @Override
        public Plan bestNestedPlan(PlanClass outerPlan, Collection<JoinOperator> joins, Collection<JoinOperator> outsideJoins) {
            return nestedPlan;
        }

        @Override
        public Plan bestPlan(Collection<JoinOperator> condJoins, Collection<JoinOperator> outsideJoins, boolean sortAllowed) {
            return sortAllowed ? plan : nestedPlan;
        }
    }

    static class CreateAsPlan extends Plan {
        CreateAs values;

        public CreateAsPlan(CreateAs values, CostEstimate costEstimate) {
            super(costEstimate);
            this.values = values;
        }

        @Override
        public String toString() {
            return values.getName();
        }

        @Override
        public JoinableWithConditionsToRemove install(boolean copy, boolean sortAllowed) {
            return new JoinableWithConditionsToRemove(values, null);
        }

        @Override
        public Collection<? extends ConditionExpression> getConditions() {
            return null;
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
        public JoinableWithConditionsToRemove install(boolean copy, boolean sortAllowed) {
            if (needDistinct)
                left.addDistinct();
            JoinableWithConditionsToRemove leftJoinable = left.install(copy, sortAllowed);
            JoinableWithConditionsToRemove rightJoinable = right.install(copy, false);
            ConditionList conditionsToRemove = new ConditionList();
            if (leftJoinable.getConditions() != null) {
                conditionsToRemove.addAll(leftJoinable.getConditions());
            }
            if (rightJoinable.getConditions() != null) {
                conditionsToRemove.addAll(rightJoinable.getConditions());
            }
            ConditionList joinConditions = mergeJoinConditions(joins);
            if (joinConditions != null) {
                joinConditions.removeAll(conditionsToRemove);
                conditionsToRemove.addAll(joinConditions);
            }
            JoinNode join = new JoinNode(leftJoinable.getJoinable(), rightJoinable.getJoinable(), joinType);
            join.setJoinConditions(joinConditions);
            join.setImplementation(joinImplementation);
            if (joinType == JoinType.SEMI)
                InConditionReverser.cleanUpSemiJoin(join, rightJoinable.getJoinable());
            return new JoinableWithConditionsToRemove(join, conditionsToRemove);
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

        @Override
        public Collection<? extends ConditionExpression> getConditions() {
            return null;
        }
    }

    static class HashJoinPlan extends JoinPlan {
        Plan loader;
        BaseHashTable hashTable;
        List<ExpressionNode> hashColumns, matchColumns;
        List<TKeyComparable> tKeyComparables ;
        List<AkCollator> collators;
        
        public HashJoinPlan(Plan loader, Plan input, Plan check,
                            JoinType joinType, JoinNode.Implementation joinImplementation,
                            Collection<JoinOperator> joins, CostEstimate costEstimate,
                            BaseHashTable hashTable, List<ExpressionNode> hashColumns, List<ExpressionNode> matchColumns,
                            List<TKeyComparable> tKeyComparables, List<AkCollator> collators
        ) {
            super(input, check, joinType, joinImplementation, joins, costEstimate);
            this.loader = loader;
            this.hashTable = hashTable;
            this.hashColumns = hashColumns;
            this.matchColumns = matchColumns;
            this.tKeyComparables = tKeyComparables;
            this.collators = collators;
        }

        @Override
        public JoinableWithConditionsToRemove install(boolean copy, boolean sortAllowed) {
            if (needDistinct)
                left.addDistinct();
            JoinableWithConditionsToRemove loaderJoinable = loader.install(true, false);
            JoinableWithConditionsToRemove inputJoinable = left.install(copy, sortAllowed);
            JoinableWithConditionsToRemove checkJoinable = right.install(copy, false);
            ConditionList joinConditions = mergeJoinConditions(joins);
            if (loaderJoinable.getConditions() != null) {
                joinConditions.removeAll(loaderJoinable.getConditions());
            }
            if (inputJoinable.getConditions() != null) {
                joinConditions.removeAll(inputJoinable.getConditions());
            }
            if (checkJoinable.getConditions() != null) {
                joinConditions.removeAll(checkJoinable.getConditions());
            }
            // the output is different in the planString if joinConditions is null vs. empty
            // so make it null here to make the tests happier
            if (joinConditions == null || joinConditions.isEmpty()) {
                joinConditions = null;
            }
            HashJoinNode join = new HashJoinNode(loaderJoinable.getJoinable(), inputJoinable.getJoinable(), checkJoinable.getJoinable(),
                                                 joinType, hashTable, hashColumns, matchColumns, tKeyComparables, collators);
            join.setJoinConditions(joinConditions);
            join.setImplementation(joinImplementation);
            if (joinType == JoinType.SEMI)
                InConditionReverser.cleanUpSemiJoin(join, checkJoinable.getJoinable());
            return new JoinableWithConditionsToRemove(join, new ConditionList());
        }
    }

    static class JoinPlanClass extends PlanClass {
        Plan bestPlan;
        private Plan bestNestedPlan;
        GroupWithInPlanClass asGroupWithIn; // If semi-joined to one or more VALUES.

        public JoinPlanClass(JoinEnumerator enumerator, long bitset) {
            super(enumerator, bitset);
        }

        @Override
        public String toString() {
            return bestPlan.toString();
        }

        @Override
        public Plan bestPlan(Collection<JoinOperator> outsideJoins, boolean sortAllowed) {
            return sortAllowed ? bestPlan : bestNestedPlan;
        }

        @Override
        public Plan bestNestedPlan(PlanClass outerPlan, Collection<JoinOperator> joins, Collection<JoinOperator> outsideJoins) {
            return bestNestedPlan;
        }

        @Override
        public Plan bestPlan(Collection<JoinOperator> condJoins, Collection<JoinOperator> outsideJoins, boolean sortAllowed) {
            return sortAllowed ? bestPlan : bestNestedPlan;
        }

        public void considerNested(Plan plan) {
            if (bestNestedPlan == null) {
                logger.debug("Selecting (nested) {}, {}", plan, plan.costEstimate);
                bestNestedPlan = plan;
            }
            else if (bestNestedPlan.compareTo(plan) > 0) {
                logger.debug("Preferring (nested) {}, {}", plan, plan.costEstimate);
                bestNestedPlan = plan;
            }
            else {
                logger.debug("Rejecting (nested) {}, {}", plan, plan.costEstimate);
            }
        }

        public void consider(Plan plan) {
            if (bestPlan == null) {
                logger.debug("Selecting {}, {}", plan, plan.costEstimate);
                bestPlan = plan;
            }
            else if (bestPlan.compareTo(plan) > 0) {
                logger.debug("Preferring {}, {}", plan, plan.costEstimate);
                bestPlan = plan;
            }
            else {
                logger.debug("Rejecting {}, {}", plan, plan.costEstimate);
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
                                                              (TableGroupJoinTree)joinable, picker.getPlanContext());
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
            if (joinable instanceof CreateAs){
                return new CreateAsPlanClass(this, s, (CreateAs)joinable, picker);
            }
            throw new AkibanInternalException("Unknown join element: " + joinable);
        }

        @Override
        public PlanClass evaluateJoin(long leftBitset, PlanClass left,
                                      long rightBitset, PlanClass right,
                                      long bitset, PlanClass existing,
                                      JoinType joinType, Collection<JoinOperator> joins, Collection<JoinOperator> outsideJoins) {
            JoinPlanClass planClass = (JoinPlanClass) existing;
            if (planClass == null)
                planClass = new JoinPlanClass(this, bitset);
            if ((joinType.isSemi() && (right instanceof ValuesPlanClass))) {
                // Semi-join a VALUES on the inside by turning it into
                // predicate, which can be better optimized.
                assert (planClass.asGroupWithIn == null);
                GroupWithInPlanClass asGroupWithIn = null;
                if (left instanceof GroupPlanClass) {
                    asGroupWithIn = new GroupWithInPlanClass((GroupPlanClass) left,
                            (ValuesPlanClass) right,
                            joins);
                } else if (left instanceof JoinPlanClass) {
                    JoinPlanClass leftJoinPlanClass = (JoinPlanClass) left;
                    if (leftJoinPlanClass.asGroupWithIn != null) {
                        asGroupWithIn = new GroupWithInPlanClass(leftJoinPlanClass.asGroupWithIn,
                                (ValuesPlanClass) right,
                                joins);
                    }
                }
                if ((asGroupWithIn != null) &&
                        (asGroupWithIn.getExtraConditions() != null)) {
                    Plan withInPlan = asGroupWithIn.bestPlan(outsideJoins, true);
                    if (withInPlan != null) {
                        planClass.asGroupWithIn = asGroupWithIn;
                        planClass.consider(withInPlan);
                    }
                    withInPlan = asGroupWithIn.bestPlan(outsideJoins, false);
                    if (withInPlan != null) {
                        planClass.asGroupWithIn = asGroupWithIn;
                        planClass.considerNested(withInPlan);
                    }
                    return planClass;
                }
            }
            // TODO Could potentially do a check if bitset == overall bitset
            considerJoinPlan(left, right, joinType, joins, outsideJoins, planClass, true);
            considerJoinPlan(left, right, joinType, joins, outsideJoins, planClass, false);
            return planClass;
        }

        public void considerJoinPlan(PlanClass left, PlanClass right, JoinType joinType,
                                     Collection<JoinOperator> joins, Collection<JoinOperator> outsideJoins,
                                     JoinPlanClass planClass, boolean sortAllowed) {

            joins = duplicateJoins(joins);
            Collection<JoinOperator> condJoins = joins; // Joins with conditions for indexing.
            if (subqueryJoins != null) {
                // "Push down" joins into the subquery. Since these
                // are joins to the dervived table, they still need to
                // be recognized to match an indexable column.
                condJoins = new ArrayList<>(joins);
                condJoins.addAll(subqueryJoins);
            }
            if (subqueryOutsideJoins != null) {
                outsideJoins.addAll(subqueryOutsideJoins);
            }
            outsideJoins.addAll(joins); // Total set for outer; inner must subtract.

            Plan leftPlan;
            if (joinType.isRightLinear() || joinType.isSemi()) {
                leftPlan = left.bestPlan(condJoins, outsideJoins, sortAllowed);
            } else {
                leftPlan = left.bestPlan(Collections.<JoinOperator>emptyList(), outsideJoins, sortAllowed);
            }
            Plan rightPlan;
            if (joinType.isLeftLinear()) {
                rightPlan = right.bestNestedPlan(left, condJoins, outsideJoins);
            } else {
                rightPlan = right.bestNestedPlan(left, Collections.<JoinOperator>emptyList(), outsideJoins);
            }
            CostEstimate costEstimate = leftPlan.costEstimate.nest(rightPlan.costEstimate);
            JoinPlan joinPlan = new JoinPlan(leftPlan, rightPlan,
                    joinType, JoinNode.Implementation.NESTED_LOOPS,
                    joins, costEstimate);

            if (isFreeOfJoinCondition(leftPlan, rightPlan.getConditions())) {
                List<JoinOperator> joinOperators = duplicateJoins(joins);
                Plan loaderPlan = right.bestPlan(condJoins, outsideJoins, false);
                JoinPlan hashPlan = buildHashTableJoin(loaderPlan, joinPlan, joinOperators);
                if (hashPlan != null) {
                    if (sortAllowed) {
                        planClass.consider(hashPlan);
                    } else {
                        planClass.considerNested(hashPlan);
                    }
                }
            }
            if (joinType.isSemi() || (joinType.isInner() && rightPlan.semiJoinEquivalent())) {
                Collection<JoinOperator> semiJoins = duplicateJoins(joins);
                Plan loaderPlan = right.bestPlan(condJoins, outsideJoins, false);
                cleanJoinConditions(semiJoins, loaderPlan, leftPlan);
                // buildBloomFilterSemiJoin modifies the joinPlan.
                JoinPlan hashPlan = buildBloomFilterSemiJoin(loaderPlan, joinPlan, semiJoins);
                if (hashPlan != null) {
                    if (sortAllowed) {
                        planClass.consider(hashPlan);
                    } else {
                        planClass.considerNested(hashPlan);
                    }
                }
            }
            cleanJoinConditions(joins, leftPlan, rightPlan);
            if (sortAllowed) {
                planClass.consider(joinPlan);
            } else {
                planClass.considerNested(joinPlan);
            }
        }

        private void joinsForOuterPlan(Collection<JoinOperator> condJoins, PlanClass left,
                                       Collection<JoinOperator> joinsForLeft) {
            for (JoinOperator join : condJoins) {
                if (JoinableBitSet.isSubset(join.getTables(), left.bitset)) {
                    joinsForLeft.add(join);
                }
            }
        }

        private void cleanJoinConditions(Collection<JoinOperator> joins, Plan leftPlan, Plan rightPlan) {
            Collection<? extends ConditionExpression> leftConditions = leftPlan.getConditions();
            Collection<? extends ConditionExpression> rightConditions = rightPlan.getConditions();
            for (JoinOperator join : joins) {
                if (join.getJoinConditions() != null) {
                    if (leftConditions != null) {
                        join.getJoinConditions().removeAll(leftConditions);
                    }
                    if (rightConditions != null) {
                        join.getJoinConditions().removeAll(rightConditions);
                    }
                }
            }
        }

        private List<JoinOperator> duplicateJoins(Collection<JoinOperator> joins) {
            List<JoinOperator> retJoins = new ArrayList<>();
            for (JoinOperator join : joins) {
                retJoins.add(new JoinOperator(join));
            }
            return retJoins;
        }

        /** Get the tables that correspond to the given bitset, plus
         * any that are bound outside the subquery, either
         * syntactically or via joins to it.
         */
        public Set<ColumnSource> boundTables(long tables) {
            if (JoinableBitSet.isEmpty(tables) &&
                (subqueryBoundTables == null))
                return picker.queryGoal.getQuery().getOuterTables();
            Set<ColumnSource> boundTables = new HashSet<>();
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

        public JoinPlan buildBloomFilterSemiJoin(Plan loaderPlan, JoinPlan joinPlan,
                                                 Collection<JoinOperator> joinOperators) {
            Plan inputPlan = joinPlan.left;
            Plan checkPlan = joinPlan.right;
            Collection<JoinOperator> joins = joinOperators;
            if (checkPlan.costEstimate.getRowCount() > 1)
                return null;    // Join not selective.
            double maxSelectivity;
            String prop = picker.rulesContext.getProperty("bloomFilterMaxSelectivity");
            if (prop != null)
                maxSelectivity = Double.valueOf(prop);
            else
                maxSelectivity = BLOOM_FILTER_MAX_SELECTIVITY_DEFAULT;
            if (maxSelectivity <= 0.0) return null; // Feature turned off.
            HashTableColumns hashTableColumns = hashTableColumns(joins, inputPlan, checkPlan);
            if (hashTableColumns == null)
                return null;
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
                // Doing so would also make optimizing take longer
                limit = Math.round(limit / selectivity);
                joinPlan.redoCostWithLimit(limit);
            }
            BloomFilter bloomFilter = new BloomFilter(loaderPlan.costEstimate.getRowCount(), 1);
            CostEstimate costEstimate = picker.getCostEstimator()
                .costBloomFilter(loaderPlan.costEstimate, inputPlan.costEstimate, checkPlan.costEstimate, selectivity);
            return new HashJoinPlan(loaderPlan, inputPlan, checkPlan,
                                    JoinType.SEMI, JoinNode.Implementation.BLOOM_FILTER,
                                    joins, costEstimate, bloomFilter, hashTableColumns.hashColumns, hashTableColumns.matchColumns, hashTableColumns.tKeyComparables, hashTableColumns.collators);
        }


        private List<JoinOperator> cleanJoinOperators(Plan inputPlan, List<JoinOperator> joinOperators) {
            if (inputPlan instanceof GroupPlan && ((GroupPlan) inputPlan).scan instanceof SingleIndexScan && ((GroupPlan)inputPlan).scan.getConditions() != null) {
                SingleIndexScan scan = (SingleIndexScan) ((GroupPlan) inputPlan).scan;
                for (ConditionExpression indexCond : scan.getConditions()) {
                    if (indexCond instanceof ComparisonCondition) {
                        for (int i = 0; i < joinOperators.size(); i++) {
                            removeIfExists(indexCond, joinOperators.get(i).getJoinConditions());
                            if (joinOperators.get(i).getJoin() != null)
                                removeIfExists(indexCond, joinOperators.get(i).getJoin().getJoinConditions());


                        }
                    }
                }
            }
             return joinOperators;
        }

        private void removeIfExists(ConditionExpression condToRemove, ConditionList conditionList){
            if (conditionList == null || conditionList.isEmpty())
                return;
            for (int j = 0; j < conditionList.size(); j++) {
                if (conditionList.get(j) instanceof ComparisonCondition) {
                    ComparisonCondition compCondition = (ComparisonCondition) conditionList.get(j);
                    if (compCondition.getRight() instanceof ColumnExpression && compCondition.getLeft() instanceof ColumnExpression)
                        continue;
                    if (conditionList.get(j).equals(condToRemove))
                        conditionList.remove(j--);
                }
            }
        }


        int MAX_COL_COUNT = 5000;
        int DEFAULT_COLUMN_COUNT = 5;

        public JoinPlan buildHashTableJoin(Plan loaderPlan, JoinPlan joinPlan,
                                           List<JoinOperator> joinOperators) {
            Plan outerPlan = joinPlan.left;
            joinOperators = cleanJoinOperators(outerPlan, joinOperators);
            Plan innerPlan = joinPlan.right;
            joinOperators = cleanJoinOperators(innerPlan, joinOperators);

            Collection<JoinOperator> joins = joinOperators;
            HashTableColumns hashTableColumns = hashTableColumns(joins, outerPlan, innerPlan);
            if (hashTableColumns == null)
                return null;
            String prop = picker.rulesContext.getProperty("hashTableMaxRowCount");
            int maxColumnCount;
            if (prop != null){
                maxColumnCount = Integer.parseInt(prop);
                if (maxColumnCount == 0) return null; }
            else
                maxColumnCount = MAX_COL_COUNT;
            if (loaderPlan.costEstimate.getRowCount() * hashTableColumns.hashColumns.size() > maxColumnCount)
                return null;
            int outerColumnCount = DEFAULT_COLUMN_COUNT;
            int innerColumnCount = DEFAULT_COLUMN_COUNT;
            HashTable hashTable = new HashTable(loaderPlan.costEstimate.getRowCount());
            for (ExpressionNode expression : hashTableColumns.matchColumns) {
                if (expression instanceof ColumnExpression) {
                    ColumnSource columnSource = ((ColumnExpression)expression).getTable();
                    if (columnSource instanceof TableSource) {
                        TableNode table = ((TableSource)columnSource).getTable();
                        outerColumnCount = table.getTable().getColumnsIncludingInternal().size();
                        
                        break;
                    }
                }
            }
            for (ExpressionNode expression : hashTableColumns.hashColumns) {
                if (expression instanceof ColumnExpression) {
                    ColumnSource columnSource = ((ColumnExpression)expression).getTable();
                    if (columnSource instanceof TableSource) {
                        TableNode table = ((TableSource)columnSource).getTable();
                        innerColumnCount = table.getTable().getColumnsIncludingInternal().size();
                        break;
                    }
                }
            }
            CostEstimate costEstimate = picker.getCostEstimator()
                .costHashLookup(innerPlan.costEstimate, hashTableColumns.hashColumns.size(), innerColumnCount);
            HashLookupPlan lookupPlan = new HashLookupPlan(costEstimate, hashTable, hashTableColumns);
            costEstimate = picker.getCostEstimator()
                    .costHashJoin(loaderPlan.costEstimate, outerPlan.costEstimate, costEstimate, hashTableColumns.hashColumns.size(), outerColumnCount, innerColumnCount);
            return new HashJoinPlan(loaderPlan, outerPlan, lookupPlan,
                    joinPlan.joinType, JoinNode.Implementation.HASH_TABLE,
                    joins, costEstimate, hashTable, hashTableColumns.hashColumns, hashTableColumns.matchColumns, hashTableColumns.tKeyComparables, hashTableColumns.collators);
        }

        static class HashLookupPlan extends Plan {
            HashTable hashTable;
            HashTableColumns hashTableColumns;

            public HashLookupPlan(CostEstimate costEstimate, HashTable hashTable, HashTableColumns hashTableColumns) {
                super(costEstimate);
                this.hashTable = hashTable;
                this.hashTableColumns = hashTableColumns;
            }

            @Override
            public String toString() {
                return hashTableColumns.conditions.toString();
            }

            @Override
            public JoinableWithConditionsToRemove install(boolean copy, boolean sortAllowed) {
                HashTableLookup lookup = new HashTableLookup(hashTable,
                                                             hashTableColumns.matchColumns,
                                                             hashTableColumns.conditions,
                                                             hashTableColumns.tables);
                return new JoinableWithConditionsToRemove(lookup,
                                                          hashTableColumns.conditions);
            }

            @Override
            public Collection<? extends ConditionExpression> getConditions() {
                return hashTableColumns.conditions;
            }
        }

        static class HashTableColumns {
            List<ConditionExpression> conditions = new ArrayList<>();
            List<ExpressionNode> matchColumns = new ArrayList<>();
            List<ExpressionNode> hashColumns = new ArrayList<>();
            Collection<? extends ColumnSource> tables;
            List<TKeyComparable> tKeyComparables = new ArrayList<>();
            List<AkCollator> collators = new ArrayList<>();
        }

        /** Find some equality conditions between tables on the two sides of the join.
         * These can be used to load a hash table / Bloom filter.
         */
        public HashTableColumns hashTableColumns(Collection<JoinOperator> joins, Plan inputPlan, Plan checkPlan) {
            HashTableColumns result = new HashTableColumns();

            for (JoinOperator join : joins) {
                if (join.getJoinConditions() != null) {
                    for (ConditionExpression cond : join.getJoinConditions()) {
                        if (!(cond instanceof ComparisonCondition)) continue;
                        ComparisonCondition ccond = (ComparisonCondition) cond;
                        if (ccond.getOperation() != Comparison.EQ) continue;
                        ExpressionNode left = ccond.getLeft();
                        ExpressionNode right = ccond.getRight();
                        if (!((left instanceof ColumnExpression) &&
                              (right instanceof ColumnExpression)))
                            continue;
                        ColumnExpression matchColumn, hashColumn;
                        if (inputPlan.containsColumn((ColumnExpression) left) &&
                            checkPlan.containsColumn((ColumnExpression) right)) {
                            matchColumn = (ColumnExpression) left;
                            hashColumn = (ColumnExpression) right;
                        } 
                        else if (inputPlan.containsColumn((ColumnExpression) right) &&
                                 checkPlan.containsColumn((ColumnExpression) left)) {
                            matchColumn = (ColumnExpression) right;
                            hashColumn = (ColumnExpression) left;
                        }
                        else continue;
                        result.conditions.add(cond);
                        result.matchColumns.add(matchColumn);
                        result.hashColumns.add(hashColumn);
                        result.tKeyComparables.add(ccond.getKeyComparable());
                        AkCollator collator = null;
                        if (left.getType().hasAttributes(StringAttribute.class) &&
                            right.getType().hasAttributes(StringAttribute.class)) {
                            CharacterTypeAttributes leftAttributes = StringAttribute.characterTypeAttributes(left.getType());
                            CharacterTypeAttributes rightAttributes = StringAttribute.characterTypeAttributes(right.getType());
                            collator = TString.mergeAkCollators(leftAttributes, rightAttributes);

                        }
                        result.collators.add(collator);
                    }
                }
            }
            if (result.conditions.isEmpty())
                return null;
            result.tables = checkPlan.columnTables();
            return result;
        }
    }

    // Find top-level joins and note what query they come from; 
    // Top-level queries and those used in expressions are returned directly.
    // Derived tables are deferred, since they need to be planned in
    // the context of various join orders to allow for join predicated
    // to be pushed "inside." So they are stored in a Map accessible
    // to other Pickers.
    static class JoinsFinder extends SubqueryBoundTablesTracker {
        List<Picker> result;
        Map<SubquerySource,Picker> subpickers;

        public JoinsFinder(PlanContext planContext) {
            super(planContext);
        }

        public List<Picker> find() {
            result = new ArrayList<>();
            subpickers = new HashMap<>();
            run();
            result.removeAll(subpickers.values()); // Do these in context.
            return result;
        }

        @Override
        public boolean visit(PlanNode n) {
            super.visit(n);
            if ((n instanceof Joinable) && !(n instanceof TableSource)) {
                Joinable j = (Joinable)n;
                while (j.getOutput() instanceof Joinable)
                    j = (Joinable)j.getOutput();
                BaseQuery query = currentQuery();
                SubquerySource subquerySource = null;
                if (query.getOutput() instanceof SubquerySource)
                    subquerySource = (SubquerySource)query.getOutput();
                for (Picker picker : result) {
                    if (picker.joinable == j)
                        // Already have another set of joins to same root join.
                        return true;
                }
                Picker picker = new Picker(j, query, planContext, subpickers);
                result.add(picker);
                if (subquerySource != null)
                    subpickers.put(subquerySource, picker);
            }
            return true;
        }
    }

    /** Does this scan only handle conditions independent of the outer scan? */
    public static boolean isFreeOfJoinCondition(Plan outerPlan,
                                                Collection<? extends ConditionExpression> conditions) {
        if ((conditions == null) || conditions.isEmpty())
            return true;
        JoinConditionChecker checker = new JoinConditionChecker(outerPlan);
        for (ConditionExpression cond : conditions) {
            if (!cond.accept(checker)) break;
        }
        return !checker.found;
    }

    static class JoinConditionChecker implements ExpressionVisitor {
        final Plan plan;
        boolean found;

        public JoinConditionChecker(Plan plan) {
            this.plan = plan;
        }

        public boolean visitEnter(ExpressionNode n) {
            return visit(n);
        }

        public boolean visitLeave(ExpressionNode n) {
            return !found;
        }

        public boolean visit(ExpressionNode n) {
            if (n instanceof ColumnExpression) {
                if (plan.containsColumn((ColumnExpression)n)) {
                    found = true;
                }
            }
            return !found;
        }
    }

}
