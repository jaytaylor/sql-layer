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

import com.foundationdb.server.error.CorruptedPlanException;
import com.foundationdb.server.types.common.funcs.BoolLogic;
import com.foundationdb.server.types.texpressions.TValidatedScalar;
import com.foundationdb.sql.optimizer.plan.*;
import com.foundationdb.sql.optimizer.plan.JoinNode.JoinType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Convert nested loop join into map.
 * This rule only does the immediate conversion to a Map. The map
 * still needs to be folded after conditions have moved down and so on.
 */
public class NestedLoopMapper extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(NestedLoopMapper.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    static class NestedLoopsJoinsFinder implements PlanVisitor, ExpressionVisitor {
        List<JoinNode> result = new ArrayList<>();

        public List<JoinNode> find(PlanNode root) {
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
            if (n instanceof JoinNode) {
                JoinNode j = (JoinNode)n;
                switch (j.getImplementation()) {
                case NESTED_LOOPS:
                case BLOOM_FILTER:
                case HASH_TABLE:
                    result.add(j);
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
    public void apply(PlanContext planContext) {
        BaseQuery query = (BaseQuery)planContext.getPlan();
        List<JoinNode> joins = new NestedLoopsJoinsFinder().find(query);
        for (JoinNode join : joins) {
            PlanNode outer = join.getLeft();
            PlanNode inner = join.getRight();
            if (join.hasJoinConditions()) {
                outer = moveConditionsToOuterNode(outer, join.getJoinConditions(),
                                                  getQuery(join).getOuterTables(),
                                                  JoinType.ANTI.equals(join.getJoinType()));
                if (join.hasJoinConditions())
                    inner = new Select(inner, join.getJoinConditions());
            }
            PlanNode map;
            switch (join.getImplementation()) {
            case NESTED_LOOPS:
                map = new MapJoin(join.getJoinType(), outer, inner);
                break;
            case BLOOM_FILTER:
                {
                    HashJoinNode hjoin = (HashJoinNode)join;
                    BloomFilter bf = (BloomFilter)hjoin.getHashTable();
                    map = new BloomFilterFilter(bf, hjoin.getMatchColumns(),
                                                outer, inner);
                    PlanNode loader = hjoin.getLoader();
                    loader = new Project(loader, hjoin.getHashColumns());
                    map = new UsingBloomFilter(bf, loader, map);
                }
                break;
            case HASH_TABLE:
                {
                    map = new MapJoin(join.getJoinType(), outer, inner);
                    HashJoinNode hjoin = (HashJoinNode)join;
                    HashTable ht = (HashTable)hjoin.getHashTable();
                    PlanNode loader = hjoin.getLoader();
                    map = new UsingHashTable(ht, loader, map,
                                             hjoin.getHashColumns(),
                                             hjoin.getTKeyComparables(),
                                             hjoin.getCollators());
                }
                break;
            default:
                assert false : join;
                map = join;
            }
            join.getOutput().replaceInput(join, map);
        }
    }

    private BaseQuery getQuery(PlanNode node) {
        PlanWithInput output = node.getOutput();
        while (output != null) {
            if (output instanceof BaseQuery) {
                return (BaseQuery) output;
            }
            output = output.getOutput();
        }
        throw new CorruptedPlanException("PlanNode did not have BaseQuery");
    }


    private PlanNode moveConditionsToOuterNode(PlanNode planNode, ConditionList conditions,
                                               Set<ColumnSource> outerSources, boolean isAntiJoin) {
        ConditionList selectConditions = new ConditionList();
        Iterator<ConditionExpression> iterator = conditions.iterator();
        while (iterator.hasNext()) {
            ConditionExpression condition = iterator.next();
            Set<ColumnSource> columnSources = new ConditionColumnSourcesFinder().find(condition);
            columnSources.removeAll(outerSources);
            PlanNodeProvidesSourcesChecker checker = new PlanNodeProvidesSourcesChecker(columnSources, planNode);
            if (checker.run()) {
                selectConditions.add(condition);
                iterator.remove();
            }
        }
        return selectConditions.isEmpty() ? planNode
                                          : isAntiJoin ? new Select(planNode, negateConjunction(selectConditions))
                                                       : new Select(planNode, selectConditions);
    }

    private ConditionList negateConjunction(ConditionList conditionList) {
        ConditionExpression firstCondition = conditionList.get(0);

        // and all the conditions first
        ConditionExpression condition = new LogicalFunctionCondition("and", conditionList,
                                                                     firstCondition.getSQLtype(),
                                                                     firstCondition.getSQLsource(),
                                                                     firstCondition.getType());
        TValidatedScalar validatedScalarAnd = new TValidatedScalar(BoolLogic.BINARIES[0]); // and
        ((LogicalFunctionCondition)condition).setResolved(validatedScalarAnd);
        List<ConditionExpression> newConditionList = new ArrayList<ConditionExpression>();
        newConditionList.add(condition);

        // negate the resulting condition
        condition = new LogicalFunctionCondition("not", newConditionList,
                                                 condition.getSQLtype(),
                                                 condition.getSQLsource(),
                                                 condition.getType());
        TValidatedScalar validatedScalarNot = new TValidatedScalar(BoolLogic.NOT);
        ((LogicalFunctionCondition)condition).setResolved(validatedScalarNot);

        ConditionList negatedConjunction = new ConditionList();
        negatedConjunction.add(condition);
        return negatedConjunction;
    }

    private static class PlanNodeProvidesSourcesChecker implements PlanVisitor {

        private final Set<ColumnSource> columnSources;
        private final PlanNode planNode;

        private PlanNodeProvidesSourcesChecker(Set<ColumnSource> columnSources, PlanNode node) {
            this.columnSources = columnSources;
            this.planNode = node;
        }

        public boolean run() {
            planNode.accept(this);
            return columnSources.isEmpty();
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            if (columnSources.isEmpty()) {
                return false;
            }
            if (n instanceof ColumnSource) {
                columnSources.remove(n);
                // We want to go inside, because if you have a Group Join, the inner groups are nested nodes within
                // the outer table source
                return true;
            }
            if (n instanceof Subquery) {
                // subquery sources are the source you can see from outside, don't go into the inner subquery
                return false;
            }
            return true;
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            return false;
        }

        @Override
        public boolean visit(PlanNode n) {
            if (columnSources.isEmpty()) {
                return false;
            }
            if (n instanceof ColumnSource) {
                columnSources.remove(n);
                return true;
            }
            if (n instanceof Subquery) {
                return false;
            }
            return true;
        }
    }

}
