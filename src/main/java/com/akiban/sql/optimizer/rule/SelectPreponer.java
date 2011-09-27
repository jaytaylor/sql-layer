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

import java.util.*;

/** Move WHERE clauses closer to their table origin.
 * This rule runs after flattening has been laid out.
 *
 * Note: <i>prepone</i>, while not an American or British English
 * word, is the transparent opposite of <i>postpone</i>.
 */
// TODO: Something similar is needed to handle moving HAVING
// conditions on the group by fields across the aggregation boundary
// and WHERE conditions on subqueries (views) into the subquery
// itself. These need to run earlier to affect indexing. Not sure how
// to integrate all these. Maybe move everything earlier on and then
// recognize joins of such filtered tables.
public class SelectPreponer extends BaseRule
{
    static class TableOriginFinder implements PlanVisitor, ExpressionVisitor {
        List<PlanNode> origins = new ArrayList<PlanNode>();
        Map<PlanNode,PlanNode> nestings;

        public void find(PlanNode root) {
            root.accept(this);
        }

        public List<PlanNode> getOrigins() {
            return origins;
        }

        public Map<PlanNode,PlanNode> getNestings() {
            return nestings;
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
            if (n instanceof IndexScan) {
                origins.add(n);
            }
            else if (n instanceof TableLoader) {
                if (n instanceof BasePlanWithInput) {
                    PlanNode input = ((BasePlanWithInput)n).getInput();
                    if (!((input instanceof TableLoader) ||
                          (input instanceof IndexScan))) {
                        // Don't bother putting both in.
                        origins.add(n);
                    }
                }
                else {
                    origins.add(n);
                }
            }
            else if (n instanceof Product) {
                if (nestings == null)
                    nestings = new HashMap<PlanNode,PlanNode>();
                for (PlanNode subplan : ((Product)n).getSubplans())
                    nestings.put(subplan, n);
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
        TableOriginFinder finder = new TableOriginFinder();
        finder.find(plan.getPlan());
        for (PlanNode origin : finder.getOrigins()) {
            new Preponer(finder.getNestings()).pullToward(origin);
        }
    }
    
    static class Preponer {
        Map<PlanNode,PlanNode> nestings;
        Map<TableSource,PlanNode> loaders;
        Map<ExpressionNode,PlanNode> indexColumns;
        
        public Preponer(Map<PlanNode,PlanNode> nestings) {
            this.nestings = nestings;
        }

        protected void pullToward(PlanNode node) {
            loaders = new HashMap<TableSource,PlanNode>();
            if (node instanceof IndexScan) {
                indexColumns = new HashMap<ExpressionNode,PlanNode>();
                for (ExpressionNode column : ((IndexScan)node).getColumns())
                    indexColumns.put(column, node);
                node = node.getOutput();
            }
            while (node instanceof TableLoader) {
                for (TableSource table : ((TableLoader)node).getTables()) {
                    loaders.put(table, node);
                }
                node = getOutput(node);
            }
            boolean sawJoin = false;
            while (true) {
                if (node instanceof Flatten) {
                    // Limit to tables that are inner joined (and on the
                    // outer side of outer joins.)
                    Set<TableSource> inner = ((Flatten)node).getInnerJoinedTables();
                    loaders.keySet().retainAll(inner);
                    if (indexColumns != null) {
                        Iterator<ExpressionNode> iter = indexColumns.keySet().iterator();
                        while (iter.hasNext()) {
                            ExpressionNode expr = iter.next();
                            if (expr.isColumn() && 
                                !inner.contains(((ColumnExpression)expr).getTable()))
                                iter.remove();
                        }
                    }
                    sawJoin = true;
                }
                else if (node instanceof Product) {
                    // Only inner right now.
                    sawJoin = true;
                }
                else
                    break;
                node = getOutput(node);
            }
            if (!sawJoin ||
                (loaders.isEmpty() &&
                 ((indexColumns == null) || indexColumns.isEmpty()))) {
                // We didn't see any joins (conditions will follow
                // loading directly -- nothing to move over) or ran
                // out of things to move.
                return;
            }
            if (node instanceof Select) {
                moveConditions(((Select)node).getConditions());
            }
        }

        // Have a straight path to these conditions and know where
        // tables came from.  See what can be moved back there.
        protected void moveConditions(List<ConditionExpression> conditions) {
            Iterator<ConditionExpression> iter = conditions.iterator();
            while (iter.hasNext()) {
                ConditionExpression condition = iter.next();
                PlanNode moveTo = canMove(condition);
                if (moveTo != null) {
                    moveCondition(condition, moveTo);
                    iter.remove();
                }
            }
        }

        // Return where this condition can move.
        // TODO: Can move to after subset of joins once enough tables are joined.
        protected PlanNode canMove(ConditionExpression condition) {
            TableSource table = getSingleTableConditionTable(condition);
            if (table == null)
                return null;
            if (indexColumns != null) {
                // Can check the index column before it's used for lookup.
                PlanNode loader = indexColumns.get(((ComparisonCondition)
                                                    condition).getLeft());
                if (loader != null)
                    return loader;
            }
            return loaders.get(table);
        }

        protected void moveCondition(ConditionExpression condition, 
                                     PlanNode before) {
            Select select = null;
            PlanWithInput after = before.getOutput();
            if (after instanceof Select)
                select = (Select)after;
            else {
                select = new Select(before, new ArrayList<ConditionExpression>(1));
                after.replaceInput(before, select);
            }
            select.getConditions().add(condition);
        }

        // Like PlanNode.getOutput(), except that it jumps up to Products, etc.
        protected PlanNode getOutput(PlanNode input) {
            PlanNode output = input.getOutput();
            if ((output == null) && (nestings != null))
                output = nestings.get(input);
            return output;
        }

    }

    /** If this condition involves only a single table, return it. */
    // TODO: Lots of room for improvement here, even with that simple contract.
    public static TableSource getSingleTableConditionTable(ConditionExpression condition) {
        if (!(condition instanceof ComparisonCondition))
            return null;
        ComparisonCondition comp = (ComparisonCondition)condition;
        ExpressionNode left = comp.getLeft();
        ExpressionNode right = comp.getRight();
        if (!(left.isColumn()))
            return null;
        ColumnSource table = null;
        table = ((ColumnExpression)left).getTable();
        if (!(table instanceof TableSource))
            return null;
        if (!isConstant(right))
            return null;
        return (TableSource)table;
    }

    // TODO: Column from outer table okay, too. Need general predicate for that.
    protected static boolean isConstant(ExpressionNode node) {
        if ((node instanceof ConstantExpression) || 
            (node instanceof ParameterExpression))
            return true;
        if (node instanceof CastExpression)
            return isConstant(((CastExpression)node).getOperand());
        return false;
    }

}
