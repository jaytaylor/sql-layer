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

import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;

import java.util.*;

public class IndexGoal
{
    // Tables already bound outside.
    private Set<TableSource> boundTables;

    // The group we want to index.
    private TableGroup group;
    // All the tables that are joined together here, including outer
    // join ones that can't have indexes, but might have sorting or
    // covering.
    private List<TableSource> tables;

    // All the conditions that might be indexable.
    private List<ConditionExpression> conditions;

    // If both grouping and ordering are present, they must be
    // compatible: something satisfying the ordering would also handle
    // the grouping.
    private List<ExpressionNode> grouping;
    private List<OrderByExpression> ordering;

    public IndexGoal(TableGroup group) {
    }

    public static enum OrderEffectiveness {
        NONE, PARTIAL_GROUP, GROUP, SORT
    }
    
    /** Populate given index usage according to goal.
     * Return <code>null</code> if useless, otherwise {@link OrderEffectiveness} 
     * for how well it orders.
     */
    public Effectiveness usable(IndexUsage index) {
        Index aisIndex = index.getIndex();
        if (aisIndex.isGroupIndex()) {
            // The leaf must be used or else we'll get duplicates from a
            // scan (the indexed columns need not be root to leaf, making
            // ancestors discontiguous and duplicates hard to eliminate).
            if (!contains(tables, index.leafMostTable()))
                return null;
            // The root must be present, since the index does not contain
            // orphans.
            if (!contains(tables, index.rootMostTable()))
                return null;
        }        
        List<IndexColumn> indexColumns = aisIndex.getColumns();
        int ncols = indexColumns.size();
        int nequals = 0;
        while (nequals < ncols) {
            IndexColumn indexColumn = indexColumns.get(nequals);
            ExpressionNode indexExpression = getIndexExpression(indexColumn);
            ConditionExpression equalityCondition = null;
            ExpressionNode otherComparand = null;
            for (ConditionExpression condition : conditions) {
                if (condition instanceof ComparisonCondition) {
                    ComparisonCondition ccond = (ComparisonCondition)condition;
                    if (ccond.getOperation() == Comparison.EQ) {
                        if (indexExpression.equals(ccond.getLeft())) {
                            otherComparand = ccond.getRight();
                            equalityCondition = condition;
                            break;
                        }
                        else if (indexExpression.equals(ccond.getRight())) {
                            otherComparand = ccond.getLeft();
                            equalityCondition = condition;
                            break;
                        }
                    }
                }
            }
            if (equalityCondition == null)
                break;
            index.addEqualityCondition(equalityCondition, otherComparand);
            nequals++;
        }
        if (nequals < ncols) {
            IndexColumn indexColumn = indexColumns.get(nequals);
            ExpressionNode indexExpression = getIndexExpression(indexColumn);
            for (ConditionExpression condition : conditions) {
                if (condition instanceof ComparisonCondition) {
                    ComparisonCondition ccond = (ComparisonCondition)condition;
                    ExpressionNode otherComparand = null;
                    if (indexExpression.equals(ccond.getLeft())) {
                        otherComparand = ccond.getRight();
                    }
                    else if (indexExpression.equals(ccond.getRight())) {
                        otherComparand = ccond.getLeft();
                    }
                    if (otherComparand != null) {
                        index.addInequalityCondition(condition,
                                                     ccond.getOperation(),
                                                     otherComparand);
                    }
                }
            }
            List<OrderByExpression> ordering = 
                new ArrayList<OrderByExpression(ncols - nequals);
            for (int i = nequals; i < ncols; i++) {
                IndexColumn indexColumn = indexColumns.get(i);
                ExpressionNode indexExpression = getIndexExpression(indexColumn);
                OrderByExpression order = 
                    new OrderByExpression(indexExpresion, indexColumn.isAscending());
            }
            // TODO: Check whether better to reverse the sort.
        }
    }

    protected static boolean contains(Collection<TableSource> tables,
                                      Table aisTable) {
        for (TableSource table : tables) {
            if (table.getTable().getTable() == aisTable)
                return true;
        }
        return false;
    }

}
