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

package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.UserTable;

import java.util.List;

public final class SingleIndexScan extends IndexScan {

    private Index index;

    public SingleIndexScan(Index index, TableSource table)
    {
        super(table);
        this.index = index;
    }
    
    public SingleIndexScan(Index index, TableSource rootMost, TableSource leafMost) {
        this(index, rootMost, rootMost, leafMost, leafMost);
    }

    public SingleIndexScan(Index index,
                     TableSource rootMostTable,
                     TableSource rootMostInnerTable,
                     TableSource leafMostInnerTable,
                     TableSource leafMostTable)
    {
        super(rootMostTable, rootMostInnerTable, leafMostInnerTable, leafMostTable);
        this.index = index;
    }

    public Index getIndex() {
        return index;
    }

    @Override
    public List<IndexColumn> getIndexColumns() {
        return index.getAllColumns();
    }

    @Override
    protected String summarizeIndex() {
        return String.valueOf(index);
    }

    @Override
    protected boolean isAscendingAt(int i) {
        return index.getAllColumns().get(i).isAscending();
    }

    @Override
    public List<ConditionExpression> getGroupConditions() {
        return getConditions();
    }

    @Override
    public UserTable getLeafMostUTable() {
        return (UserTable) index.leafMostTable();
    }

    @Override
    public List<IndexColumn> getAllColumns() {
        return index.getAllColumns();
    }

    @Override
    public int getPeggedCount() {
        // Note! Really what we want are the *leading* equalities. But this method is only
        // used in the context of MultiIndexEnumerator, which will only put in leading
        // equalities.
        List<ExpressionNode> equalityComparands = getEqualityComparands();
        return (equalityComparands == null) ? 0 : equalityComparands.size();
    }

    @Override
    public void removeCoveredConditions(ConditionsStack<ConditionExpression> stack) {
        for (ConditionExpression cond : getConditions())
            stack.removeCondition(cond);
    }

    @Override
    public UserTable findCommonAncestor(IndexScan otherScan) {
        TableSource myTable = getLeafMostTable();
        TableSource otherTable = otherScan.getLeafMostTable();
        int myDepth = myTable.getTable().getDepth();
        int otherDepth = otherTable.getTable().getDepth();
        while (myDepth > otherDepth) {
            myTable = myTable.getParentTable();
            myDepth = myTable.getTable().getDepth();
        }
        while (otherDepth > myDepth) {
            otherTable = otherTable.getParentTable();
            otherDepth = otherTable.getTable().getDepth();
        }
        while (myTable != otherTable) {
            myTable = myTable.getParentTable();
            otherTable = otherTable.getParentTable();
        }
        return myTable.getTable().getTable();
    }
}
