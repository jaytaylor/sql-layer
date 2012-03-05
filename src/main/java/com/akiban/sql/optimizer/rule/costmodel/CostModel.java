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

package com.akiban.sql.optimizer.rule.costmodel;

import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.rowtype.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CostModel
{
    public long indexScan(IndexRowType rowType, int nRows)
    {
        TreeStatistics treeStatistics = treeStatistics(rowType);
        return treeScan(treeStatistics.rowWidth(), nRows);
    }
    
    public long fullIndexScan(IndexRowType rowType)
    {
        TreeStatistics treeStatistics = treeStatistics(rowType);
        return treeScan(treeStatistics.rowWidth(), treeStatistics.rowCount());
    }

    public long fullGroupScan(UserTableRowType rootTableRowType)
    {
        // A group scan basically does no random access, even to the very first row (I think, at least as far as CPU
        // costs are concerned). So for each table in the group, subtract the cost of a tree scan for 0 rows to account
        // for this. This leaves just the sequential access costs.
        long cost = 0;
        for (UserTableRowType rowType : groupTableRowTypes(rootTableRowType)) {
            TreeStatistics treeStatistics = statisticsMap.get(rowType.typeId());
            cost += 
                treeScan(treeStatistics.rowWidth(), treeStatistics.rowCount()) 
                - treeScan(treeStatistics.rowWidth(), 0);
        }
        return cost;
    }

    public long ancestorLookup(List<UserTableRowType> ancestorTableTypes)
    {
        long cost = 0;
        for (UserTableRowType ancestorTableType : ancestorTableTypes) {
            cost += hKeyBoundGroupScanSingleRow(ancestorTableType);
        }
        return cost;
    }
    
    public long branchLookup(UserTableRowType branchRootType)
    {
        return hKeyBoundGroupScanBranch(branchRootType);
    }

    public long sort()
    {
        assert false : "Not implemented yet";
        return -1L;
    }

    public long sortDistinct()
    {
        assert false : "Not implemented yet";
        return -1L;
    }

    public long select(int nRows)
    {
        return nRows * expression();
    }

    public long project(int nRows)
    {
        return nRows * expression();
    }

    public long distinct()
    {
        assert false : "Not implemented yet";
        return -1L;
    }

    public long product()
    {
        assert false : "Not implemented yet";
        return -1L;
    }

    public long map()
    {
        assert false : "Not implemented yet";
        return -1L;
    }

    public long flatten()
    {
        assert false : "Not implemented yet";
        return -1L;
    }

    private long hKeyBoundGroupScanSingleRow(UserTableRowType rootTableRowType)
    {
        TreeStatistics treeStatistics = treeStatistics(rootTableRowType);
        return treeScan(treeStatistics.rowWidth(), 1);
    }
    
    private long hKeyBoundGroupScanBranch(UserTableRowType rootTableRowType)
    {
        // Cost includes access to root
        long cost = hKeyBoundGroupScanSingleRow(rootTableRowType);
        // The rest of the cost is sequential access. It is proportional to the fullGroupScan -- divide that cost
        // by the number of rows in the root table, assuming that each group has the same size.
        TreeStatistics rootTableStatistics = statisticsMap.get(rootTableRowType.typeId());
        cost += fullGroupScan(rootTableRowType) / rootTableStatistics.rowCount();
        return cost;
    }
    
    public static CostModel newCostModel(Schema schema)
    {
        return new CostModel(schema);
    }

    private static long treeScan(int rowWidth, long nRows)
    {
        // These coefficients approximate the measurements made by TreeScanCT.
        // Random access cost is (ar * rowWidth + br)
        double ar = 0.012;
        double br = 5.15;
        // Sequential access cost coefficient is (as * rowWidth * bs)
        double as = 0.0046;
        double bs = 0.61;
        double cost = ar * rowWidth + br + nRows * (as * rowWidth + bs);
        return (long) cost;
    }
    
    private static long expression()
    {
        // According to ExpressionCT, which tries expressions with up to 8 literals and 7 operators, expression
        // evaluation is always under 1 usec.
        return 1;
    }

    private TreeStatistics treeStatistics(RowType rowType)
    {
        return statisticsMap.get(rowType.typeId());
    }

    private List<UserTableRowType> groupTableRowTypes(UserTableRowType rootTableRowType)
    {
        List<UserTableRowType> rowTypes = new ArrayList<UserTableRowType>();
        List<UserTable> groupTables = new ArrayList<UserTable>();
        findGroupTables(rootTableRowType.userTable(), groupTables);
        for (UserTable table : groupTables) {
            rowTypes.add(schema.userTableRowType(table));
        }
        return rowTypes;
    }
    
    private void findGroupTables(UserTable table, List<UserTable> groupTables)
    {
        groupTables.add(table);
        for (Join join : table.getChildJoins()) {
            findGroupTables(join.getChild(), groupTables);
        }
    }
    
    private CostModel(Schema schema)
    {
        this.schema = schema;
        for (UserTableRowType tableRowType : schema.userTableTypes()) {
            TreeStatistics tableStatistics = TreeStatistics.forTable(tableRowType);
            statisticsMap.put(tableRowType.typeId(), tableStatistics);
            for (IndexRowType indexRowType : tableRowType.indexRowTypes()) {
                TreeStatistics indexStatistics = TreeStatistics.forIndex(indexRowType);
                statisticsMap.put(indexRowType.typeId(), indexStatistics);
            }
        }
    }

    private final Schema schema;
    private final Map<Integer, TreeStatistics> statisticsMap = new HashMap<Integer, TreeStatistics>();
}
