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
import com.akiban.qp.operator.API;
import com.akiban.qp.rowtype.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.akiban.sql.optimizer.rule.costmodel.CostModelMeasurements.*;
import static java.lang.Math.round;

public class CostModel
{
    public double indexScan(IndexRowType rowType, int nRows)
    {
        TreeStatistics treeStatistics = treeStatistics(rowType);
        return treeScan(treeStatistics.rowWidth(), nRows);
    }
    
    public double fullIndexScan(IndexRowType rowType)
    {
        TreeStatistics treeStatistics = treeStatistics(rowType);
        return treeScan(treeStatistics.rowWidth(), treeStatistics.rowCount());
    }

    public double fullGroupScan(UserTableRowType rootTableRowType)
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

    public double ancestorLookup(List<UserTableRowType> ancestorTableTypes)
    {
        // Overhead of AncestorLookup_Default not measured
        long cost = 0;
        for (UserTableRowType ancestorTableType : ancestorTableTypes) {
            cost += hKeyBoundGroupScanSingleRow(ancestorTableType);
        }
        return cost;
    }
    
    public double branchLookup(UserTableRowType branchRootType)
    {
        // Overhead of BranchLookup_Default not measured
        // TODO: Add filtering by row type
        return hKeyBoundGroupScanBranch(branchRootType);
    }

    public double sort(int nRows, boolean mixedMode)
    {
        return SORT_SETUP + SORT_PER_ROW * nRows * (mixedMode ? 1 : SORT_MIXED_MODE_FACTOR);
    }

    public double sortWithLimit(int limit, int sortFields)
    {
        return limit * SORT_LIMIT_PER_ROW * (1 + sortFields * SORT_LIMIT_PER_FIELD_FACTOR);
    }

    public double select(int nRows)
    {
        return nRows * (SELECT_PER_ROW + EXPRESSION_PER_FIELD);
    }

    public double project(RowType rowType, int nRows)
    {
        return nRows * (PROJECT_PER_ROW + rowType.nFields() * EXPRESSION_PER_FIELD);
    }

    public double distinct(int nRows)
    {
        return nRows * DISTINCT_PER_ROW;
    }

    public double product(int nRows)
    {
        return nRows * PRODUCT_PER_ROW;
    }

    public double map(int nOuterRows, int nInnerRowsPerOuter)
    {
        return (nOuterRows * (nInnerRowsPerOuter + 1)) * MAP_PER_ROW;
    }
    
    public double flatten(int nRows)
    {
        return FLATTEN_OVERHEAD + nRows * FLATTEN_PER_ROW;
    }

    public double intersect(int nLeftRows, int nRightRows)
    {
        return (nLeftRows + nRightRows) * INTERSECT_PER_ROW;
    }

    public double hKeyUnion(int nLeftRows, int nRightRows)
    {
        return (nLeftRows + nRightRows) * HKEY_UNION_PER_ROW;
    }

    private double hKeyBoundGroupScanSingleRow(UserTableRowType rootTableRowType)
    {
        TreeStatistics treeStatistics = treeStatistics(rootTableRowType);
        return treeScan(treeStatistics.rowWidth(), 1);
    }
    
    private double hKeyBoundGroupScanBranch(UserTableRowType rootTableRowType)
    {
        // Cost includes access to root
        double cost = hKeyBoundGroupScanSingleRow(rootTableRowType);
        // The rest of the cost is sequential access. It is proportional to the fullGroupScan -- divide that cost
        // by the number of rows in the root table, assuming that each group has the same size.
        TreeStatistics rootTableStatistics = statisticsMap.get(rootTableRowType.typeId());
        cost += fullGroupScan(rootTableRowType) / rootTableStatistics.rowCount();
        return cost;
    }
    
    public static CostModel newCostModel(Schema schema, TableRowCounts tableRowCounts)
    {
        return new CostModel(schema, tableRowCounts);
    }

    private static double treeScan(int rowWidth, long nRows)
    {
        return
            RANDOM_ACCESS_PER_ROW + RANDOM_ACCESS_PER_BYTE * rowWidth +
            nRows * (SEQUENTIAL_ACCESS_PER_ROW + SEQUENTIAL_ACCESS_PER_BYTE * rowWidth);
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
    
    private CostModel(Schema schema, TableRowCounts tableRowCounts)
    {
        this.schema = schema;
        this.tableRowCounts = tableRowCounts;
        for (UserTableRowType tableRowType : schema.userTableTypes()) {
            TreeStatistics tableStatistics = TreeStatistics.forTable(tableRowType, tableRowCounts);
            statisticsMap.put(tableRowType.typeId(), tableStatistics);
            for (IndexRowType indexRowType : tableRowType.indexRowTypes()) {
                TreeStatistics indexStatistics = TreeStatistics.forIndex(indexRowType, tableRowCounts);
                statisticsMap.put(indexRowType.typeId(), indexStatistics);
            }
        }
        for (IndexRowType indexRowType : schema.groupIndexRowTypes()) {
            TreeStatistics indexStatistics = TreeStatistics.forIndex(indexRowType, tableRowCounts);
            statisticsMap.put(indexRowType.typeId(), indexStatistics);
        }
    }

    private final Schema schema;
    private final TableRowCounts tableRowCounts;
    private final Map<Integer, TreeStatistics> statisticsMap = new HashMap<Integer, TreeStatistics>();
}
