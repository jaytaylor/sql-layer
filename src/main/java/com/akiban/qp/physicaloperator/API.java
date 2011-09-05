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

package com.akiban.qp.physicaloperator;

import com.akiban.ais.model.GroupTable;
import com.akiban.qp.expression.Expression;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.UserTableRowType;
import com.akiban.sql.optimizer.ExpressionRow;
import com.akiban.server.aggregation.AggregatorFactory;

import java.util.*;

public class API
{
    // Aggregate
    public static PhysicalOperator aggregate(PhysicalOperator inputOperator,
                                             int inputsIndex,
                                             AggregatorFactory factory,
                                             List<String> aggregatorNames)
    {
        return new Aggregation_Partial(inputOperator, inputsIndex, factory, aggregatorNames);
    }

    // Project

    public static PhysicalOperator project_Default(PhysicalOperator inputOperator,
                                                   RowType rowType,
                                                   List<Expression> projections)
    {
        return new Project_Default(inputOperator, rowType, projections);
    }
    
    public static PhysicalOperator project_Table(PhysicalOperator inputOperator, 
                                                 RowType inputRowType,
                                                 RowType outputRowType,
                                                 List<Expression>projections) 
    {
        return new Project_Table (inputOperator, inputRowType, outputRowType, projections);
    }
    // Flatten

    public static PhysicalOperator flatten_HKeyOrdered(PhysicalOperator inputOperator,
                                                       RowType parentType,
                                                       RowType childType,
                                                       JoinType joinType)
    {
        return flatten_HKeyOrdered(inputOperator, parentType, childType, joinType, EnumSet.noneOf(FlattenOption.class));
    }

    public static PhysicalOperator flatten_HKeyOrdered(PhysicalOperator inputOperator,
                                                       RowType parentType,
                                                       RowType childType,
                                                       JoinType joinType,
                                                       FlattenOption flag0)
    {
        return new Flatten_HKeyOrdered(inputOperator, parentType, childType, joinType, EnumSet.of(flag0));
    }

    public static PhysicalOperator flatten_HKeyOrdered(PhysicalOperator inputOperator,
                                                       RowType parentType,
                                                       RowType childType,
                                                       JoinType joinType,
                                                       FlattenOption flag0,
                                                       FlattenOption flag1)
    {
        return new Flatten_HKeyOrdered(inputOperator, parentType, childType, joinType, EnumSet.of(flag0, flag1));
    }

    public static PhysicalOperator flatten_HKeyOrdered(PhysicalOperator inputOperator,
                                                       RowType parentType,
                                                       RowType childType,
                                                       JoinType joinType,
                                                       EnumSet<FlattenOption> flags)
    {
        return new Flatten_HKeyOrdered(inputOperator, parentType, childType, joinType, flags);
    }

    // GroupScan

    public static PhysicalOperator groupScan_Default(GroupTable groupTable)
    {
        return groupScan_Default(groupTable, NO_LIMIT);
    }

    public static PhysicalOperator groupScan_Default(GroupTable groupTable, Limit limit)
    {
        return new GroupScan_Default(new GroupScan_Default.FullGroupCursorCreator(groupTable), limit);
    }

    public static PhysicalOperator groupScan_Default(GroupTable groupTable,
                                                     Limit limit,
                                                     int hKeyBindingPosition,
                                                     boolean deep)
    {
        return new GroupScan_Default(
                new GroupScan_Default.PositionalGroupCursorCreator(groupTable, hKeyBindingPosition, deep),
                limit
        );
    }

    public static PhysicalOperator valuesScan_Default (List<ExpressionRow> rows)
    {
        return new ValuesScan_Default (rows);
    }
    // BranchLookup

    public static PhysicalOperator branchLookup_Default(PhysicalOperator inputOperator,
                                                        GroupTable groupTable,
                                                        RowType inputRowType,
                                                        RowType outputRowType,
                                                        LookupOption flag)
    {
        return branchLookup_Default(inputOperator, groupTable, inputRowType, outputRowType, flag, NO_LIMIT);
    }

    public static PhysicalOperator branchLookup_Default(PhysicalOperator inputOperator,
                                                        GroupTable groupTable,
                                                        RowType inputRowType,
                                                        RowType outputRowType,
                                                        LookupOption flag,
                                                        Limit limit)
    {
        return new BranchLookup_Default(inputOperator, groupTable, inputRowType, outputRowType, flag, limit);
    }

    public static PhysicalOperator branchLookup_Nested(GroupTable groupTable,
                                                       RowType inputRowType,
                                                       RowType outputRowType,
                                                       LookupOption flag,
                                                       int inputBindingPosition)
    {
        return new BranchLookup_Nested(groupTable, inputRowType, outputRowType, flag, inputBindingPosition);
    }

    // Limit

    public static PhysicalOperator limit_Default(PhysicalOperator inputOperator, int rows)
    {
        return new Limit_Default(inputOperator, rows);
    }

    // AncestorLookup

    public static PhysicalOperator ancestorLookup_Default(PhysicalOperator inputOperator,
                                                          GroupTable groupTable,
                                                          RowType rowType,
                                                          Collection<? extends RowType> ancestorTypes,
                                                          LookupOption flag)
    {
        return new AncestorLookup_Default(inputOperator, groupTable, rowType, ancestorTypes, flag);
    }

    public static PhysicalOperator ancestorLookup_Nested(GroupTable groupTable,
                                                         RowType rowType,
                                                         Collection<? extends RowType> ancestorTypes,
                                                         int hKeyBindingPosition)
    {
        return new AncestorLookup_Nested(groupTable, rowType, ancestorTypes, hKeyBindingPosition);
    }

    // IndexScan

    public static PhysicalOperator indexScan_Default(IndexRowType indexType)
    {
        return indexScan_Default(indexType, false, null, indexType.tableType());
    }

    public static PhysicalOperator indexScan_Default(IndexRowType indexType, boolean reverse, IndexKeyRange indexKeyRange)
    {
        return indexScan_Default(indexType, reverse, indexKeyRange, indexType.tableType());
    }

    public static PhysicalOperator indexScan_Default(IndexRowType indexType,
                                                     boolean reverse,
                                                     IndexKeyRange indexKeyRange,
                                                     UserTableRowType innerJoinUntilRowType)
    {
        return new IndexScan_Default(indexType, reverse, indexKeyRange, innerJoinUntilRowType);
    }

    // Select

    public static PhysicalOperator select_HKeyOrdered(PhysicalOperator inputOperator,
                                                      RowType predicateRowType,
                                                      Expression predicate)
    {
        return new Select_HKeyOrdered(inputOperator, predicateRowType, predicate);
    }

    // Filter

    public static PhysicalOperator filter_Default(PhysicalOperator inputOperator, Collection<RowType> keepTypes)
    {
        return new Filter_Default(inputOperator, keepTypes);
    }

    // Product

    /**
     * @deprecated Use product_NestedLoops instead.
     */
    public static PhysicalOperator product_ByRun(PhysicalOperator input,
                                                 RowType leftType,
                                                 RowType rightType)
    {
        return new Product_ByRun(input, leftType, rightType);
    }

    public static PhysicalOperator product_NestedLoops(PhysicalOperator outerInput,
                                                       PhysicalOperator innerInput,
                                                       RowType outerType,
                                                       RowType innerType,
                                                       int inputBindingPosition)
    {
        return new Product_NestedLoops(outerInput, innerInput, outerType, innerType, inputBindingPosition);
    }

    // Cut

    public static PhysicalOperator count_Default(PhysicalOperator input,
                                                 RowType countType)
    {
        return new Count_Default(input, countType);
    }

    // Sort

    public static PhysicalOperator sort_InsertionLimited(PhysicalOperator inputOperator, 
                                                         RowType sortType, 
                                                         List<Expression> sortExpressions,
                                                         List<Boolean> sortDescendings,
                                                         int limit)
    {
        return new Sort_InsertionLimited(inputOperator, sortType, 
                                         sortExpressions, sortDescendings, limit);
    }

    // Execution interface

    public static Cursor cursor(PhysicalOperator root, StoreAdapter adapter)
    {
        // if all they need is the wrapped cursor, create it directly
        return new TopLevelWrappingCursor(root.cursor(adapter));
    }

    // Options

    // Flattening flags

    public static enum JoinType {
        INNER_JOIN,
        LEFT_JOIN,
        RIGHT_JOIN,
        FULL_JOIN
    }

    public static enum FlattenOption {
        KEEP_PARENT,
        KEEP_CHILD,
        LEFT_JOIN_SHORTENS_HKEY
    }

    // Lookup flags

    public static enum LookupOption {
        KEEP_INPUT,
        DISCARD_INPUT
    }

    // Class state

    private static final Limit NO_LIMIT = new Limit()
    {

        @Override
        public boolean limitReached(RowBase row)
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "NO LIMIT";
        }

    };
}
