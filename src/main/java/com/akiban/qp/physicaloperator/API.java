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

import java.util.Collection;
import java.util.List;

public class API
{
    public static PhysicalOperator project_Default(PhysicalOperator inputOperator,
                                                   RowType rowType,
                                                   List<Expression> projections)
    {
        return new Project_Default(inputOperator, rowType, projections);
    }

    public static PhysicalOperator flatten_HKeyOrdered(PhysicalOperator inputOperator,
                                                       RowType parentType,
                                                       RowType childType)
    {
        return flatten_HKeyOrdered(inputOperator, parentType, childType, Flatten_HKeyOrdered.DEFAULT);
    }

    public static PhysicalOperator flatten_HKeyOrdered(PhysicalOperator inputOperator,
                                                       RowType parentType,
                                                       RowType childType,
                                                       int flags)
    {
        return new Flatten_HKeyOrdered(inputOperator, parentType, childType, flags);
    }

    public static PhysicalOperator groupScan_Default(GroupTable groupTable)
    {
        return groupScan_Default(groupTable, NO_LIMIT);
    }

    public static PhysicalOperator groupScan_Default(GroupTable groupTable,
                                                     Limit limit,
                                                     IndexKeyRange indexKeyRange)
    {
        return new GroupScan_Default(groupTable, limit, indexKeyRange);
    }

    public static PhysicalOperator groupScan_Default(GroupTable groupTable, Limit limit)
    {
        return new GroupScan_Default(groupTable, limit, null);
    }

    public static PhysicalOperator branchLookup_Default(PhysicalOperator inputOperator,
                                                        GroupTable groupTable,
                                                        RowType inputRowType,
                                                        RowType outputRowType,
                                                        boolean keepInput)
    {
        return branchLookup_Default(inputOperator, groupTable, inputRowType, outputRowType, keepInput, NO_LIMIT);
    }

    public static PhysicalOperator branchLookup_Default(PhysicalOperator inputOperator,
                                                        GroupTable groupTable,
                                                        RowType inputRowType,
                                                        RowType outputRowType,
                                                        boolean keepInput,
                                                        Limit limit)
    {
        return new BranchLookup_Default(inputOperator, groupTable, inputRowType, outputRowType, keepInput, limit);
    }

    public static PhysicalOperator limit_Default(PhysicalOperator inputOperator, int rows) {
        return new Limit_Default(inputOperator, rows);
    }

    public static PhysicalOperator ancestorLookup_Default(PhysicalOperator inputOperator,
                                                          GroupTable groupTable,
                                                          RowType rowType,
                                                          List<? extends RowType> ancestorTypes,
                                                          boolean keepInput)
    {
        return new AncestorLookup_Default(inputOperator, groupTable, rowType, ancestorTypes, keepInput);
    }

    public static PhysicalOperator indexScan_Default(IndexRowType indexType)
    {
        return indexScan_Default(indexType, false, null);
    }

    public static PhysicalOperator indexScan_Default(IndexRowType indexType, boolean reverse, IndexKeyRange indexKeyRange)
    {
        return new IndexScan_Default(indexType, reverse, indexKeyRange);
    }

    public static PhysicalOperator select_HKeyOrdered(PhysicalOperator inputOperator,
                                                      RowType predicateRowType,
                                                      Expression predicate)
    {
        return new Select_HKeyOrdered(inputOperator, predicateRowType, predicate);
    }

    public static PhysicalOperator cut_Default(PhysicalOperator inputOperator, RowType cutType)
    {
        return new Cut_Default(inputOperator, cutType);
    }

    public static PhysicalOperator extract_Default(PhysicalOperator inputOperator,
                                                   Collection<RowType> extractTypes)
    {
        return new Extract_Default(inputOperator, extractTypes);
    }

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

    public static Cursor cursor(PhysicalOperator root, StoreAdapter adapter) {
        // if all they need is the wrapped cursor, create it directly
        return new TopLevelWrappingCursor(root.cursor(adapter));
    }

    // For all flags
    public static final int DEFAULT = 0x00;
    // Flattening flags
    public static final int KEEP_PARENT = Flatten_HKeyOrdered.KEEP_PARENT;
    public static final int KEEP_CHILD = Flatten_HKeyOrdered.KEEP_CHILD;
    public static final int INNER_JOIN = Flatten_HKeyOrdered.INNER_JOIN;
    public static final int LEFT_JOIN = Flatten_HKeyOrdered.LEFT_JOIN;
    public static final int RIGHT_JOIN = Flatten_HKeyOrdered.RIGHT_JOIN;
    public static final int OUTER_JOIN_SHORTENS_HKEY = Flatten_HKeyOrdered.OUTER_JOIN_SHORTENS_HKEY;
}
