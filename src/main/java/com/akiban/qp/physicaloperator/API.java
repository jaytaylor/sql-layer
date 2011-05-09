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
import com.akiban.ais.model.Index;
import com.akiban.qp.expression.Expression;
import com.akiban.qp.expression.IndexKeyRange;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.RowBase;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;

import java.util.Collection;
import java.util.List;

public class API
{
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
        return groupScan_Default(groupTable, false, NO_LIMIT);
    }

    public static PhysicalOperator groupScan_Default(GroupTable groupTable,
                                                     boolean reverse,
                                                     Limit limit,
                                                     Bindable<IndexKeyRange> bindableIndexKeyRange)
    {
        return new GroupScan_Default(groupTable,
                                     reverse,
                                     limit,
                                     bindableIndexKeyRange);
    }

    public static PhysicalOperator groupScan_Default(GroupTable groupTable, boolean reverse, Limit limit)
    {
        return new GroupScan_Default(groupTable,
                                     reverse,
                                     limit,
                                     ConstantValueBindable.ofNull(IndexKeyRange.class));
    }

    public static PhysicalOperator indexLookup_Default(PhysicalOperator inputOperator,
                                                       GroupTable groupTable)
    {
        return indexLookup_Default(inputOperator, groupTable, NO_LIMIT);
    }

    public static PhysicalOperator indexLookup_Default(PhysicalOperator inputOperator,
                                                       GroupTable groupTable,
                                                       Limit limit)
    {
        return new IndexLookup_Default(inputOperator, groupTable, limit);
    }

    public static PhysicalOperator ancestorLookup_Default(PhysicalOperator inputOperator,
                                                          GroupTable groupTable,
                                                          RowType rowType,
                                                          List<RowType> ancestorTypes)
    {
        return new AncestorLookup_Default(inputOperator, groupTable, rowType, ancestorTypes);
    }

    public static PhysicalOperator indexScan_Default(Index index)
    {
        return indexScan_Default(index, false, ConstantValueBindable.ofNull(IndexKeyRange.class));
    }

    public static PhysicalOperator indexScan_Default(Index index,
                                                     boolean reverse,
                                                     Bindable<IndexKeyRange> indexKeyRangeBindable)
    {
        return new IndexScan_Default(index, reverse, indexKeyRangeBindable);
    }

    public static PhysicalOperator select_HKeyOrdered(PhysicalOperator inputOperator,
                                                      RowType predicateRowType,
                                                      Expression predicate)
    {
        return new Select_HKeyOrdered(inputOperator, predicateRowType, predicate);
    }

    public static PhysicalOperator cut_Default(Schema schema,
                                               PhysicalOperator inputOperator,
                                               Collection<RowType> cutTypes)
    {
        return new Cut_Default(schema, inputOperator, cutTypes);
    }

    public static PhysicalOperator extract_Default(Schema schema,
                                                   PhysicalOperator inputOperator,
                                                   Collection<RowType> extractTypes)
    {
        return new Extract_Default(schema, inputOperator, extractTypes);
    }

    public static Cursor emptyBindings(StoreAdapter adapter, PhysicalOperator physicalOperator) {
        Bindings empty = new ArrayBindings(0);
        return physicalOperator.cursor(adapter, empty);
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
}
