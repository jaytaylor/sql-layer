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
import com.akiban.qp.rowtype.RowType;

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

    public static PhysicalOperator groupScan_Default(StoreAdapter store, GroupTable groupTable)
    {
        return new GroupScan_Default(store, groupTable);
    }

    public static PhysicalOperator indexLookup_Default(PhysicalOperator inputOperator,
                                                       GroupTable groupTable,
                                                       List<RowType> missingTypes)
    {
        return new IndexLookup_Default(inputOperator, groupTable, missingTypes);
    }

    public static PhysicalOperator indexScan_Default(Index index)
    {
        return new IndexScan_Default(index);
    }

    public static PhysicalOperator select_HKeyOrdered(PhysicalOperator inputOperator,
                                                      RowType predicateRowType,
                                                      Expression predicate)
    {
        return new Select_HKeyOrdered(inputOperator, predicateRowType, predicate);
    }
}
