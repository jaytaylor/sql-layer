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

package com.akiban.qp.rowtype;

import com.akiban.ais.model.HKey;
import com.akiban.server.aggregation.AggregatorFactory;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DerivedTypesSchema {

    public static Set<RowType> descendentTypes(RowType ancestorType, Set<? extends RowType> allTypes)
    {
        Set<RowType> descendentTypes = new HashSet<RowType>();
        for (RowType type : allTypes) {
            if (type != ancestorType && ancestorType.ancestorOf(type)) {
                descendentTypes.add(type);
            }
        }
        return descendentTypes;
    }

    public synchronized AggregatedRowType newAggregateType(RowType parent, int inputsIndex, List<AggregatorFactory> aggregatorFactories)
    {
        return new AggregatedRowType(this, nextTypeId(), parent, inputsIndex, aggregatorFactories);
    }

    public synchronized FlattenedRowType newFlattenType(RowType parent, RowType child)
    {
        return new FlattenedRowType(this, nextTypeId(), parent, child);
    }

    public synchronized ProjectedRowType newProjectType(List<Expression> columns)
    {
        return new ProjectedRowType(this, nextTypeId(), columns);
    }

    public ProductRowType newProductType(RowType leftType, UserTableRowType branchType, RowType rightType)
    {
        return new ProductRowType(this, nextTypeId(), leftType, branchType, rightType);
    }

    public synchronized ValuesRowType newValuesType(AkType... fields)
    {
        return new ValuesRowType(this, nextTypeId(), fields);
    }

    public HKeyRowType newHKeyRowType(HKey hKey)
    {
        return new HKeyRowType(this, hKey);
    }

    synchronized final int nextTypeId()
    {
        return ++typeIdCounter;
    }

    synchronized final void typeIdToLeast(int minValue) {
        typeIdCounter = Math.max(typeIdCounter, minValue);
    }

    private volatile int typeIdCounter = -1;
}
