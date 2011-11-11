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

import com.akiban.server.types.AkType;

import com.akiban.server.aggregation.AggregatorFactory;

import java.util.List;

public final class AggregatedRowType extends DerivedRowType {
    @Override
    public int nFields() {
        return base.nFields();
    }

    @Override
    public AkType typeAt(int index) {
        if (index < inputsIndex)
            return base.typeAt(index);
        else
            return aggregatorFactories.get(index - inputsIndex).outputType();
    }

    public AggregatedRowType(DerivedTypesSchema schema, int typeId,
                             RowType base, int inputsIndex, List<AggregatorFactory> aggregatorFactories) {
        super(schema, typeId);
        this.base = base;
        this.inputsIndex = inputsIndex;
        this.aggregatorFactories = aggregatorFactories;
    }

    private final RowType base;
    private final int inputsIndex;
    private final List<AggregatorFactory> aggregatorFactories;
}
