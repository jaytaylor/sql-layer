/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.qp.rowtype;

import com.foundationdb.server.types.AkType;

import com.foundationdb.server.aggregation.AggregatorFactory;
import com.foundationdb.server.types3.TInstance;

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
    
    @Override
    public TInstance typeInstanceAt(int index) {
        if (index < inputsIndex)
            return base.typeInstanceAt(index);
        else
            return pAggrTypes.get(index - inputsIndex);
    }

    public AggregatedRowType(DerivedTypesSchema schema, int typeId,
                             RowType base, int inputsIndex, List<AggregatorFactory> aggregatorFactories) {
        this(schema, typeId, base, inputsIndex, aggregatorFactories, null);
    }

    public AggregatedRowType(DerivedTypesSchema schema, int typeId,
                             RowType base, int inputsIndex, List<AggregatorFactory> aggregatorFactories,
                             List<? extends TInstance> pAggrTypes) {
        super(schema, typeId);
        this.base = base;
        this.inputsIndex = inputsIndex;
        this.aggregatorFactories = aggregatorFactories;
        this.pAggrTypes = pAggrTypes;
    }

    private final RowType base;
    private final int inputsIndex;
    private final List<AggregatorFactory> aggregatorFactories;
    private final List<? extends TInstance> pAggrTypes;
}
