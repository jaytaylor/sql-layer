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
        AkType override = aggregatorFactories.get(index).overrideType();
        if (override != null)
            return override;
        else
            return base.typeAt(index);
    }

    public AggregatedRowType(Schema schema, int typeId, 
                             RowType base, List<AggregatorFactory> aggregatorFactories) {
        super(schema, typeId);
        this.base = base;
        this.aggregatorFactories = aggregatorFactories;
    }

    private final RowType base;
    private final List<AggregatorFactory> aggregatorFactories;
}
