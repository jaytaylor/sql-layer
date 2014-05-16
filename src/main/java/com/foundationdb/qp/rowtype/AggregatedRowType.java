/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

import com.foundationdb.server.types.TInstance;

import java.util.List;

public final class AggregatedRowType extends DerivedRowType {
    @Override
    public int nFields() {
        return base.nFields();
    }

    @Override
    public TInstance typeAt(int index) {
        if (index < inputsIndex)
            return base.typeAt(index);
        else
            return pAggrTypes.get(index - inputsIndex);
    }


    public AggregatedRowType(Schema schema, int typeId,
                             RowType base, int inputsIndex, List<? extends TInstance> pAggrTypes) {
        super(schema, typeId);
        this.base = base;
        this.inputsIndex = inputsIndex;
        this.pAggrTypes = pAggrTypes;
    }

    private final RowType base;
    private final int inputsIndex;
    private final List<? extends TInstance> pAggrTypes;
}
