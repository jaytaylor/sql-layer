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

import java.util.Arrays;

public class ValuesRowType extends DerivedRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return "values(" + Arrays.toString(tInstances) + ')';
    }

    // RowType interface

    @Override
    public int nFields()
    {
        return (tInstances == null) ? 0 : tInstances.length;
    }

    @Override
    public TInstance typeAt(int index) {
        return tInstances[index];
    }

    // ValuesRowType interface

    public ValuesRowType(Schema schema, int typeId, TInstance... fields) {
        super(schema, typeId);
        this.tInstances = fields;
    }

    // Object state
    private TInstance[] tInstances;
}
