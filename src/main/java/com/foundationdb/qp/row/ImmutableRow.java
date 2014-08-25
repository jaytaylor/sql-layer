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

package com.foundationdb.qp.row;

import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.types.value.ValueSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class ImmutableRow extends AbstractValuesHolderRow
{
    public ImmutableRow(Row row)
    {
        this(row.rowType(), getValueSources(row));
    }

    public ImmutableRow(RowType rowType, Iterator<? extends ValueSource> initialValues)
    {
        super(rowType, false, initialValues);
    }

    public static Iterator<ValueSource> getValueSources(Row row)
    {
        int size = row.rowType().nFields();
        List<ValueSource> ret = new ArrayList<>(size);
        for (int i = 0; i < size; ++i)
            ret.add(row.value(i));
        return ret.iterator();
    }
}
