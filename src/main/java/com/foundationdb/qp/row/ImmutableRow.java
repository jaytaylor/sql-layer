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

import com.foundationdb.qp.rowtype.CompoundRowType;
import com.foundationdb.qp.rowtype.FlattenedRowType;
import com.foundationdb.qp.rowtype.ProductRowType;
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
        // this is after project, why doesn't the rowtype align with the valuetype
        for (int i = 0; i < size; ++i)
            ret.add(row.value(i));
        return ret.iterator();
    }

    public static Row buildImmutableRow(Row row) {
        if (!row.isBindingsSensitive()) {
            return row;
        }
        if (row instanceof FlattenedRow) {
            FlattenedRow fRow = (FlattenedRow) row;
            return new FlattenedRow((FlattenedRowType)fRow.rowType(), buildImmutableRow(fRow.getFirstRow()), buildImmutableRow(fRow.getSecondRow()), fRow.hKey());
        }
        else if (row instanceof ProductRow) {
            ProductRow pRow = (ProductRow) row;
            return new ProductRow((ProductRowType)pRow.rowType(), buildImmutableRow(pRow.getFirstRow()), buildImmutableRow(pRow.getSecondRow()));
        }
        else if (row instanceof CompoundRow) {
            CompoundRow cRow = (FlattenedRow) row;
            return new CompoundRow((CompoundRowType)cRow.rowType(), buildImmutableRow(cRow.getFirstRow()), buildImmutableRow(cRow.getSecondRow()));
        }
        else {
            return new ImmutableRow(row);
        }
    }
}
