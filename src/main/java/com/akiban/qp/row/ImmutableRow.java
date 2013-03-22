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

package com.akiban.qp.row;

import com.akiban.qp.rowtype.RowType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;

import java.util.Iterator;

public final class ImmutableRow extends AbstractValuesHolderRow
{
    @Deprecated
    public ImmutableRow(RowType rowType, Iterator<? extends ValueSource> initialValues)
    {
        this(rowType, initialValues, null);
    }

    public ImmutableRow(ProjectedRow row)
    {
        this(row.rowType(), row.getValueSources(), row.getPValueSources());
    }
    public ImmutableRow(RowType rowType, Iterator<? extends ValueSource> initialValues, Iterator<? extends PValueSource> initialPValues)
    {
        super(rowType, false, initialValues, initialPValues);
    }
}
