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

package com.foundationdb.qp.persistitadapter;

import com.foundationdb.qp.operator.Limit;
import com.foundationdb.qp.row.RowBase;
import com.foundationdb.server.api.dml.scan.ScanLimit;

public class PersistitRowLimit implements Limit
{
    // Object interface

    @Override
    public String toString()
    {
        return limit.toString();
    }


    // Limit interface

    @Override
    public boolean limitReached(RowBase row)
    {
        PersistitGroupRow persistitRow = (PersistitGroupRow) row;
        return limit.limitReached(persistitRow.rowData());
    }

    // PersistitRowLimit interface

    public PersistitRowLimit(ScanLimit limit)
    {
        this.limit = limit;
    }

    // Object state

    private final ScanLimit limit;
}
