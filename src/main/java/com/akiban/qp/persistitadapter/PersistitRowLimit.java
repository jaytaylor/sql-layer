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

package com.akiban.qp.persistitadapter;

import com.akiban.qp.physicaloperator.Limit;
import com.akiban.qp.row.RowBase;
import com.akiban.server.api.dml.scan.ScanLimit;

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
