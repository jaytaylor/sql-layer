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

package com.foundationdb.server.api.dml.scan;

import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.api.dml.scan.ScanLimit;
import com.foundationdb.util.ArgumentValidation;

public final class PredicateLimit implements ScanLimit {
    private final int rowDefId;
    private final int limit;
    private int count = 0;

    public PredicateLimit(int rowDefId, int limit) {
        ArgumentValidation.isGTE("limit", limit, 0);
        this.limit = limit;
        this.rowDefId = rowDefId;
    }

    @Override
    public String toString()
    {
        return Integer.toString(limit);
    }

    @Override
    public boolean limitReached(RowData candidateRow) {
        if (candidateRow != null && candidateRow.getRowDefId() == rowDefId) {
            ++count;
        }
        return limit == 0 || count > limit;
    }

    public int getLimit() {
        return limit;
    }
}
