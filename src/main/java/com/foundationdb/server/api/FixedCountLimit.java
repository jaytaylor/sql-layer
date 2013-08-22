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

package com.foundationdb.server.api;

import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.api.dml.scan.ScanLimit;
import com.foundationdb.util.ArgumentValidation;

public class FixedCountLimit implements ScanLimit {
    private final int limit;
    private int count = 0;

    public FixedCountLimit(int limit) {
        ArgumentValidation.isGTE("limit", limit, 0);
        this.limit = limit;
    }

    @Override
    public boolean limitReached(RowData previousRow) {
        return limit >= 0 && count++ >= limit;
    }

    public int getLimit() {
        return limit;
    }
}
