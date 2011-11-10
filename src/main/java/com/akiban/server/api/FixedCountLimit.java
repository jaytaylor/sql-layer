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

package com.akiban.server.api;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.api.dml.scan.ScanLimit;
import com.akiban.util.ArgumentValidation;

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
