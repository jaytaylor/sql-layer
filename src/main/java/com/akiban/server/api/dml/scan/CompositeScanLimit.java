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

package com.akiban.server.api.dml.scan;

import com.akiban.server.rowdata.RowData;

public final class CompositeScanLimit implements ScanLimit {
    private final ScanLimit[] limits;

    public CompositeScanLimit(ScanLimit... limits) {
        this.limits = new ScanLimit[limits.length];
        System.arraycopy(limits, 0, this.limits, 0, limits.length);
    }

    @Override
    public boolean limitReached(RowData previousRow) {
        for (ScanLimit limit : limits) {
            if (limit.limitReached(previousRow)) {
                return true;
            }
        }
        return false;
    }
}
