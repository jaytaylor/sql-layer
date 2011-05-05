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

import com.akiban.server.RowData;

/**
 * A ScanLimit that represents no limit. This is package private because nobody should ever instantiate it directly;
 * instead, they should grab the singleton reference {@link ScanLimit#NONE}
 */
final class NoScanLimit implements ScanLimit {
    @Override
    public String toString()
    {
        return "none";
    }

    @Override
    public boolean limitReached(RowData previousRow) {
        return false;
    }
}
