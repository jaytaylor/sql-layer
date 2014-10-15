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

public interface ScanLimit {

    /**
     * A singleton that represents no scan limit.
     */
    public static final ScanLimit NONE = new NoScanLimit();

    /**
     * Whether the limit has been reached; a {@code false} value indicates that the scan should continue. This method
     * is invoked directly after the row is collected, and before it's outputted; if this method returns {@code false},
     * the method will not be outputted
     *
     * @param row the row that has just been collected
     * @return whether scanning should stop
     */
    boolean limitReached(RowData row);

}
