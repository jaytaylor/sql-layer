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

/**
 * The state of a scan cursor.
 */
public enum CursorState {
    /**
     * Newly opened, hasn't started scanning yet.
     */
    FRESH(true),
    /**
     * At least one scan, but more rows may be available.
     */
    SCANNING(true),
    /**
     * Scanning is complete; subsequent scan requests will fail.
     */
    FINISHED(false),
    /**
     * Scanning cannot continue because a field involved in the index has been modified.
     */
    CONCURRENT_MODIFICATION(false),
    /**
     * Scanning cannot continue because of a DDL that may have affected this scan.
     */
    DDL_MODIFICATION(false),
    /**
     * The requested cursor is unknown or has been removed.
     */
    UNKNOWN_CURSOR(false)
    ;

    private final boolean isOpenState;

    CursorState(boolean openState) {
        isOpenState = openState;
    }

    public boolean isOpenState() {
        return isOpenState;
    }
}
