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

import java.util.EnumSet;

public interface Predicate {
    /**
     * Gets the row that defines this predicate's start. If and only if this predicate is
     * for an exact value (e.g., customer_id == 5), the returned object will be the same as
     * (by object identity) the result of {@linkplain #getEndRow()}.
     * @return the predicate's starting range
     */
    NewRow getStartRow();

    /**
     * Gets the row that defines this predicate's end. If and only if this predicate is
     * for an exact value (e.g., customer_id == 5), the returned object will be the same as
     * (by object identity) the result of {@linkplain #getStartRow()}.
     * @return the predicate's ending range
     */
    NewRow getEndRow();

    /**
     * Returns a copy of the scan flags set by this predicate. Not all available scan flags are relevant
     * to predicates; the DEEP flag, for instance, is not. Those flags will never be set.
     * @return a copy of the predicate's scan flags
     */
    EnumSet<ScanFlag> getScanFlags();
}
