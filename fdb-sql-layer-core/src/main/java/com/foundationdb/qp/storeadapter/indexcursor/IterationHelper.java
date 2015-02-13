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

package com.foundationdb.qp.storeadapter.indexcursor;

import com.foundationdb.qp.row.Row;
import com.persistit.Key;
import com.persistit.Key.Direction;

public interface IterationHelper
{
    /** Get the (stateful) key associated with this helper. */
    Key key();
    Key endKey();

    /** Clear internal state, including the {@link #key()}. */
    void clear();

    /** Begin a new iteration. */
    void openIteration();

    /** Close the current iteration. */
    void closeIteration();

    /**
     * Get the row for that last advancement.
     * <p/>
     * <i>
     *     Note: {@link #traverse(Direction, boolean)} must be called prior to this method.
     * </i>
     */
    Row row();

    /**
     * Advance internal state.
     * @param dir The direction to advance in.
     * @return <code>true</code> if there was a key/value to traverse to.
     */
    boolean traverse(Direction dir);

    /**
     * Start cursor for given direction if that can be done asynchronously.
     * @param dir The direction to advance in.
     */
    void preload(Direction dir);
}
