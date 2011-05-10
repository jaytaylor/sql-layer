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

package com.akiban.qp.physicaloperator;

import com.akiban.qp.row.Row;
import com.akiban.qp.row.RowBase;

public interface Cursor
{
    /**
     * Starts a complete scan of the underlying table or index.
     */
    void open(Bindings bindings);

    /**
     * Advances to the next row of the underlying table or index.
     *
     * @return true if there is a next row, false otherwise.
     */
    boolean next();

    /**
     * Terminates the scan of the underlying table or index. Further calls to next() will return false.
     */
    void close();

    /**
     * The current row of the underlying table or index.
     *
     * @return The current row of the underlying table or index, or null if the scan has ended.
     */
    Row currentRow();

    /**
     * Removes from the backing structure whatever row would be returned by {@link Cursor#currentRow()}
     * @throws CursorUpdateException if the current row couldn't be removed due to an underlying exception
     */
    void removeCurrentRow();

    /**
     * Updates in the backing structure whatever row would be returned by {@link Cursor#currentRow()}
     *
     * @param newRow the new row's value
     * @throws IncompatibleRowException if the given row is incompatible with the current row
     * @throws CursorUpdateException if the current row couldn't be updated due to an underlying exception
     */
    void updateCurrentRow(Row newRow);

    /**
     * Gets the store that backs this modifiable cursor.
     * @return the cursor's backing store
     */
    ModifiableCursorBackingStore backingStore();

    /**
     * Specifies whether this cursor can support a given ability.
     * @param ability the ability to check for
     * @return whether the ability is supported
     */
    boolean cursorAbilitiesInclude(CursorAbility ability);
}
