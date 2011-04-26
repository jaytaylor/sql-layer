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

public interface ModifiableCursor extends Cursor {
    /**
     * Removes from the backing structure whatever row would be returned by {@link Cursor#currentRow()}
     * @throws CursorUpdateException if the current row couldn't be removed due to an underlying exception
     */
    void removeCurrentRow();

    /**
     * Updates in the backing structure whatever row would be returned by {@link Cursor#currentRow()}
     * @param newRow the new row's value
     * @throws IncompatibleRowException if the given row is incompatible with the current row
     * @throws CursorUpdateException if the current row couldn't be updated due to an underlying exception
     */
    void updateCurrentRow(Row newRow);

    /**
     * Adds a row to the backing structure, in some unspecified location. The location is up to the implemenation,
     * and this is <em>not</em> an insert, in that this Cursor's next row may or may not be this one.
     * @param newRow the row to be added
     * @throws CursorUpdateException if the given row couldn't be added due to an underlying exception
     */
    void addRow(Row newRow);
}
