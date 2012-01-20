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

package com.akiban.qp.operator;

import com.akiban.qp.row.Row;

public interface UpdateFunction extends SelectionFunction {
    /**
     * Updates the given row by returning another row with the required modifications.
     * @param original the original row, which will remain untouched
     * @param context the query context for evaluation
     * @return a row of the same type as the original, but different fields
     * @throws IllegalArgumentException if the row could not be updated
     * (ie, if {@linkplain #rowIsSelected(Row)} returned {@code false})
     */
    Row evaluate(Row original, QueryContext context);
}
