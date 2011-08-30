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

public interface Cursor
{
    /**
     * Starts a complete scan of the underlying table or index.
     * @param bindings Values of variables referenced in the underlying query.
     */
    void open(Bindings bindings);

    /**
     * Advances to and returns the next row of the underlying table or index.
     * @return The next row of the scan, or null if all rows have been visited.
     */
    Row next();

    /**
     * Terminates the scan of the underlying table or index. Further calls to next() will return false.
     */
    void close();
}
