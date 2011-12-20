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

package com.akiban.server;

import com.akiban.server.rowdata.RowDef;

/**
 * Structure denotes summary information about a table, including row count,
 * uniqueId and auto-increment values. In general there is one TableStatus per
 * RowDef, and each object refers to the other.
 */
public interface TableStatus {
    /**
     * @return Current auto-increment value of the assocated table.
     */
    long getAutoIncrement();

    /**
     * @return Ordinal of the associated table.
     */
    int getOrdinal();

    /**
     * @return Current number of rows in the associated table.
     */
    long getRowCount();

    /**
     * @return The <b>last</b> unique value used for the associated table.
     */
    long getUniqueID();

    /**
     * @return RowDef of the associated table.
     */
    RowDef getRowDef();
}
