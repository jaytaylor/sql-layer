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

package com.akiban.qp.expression;

import com.akiban.ais.model.UserTable;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexKeyType;

public class IndexBound
{
    public String toString()
    {
        return String.format("%s=%s", table, row);
    }

    public UserTable table()
    {
        return table;
    }

    public Row row()
    {
        return row;
    }

    public IndexBound(UserTable table, Row row)
    {
        this.table = table;
        this.row = row;
    }

    // Object state

    private final UserTable table;
    private final Row row;
}
