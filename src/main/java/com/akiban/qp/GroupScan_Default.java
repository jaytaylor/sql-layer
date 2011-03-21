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

package com.akiban.qp;

import com.akiban.ais.model.GroupTable;

public class GroupScan_Default implements PhysicalOperator
{
    // PhysicalOperator interface

    @Override
    public void open()
    {
    }

    @Override
    public Row next()
    {
        return cursor.next();
    }

    @Override
    public void close()
    {
        cursor.close();
    }

    // GroupScan_Default interface

    public GroupScan_Default(BTreeAdapter btree, GroupTable groupTable)
    {
        this.cursor = btree.newCursor(groupTable);
    }

    // Object state

    private final Cursor cursor;
}
