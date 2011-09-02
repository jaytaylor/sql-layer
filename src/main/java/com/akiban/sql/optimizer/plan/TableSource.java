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

package com.akiban.sql.optimizer.plan;

/** A join to an actual table. */
public class TableSource extends JoinSource
{
    private TableNode table;
    // TODO: Add conditions, ...

    public TableSource(TableNode table) {
        this.table = table;
    }

    public TableNode getTable() {
        return table;
    }

    @Override
    public boolean isTable() {
        return true;
    }

    @Override
    public String toString() {
        return table.toString();
    }
}
