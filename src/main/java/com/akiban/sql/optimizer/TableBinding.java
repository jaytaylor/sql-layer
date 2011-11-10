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

package com.akiban.sql.optimizer;

import com.akiban.ais.model.Table;

/**
 * A table binding: stored in the UserData of a FromBaseTable and
 * referring to a Table in the AIS.
 */
public class TableBinding 
{
    private Table table;
    private boolean nullable;
        
    public TableBinding(Table table, boolean nullable) {
        this.table = table;
        this.nullable = nullable;
    }

    public TableBinding(TableBinding other) {
        this.table = other.table;
    }

    public Table getTable() {
        return table;
    }

    public boolean isNullable() {
        return nullable;
    }

    public String toString() {
        return table.toString();
    }
}
