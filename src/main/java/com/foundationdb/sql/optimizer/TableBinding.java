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

package com.foundationdb.sql.optimizer;

import com.foundationdb.ais.model.Columnar;

/**
 * A table binding: stored in the UserData of a FromBaseTable and
 * referring to a Table in the AIS.
 */
public class TableBinding 
{
    private Columnar table;
    private boolean nullable;
        
    public TableBinding(Columnar table, boolean nullable) {
        this.table = table;
        this.nullable = nullable;
    }

    public TableBinding(TableBinding other) {
        this.table = other.table;
    }

    public Columnar getTable() {
        return table;
    }

    public boolean isNullable() {
        return nullable;
    }

    public String toString() {
        return table.toString();
    }
}
