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

import com.foundationdb.sql.parser.FromTable;
import com.foundationdb.sql.parser.ResultColumn;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.types.DataTypeDescriptor;

import com.foundationdb.ais.model.Column;

/**
 * A column binding: stored in the UserData of a ColumnReference and
 * referring to a column from one of the tables in some FromList,
 * either as a DDL column (which may not be mentioned in any select
 * list) or a result column in a subquery.
 */
public class ColumnBinding 
{
    private FromTable fromTable;
    private Column column;
    private ResultColumn resultColumn;
    private boolean nullable;
        
    public ColumnBinding(FromTable fromTable, Column column, boolean nullable) {
        this.fromTable = fromTable;
        this.column = column;
        this.nullable = nullable;
    }
    public ColumnBinding(FromTable fromTable, ResultColumn resultColumn) {
        this.fromTable = fromTable;
        this.resultColumn = resultColumn;
    }

    public FromTable getFromTable() {
        return fromTable;
    }

    public Column getColumn() {
        return column;
    }

    /** Is the column nullable by virtue of its table being in an outer join? */
    public boolean isNullable() {
        return nullable;
    }

    public ResultColumn getResultColumn() {
        return resultColumn;
    }

    public DataTypeDescriptor getType() throws StandardException {
        if (resultColumn != null) {
            return resultColumn.getType();
        }
        else {
            return getType(column, nullable);
        }
    }
    
    public static DataTypeDescriptor getType(Column column, boolean nullable)
            throws StandardException {
        DataTypeDescriptor type = column.getType().dataTypeDescriptor();
        if (nullable)
            type = type.getNullabilityType(nullable);
        return type;
    }

    @Override
    public String toString() {
        StringBuffer result = new StringBuffer();
        if (resultColumn != null) {
            result.append(resultColumn.getClass().getName());
            result.append('@');
            result.append(Integer.toHexString(resultColumn.hashCode()));
        }
        else
            result.append(column);
        if (fromTable != null) {
            result.append(" from ");
            result.append(fromTable.getClass().getName());
            result.append('@');
            result.append(Integer.toHexString(fromTable.hashCode()));
        }
        return result.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof ColumnBinding)) return false;
        ColumnBinding other = (ColumnBinding)obj;
        return ((fromTable == other.fromTable) &&
                (column == other.column) &&
                (resultColumn == other.resultColumn));
    }

    @Override
    public int hashCode() {
        int hash = fromTable.hashCode();
        if (column != null)
            hash += column.hashCode();
        if (resultColumn != null)
            hash += resultColumn.hashCode();
        return hash;
    }

}
