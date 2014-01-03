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

package com.foundationdb.ais.model;

import com.foundationdb.util.Strings;

public class ColumnName
{
    private final TableName tableName;
    private final String columnName;

    public ColumnName(String schema, String table, String columnName) {
        this(new TableName(schema, table), columnName);
    }

    public ColumnName(Column column) {
        this(column.getTable().getName(), column.getName());
    }

    public ColumnName(TableName tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    /** Parse a qualified string (e.g. test.foo.id) into a ColumnName. */
    public static ColumnName parse(String defaultSchema, String s) {
        String[] parts = Strings.parseQualifiedName(s, 3);
        return new ColumnName(parts[0].isEmpty() ? defaultSchema : parts[0], parts[1], parts[2]);
    }

    public TableName getTableName() {
        return tableName;
    }

    public String getName() {
        return columnName;
    }

    public String toStringEscaped() {
        return String.format("%s.%s", tableName.toStringEscaped(), Strings.escapeIdentifier(columnName));
    }

    @Override
    public String toString() {
        return String.format("%s.%s.%s", tableName.getSchemaName(), tableName.getTableName(), columnName);
    }

    @Override
    public boolean equals(Object o) {
        if(this == o) {
            return true;
        }
        if(o == null || getClass() != o.getClass()) {
            return false;
        }
        ColumnName that = (ColumnName)o;
        return columnName.equals(that.columnName) && tableName.equals(that.tableName);
    }

    @Override
    public int hashCode() {
        int result = tableName.hashCode();
        result = 31 * result + columnName.hashCode();
        return result;
    }
}
