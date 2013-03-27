/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.entity.model;

import com.google.common.base.Function;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonValue;

import java.util.Arrays;
import java.util.List;

public final class EntityColumn {

    public String getTable() {
        return table;
    }

    public String getColumn() {
        return column;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EntityColumn that = (EntityColumn) o;
        return column.equals(that.column) && table.equals(that.table);
    }

    @Override
    public int hashCode() {
        int result = table.hashCode();
        result = 31 * result + column.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("%s.%s", table, column);
    }

    public EntityColumn(String table, String column) {
        this.table = table;
        this.column = column;
    }

    @JsonValue
    public Object asJsonValue() {
        return Arrays.asList(table, column);
    }

    @JsonCreator
    EntityColumn(List<String> names) {
        if (names.size() != 2)
            throw new IllegalEntityDefinition("column names must be 2-long");
        table = names.get(0);
        column = names.get(1);
    }

    private final String table;
    private final String column;

    static final Function<List<String>, EntityColumn> namesToColumn = new Function<List<String>, EntityColumn>() {
        @Override
        public EntityColumn apply(List<String> column) {
            return new EntityColumn(column);
        }
    };
}
