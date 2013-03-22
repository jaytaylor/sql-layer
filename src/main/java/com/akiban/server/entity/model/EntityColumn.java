
package com.akiban.server.entity.model;

import com.google.common.base.Function;

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
