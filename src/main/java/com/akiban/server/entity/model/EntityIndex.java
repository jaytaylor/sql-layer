
package com.akiban.server.entity.model;

import com.google.common.collect.ImmutableList;

import java.util.List;

public final class EntityIndex {

    public List<EntityColumn> getColumns() {
        return columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EntityIndex that = (EntityIndex) o;
        return columns.equals(that.columns);

    }

    @Override
    public int hashCode() {
        return columns.hashCode();
    }

    @Override
    public String toString() {
        return columns.toString();
    }

    public EntityIndex(List<EntityColumn> columns) {
        this.columns = ImmutableList.copyOf(columns);
    }

    public static EntityIndex create(List<EntityColumn> columns) {
        return new EntityIndex(columns);
    }

    private final List<EntityColumn> columns;
}
