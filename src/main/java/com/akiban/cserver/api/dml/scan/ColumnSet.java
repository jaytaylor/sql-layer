package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.api.common.ColumnId;

import java.util.HashSet;
import java.util.Set;

public final class ColumnSet {
    private final Set<ColumnId> columns;

    public ColumnSet() {
        this.columns = new HashSet<ColumnId>();
    }

    ColumnSet(ColumnSet copy) {
        this.columns = new HashSet<ColumnId>(copy.columns);
    }

    public void addColumn(ColumnId column) {
        columns.add(column);
    }

    public void removeColumn(ColumnId column) {
        columns.remove(column);
    }
}
