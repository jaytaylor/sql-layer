package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.api.common.ColumnId;

public final class SingleColumnPredicate implements Predicate {
    public enum Comparison {
        EQUALS
    }

    private final ColumnId column;
    private final Object value;
    private final Comparison comparison;

    public SingleColumnPredicate(ColumnId column, Object value, Comparison comparison) {
        this.column = column;
        this.value = value;
        this.comparison = comparison;
    }
}
