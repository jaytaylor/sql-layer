package com.akiban.cserver.util;

public final class RowDefNotFoundException extends RuntimeException {
    private final int rowDefId;
    public RowDefNotFoundException(int rowDefId) {
        super("rowDef not found: " + rowDefId);
        this.rowDefId = rowDefId;
    }

    public int getId() {
        return rowDefId;
    }
}
