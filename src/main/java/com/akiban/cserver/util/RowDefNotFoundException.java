package com.akiban.cserver.util;

public final class RowDefNotFoundException extends RuntimeException {

    public RowDefNotFoundException(int rowDef) {
        super("rowDef not found: " + rowDef);
    }
}
