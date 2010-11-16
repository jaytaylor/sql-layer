package com.akiban.cserver.api.dml;

import com.akiban.cserver.InvalidOperationException;

public final class TableDefinitionMismatchException extends DMLException {
    public TableDefinitionMismatchException(InvalidOperationException e) {
    super(e);
    }
}
