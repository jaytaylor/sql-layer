package com.akiban.cserver.api.dml;

import com.akiban.cserver.InvalidOperationException;
import com.akiban.message.ErrorCode;

public final class TableDefinitionMismatchException extends DMLException {
    public TableDefinitionMismatchException(String formatter, Object... args) {
        super(ErrorCode.TABLEDEF_MISMATCH, formatter, args);
    }

    public TableDefinitionMismatchException(InvalidOperationException e) {
        super(e);
    }
}
