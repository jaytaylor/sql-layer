package com.akiban.cserver.api.dml;

import com.akiban.message.ErrorCode;

public final class TableDefinitionMismatchException extends DMLException {
    public TableDefinitionMismatchException(String message) {
        super(ErrorCode.TABLEDEF_MISMATCH, message);
    }
}
