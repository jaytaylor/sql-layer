package com.akiban.cserver.api.dml;

import com.akiban.cserver.encoding.EncodingException;
import com.akiban.message.ErrorCode;

public final class TableDefinitionMismatchException extends DMLException {
    public TableDefinitionMismatchException(String formatter, Object... args) {
        super(ErrorCode.TABLEDEF_MISMATCH, formatter, args);
    }

    public TableDefinitionMismatchException(EncodingException e) {
        super(ErrorCode.TABLEDEF_MISMATCH, "Couldn't encode a value; you probably gave a wrong type", e);
    }
}
