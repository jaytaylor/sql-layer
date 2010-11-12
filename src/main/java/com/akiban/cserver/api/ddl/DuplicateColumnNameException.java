package com.akiban.cserver.api.ddl;

import com.akiban.message.ErrorCode;

public final class DuplicateColumnNameException extends DDLException {
    public DuplicateColumnNameException(String message) {
        super(ErrorCode.DUPLICATE_COLUMN_NAMES, message);
    }
}
