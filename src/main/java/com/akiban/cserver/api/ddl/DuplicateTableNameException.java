package com.akiban.cserver.api.ddl;

import com.akiban.message.ErrorCode;

public final class DuplicateTableNameException extends DDLException {
    public DuplicateTableNameException(String message) {
        super(ErrorCode.DUPLICATE_TABLE, message);
    }
}
