package com.akiban.cserver.api.ddl;

import com.akiban.message.ErrorCode;

public final class ProtectedTableDDLException extends DDLException {
    public ProtectedTableDDLException(String message) {
        super(ErrorCode.PROTECTED_TABLE, message);
    }
}
