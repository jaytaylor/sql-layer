package com.akiban.cserver.api.ddl;

import com.akiban.message.ErrorCode;

public final class JoinToUnknownTableException extends DDLException {
    public JoinToUnknownTableException(String message) {
        super(ErrorCode.JOIN_TO_UNKNOWN_TABLE, message);
    }
}
