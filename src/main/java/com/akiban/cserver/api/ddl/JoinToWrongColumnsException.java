package com.akiban.cserver.api.ddl;

import com.akiban.message.ErrorCode;

public final class JoinToWrongColumnsException extends DDLException {
    public JoinToWrongColumnsException(String message) {
        super(ErrorCode.JOIN_TO_WRONG_COLUMNS, message);
    }
}
