package com.akiban.cserver.api.ddl;

import com.akiban.message.ErrorCode;

public final class NoPrimaryKeyException extends DDLException {
    public NoPrimaryKeyException(String message) {
        super(ErrorCode.NO_PRIMARY_KEY, message);
    }
}
