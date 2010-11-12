package com.akiban.cserver.api.dml;

import com.akiban.message.ErrorCode;

public final class NoSuchTableException extends DMLException {
    public NoSuchTableException(String message) {
        super(ErrorCode.NO_SUCH_TABLE, message);
    }
}
