package com.akiban.cserver.api.dml;

import com.akiban.message.ErrorCode;

public final class NoSuchColumnException extends DMLException {
    public NoSuchColumnException(String message) {
        super(ErrorCode.NO_SUCH_COLUMN, message);
    }
}
